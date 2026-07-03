package com.jobscheduler.core;

import com.jobscheduler.coordinator.ShardOwnership;
import com.jobscheduler.core.wheel.HierarchicalTimingWheel;
import com.jobscheduler.core.wheel.TimerEntry;
import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.DueTimer;
import com.jobscheduler.store.repo.JobClaimStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * M1 due-detection: an in-memory hierarchical timing wheel over the near horizon,
 * backed by Postgres idx_due as the durable far-horizon store.
 *
 * <p>Two loops: the tick loop advances the wheel and fires expired entries; the
 * hydration loop sweeps idx_due a window ahead and parks candidates in the wheel.
 * The first hydration on start() doubles as crash recovery — the wheel is volatile
 * and rebuilt from Postgres, so a restart loses no scheduled fire (overdue rows
 * come back as immediate candidates and go through the missed-fire policy).
 *
 * <p>Wheel entries are hints, not claims: FireProcessor re-verifies each candidate
 * against the DB (state + due-ness + execution claim), so stale or duplicated
 * entries are harmless.
 */
@Component
public class WheelSchedulerService implements SmartLifecycle, TimerOffers {

    private static final Logger log = LoggerFactory.getLogger(WheelSchedulerService.class);

    private final JobClaimStore store;
    private final FireProcessor fireProcessor;
    private final SchedulerProperties props;
    private final SchedulerMetrics metrics;
    private final ShardOwnership ownership;

    private final Object lock = new Object();
    private final Set<String> parked = ConcurrentHashMap.newKeySet();
    private final List<TimerEntry> imminent = new ArrayList<>(); // guarded by lock
    private volatile HierarchicalTimingWheel wheel;
    private Disposable tickLoop;
    private Disposable hydrationLoop;

    public WheelSchedulerService(JobClaimStore store, FireProcessor fireProcessor,
                                 SchedulerProperties props, SchedulerMetrics metrics,
                                 ShardOwnership ownership) {
        this.store = store;
        this.fireProcessor = fireProcessor;
        this.props = props;
        this.metrics = metrics;
        this.ownership = ownership;
    }

    @Override
    public void start() {
        if (!props.wheelEnabled()) {
            log.info("Timing wheel disabled by configuration");
            return;
        }
        synchronized (lock) {
            wheel = new HierarchicalTimingWheel(props.tickMs(), props.wheelSize(), System.currentTimeMillis());
            parked.clear();
            imminent.clear();
        }
        tickLoop = Flux.interval(Duration.ofMillis(props.tickMs()))
                .onBackpressureDrop()
                .concatMap(tick -> fireDue()
                        .onErrorResume(e -> {
                            log.error("Tick processing failed", e);
                            return Mono.empty();
                        }), 1)
                .subscribe();
        hydrationLoop = Flux.interval(Duration.ZERO, Duration.ofMillis(props.hydrationIntervalMs()))
                .onBackpressureDrop()
                .concatMap(tick -> hydrate()
                        .onErrorResume(e -> {
                            log.error("Wheel hydration failed", e);
                            return Mono.empty();
                        }), 1)
                .subscribe();
        log.info("Timing wheel started: tick={}ms wheelSize={} window={}ms hydrateEvery={}ms",
                props.tickMs(), props.wheelSize(), props.hydrationWindowMs(), props.hydrationIntervalMs());
    }

    private Mono<Void> fireDue() {
        long nowMs = System.currentTimeMillis();
        List<TimerEntry> due = new ArrayList<>();
        synchronized (lock) {
            Iterator<TimerEntry> carried = imminent.iterator();
            while (carried.hasNext()) {
                TimerEntry entry = carried.next();
                if (entry.deadlineMs() <= nowMs) {
                    due.add(entry);
                    carried.remove();
                }
            }
            for (TimerEntry entry : wheel.advanceTo(nowMs)) {
                if (entry.deadlineMs() <= nowMs) {
                    due.add(entry);
                } else {
                    // floor-bucketing can surface an entry up to one tick early;
                    // carry it so it fires exactly on the next tick, never early
                    imminent.add(entry);
                }
            }
            metrics.setWheelTimers(wheel.size() + imminent.size());
        }
        if (due.isEmpty()) {
            return Mono.empty();
        }
        Set<UUID> jobIds = new LinkedHashSet<>();
        for (TimerEntry entry : due) {
            parked.remove(key(entry.jobId(), entry.deadlineMs()));
            jobIds.add(entry.jobId());
        }
        return fireProcessor.processDue(jobIds).then();
    }

    private Mono<Void> hydrate() {
        Set<Integer> shards = ownership.ownedShards();
        if (shards.isEmpty()) {
            // not assigned yet (startup, or mid-rebalance) — the durable timers wait in Postgres
            return Mono.empty();
        }
        Instant until = Instant.now().plusMillis(props.hydrationWindowMs());
        return store.findDueWithinWindow(until, props.hydrationBatchSize(), shards)
                .collectList()
                .flatMap(candidates -> {
                    List<UUID> immediate = new ArrayList<>();
                    int added = 0;
                    synchronized (lock) {
                        for (DueTimer candidate : candidates) {
                            long deadline = candidate.nextFireAt().toEpochMilli();
                            String k = key(candidate.jobId(), deadline);
                            if (!parked.add(k)) {
                                continue; // already in the wheel
                            }
                            if (wheel.add(new TimerEntry(candidate.jobId(), deadline))) {
                                added++;
                            } else {
                                parked.remove(k); // already due (incl. overdue after a restart)
                                immediate.add(candidate.jobId());
                            }
                        }
                        metrics.setWheelTimers(wheel.size() + imminent.size());
                    }
                    if (added > 0) {
                        metrics.timersHydrated(added);
                    }
                    Mono<Void> fireNow = immediate.isEmpty()
                            ? Mono.empty()
                            : fireProcessor.processDue(immediate).then();
                    return fireNow.then(store.countPending().doOnNext(metrics::setDbPending)).then();
                });
    }

    @Override
    public void offer(UUID jobId, Instant nextFireAt) {
        HierarchicalTimingWheel current = wheel;
        if (current == null || nextFireAt == null) {
            return; // wheel not running — hydration will pick the job up
        }
        long deadline = nextFireAt.toEpochMilli();
        if (deadline > System.currentTimeMillis() + props.hydrationWindowMs()) {
            return; // far horizon stays in Postgres only
        }
        String k = key(jobId, deadline);
        boolean immediate = false;
        synchronized (lock) {
            if (!parked.add(k)) {
                return;
            }
            if (!current.add(new TimerEntry(jobId, deadline))) {
                parked.remove(k);
                immediate = true;
            }
        }
        if (immediate) {
            fireProcessor.processDue(List.of(jobId))
                    .subscribe(count -> { }, e -> log.error("Immediate fire of {} failed", jobId, e));
        }
    }

    private static String key(UUID jobId, long deadlineMs) {
        return jobId + ":" + deadlineMs;
    }

    @Override
    public void stop() {
        if (tickLoop != null) {
            tickLoop.dispose();
            tickLoop = null;
        }
        if (hydrationLoop != null) {
            hydrationLoop.dispose();
            hydrationLoop = null;
        }
        synchronized (lock) {
            wheel = null;
            parked.clear();
            imminent.clear();
        }
    }

    @Override
    public boolean isRunning() {
        return tickLoop != null && !tickLoop.isDisposed();
    }
}
