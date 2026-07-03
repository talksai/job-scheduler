package com.jobscheduler.coordinator;

import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.WorkerRegistryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This node's worker identity: heartbeats into worker_registry so the
 * coordinator counts it as alive, and polls its shard assignments to expose
 * them (plus the fencing token they carry) to the fire path. Stopping the
 * heartbeat is exactly what a crashed worker looks like to the coordinator.
 */
public class WorkerMembership implements SmartLifecycle, ShardOwnership {

    private static final Logger log = LoggerFactory.getLogger(WorkerMembership.class);

    private final String workerId;
    private final WorkerRegistryStore registry;
    private final CoordinationService coordination;
    private final CoordinationProperties props;
    private final SchedulerMetrics metrics;

    private final AtomicReference<Set<Integer>> owned = new AtomicReference<>(Set.of());
    private final AtomicLong assignmentEpoch = new AtomicLong();
    private volatile Disposable heartbeatLoop;
    private volatile Disposable assignmentLoop;

    public WorkerMembership(String workerId, WorkerRegistryStore registry,
                            CoordinationService coordination, CoordinationProperties props,
                            SchedulerMetrics metrics) {
        this.workerId = workerId;
        this.registry = registry;
        this.coordination = coordination;
        this.props = props;
        this.metrics = metrics;
    }

    @Override
    public void start() {
        heartbeatLoop = Flux.interval(Duration.ZERO, Duration.ofMillis(props.heartbeatIntervalMs()))
                .onBackpressureDrop()
                .concatMap(tick -> registry.heartbeat(workerId)
                        .onErrorResume(e -> {
                            log.warn("Worker heartbeat failed: {}", e.getMessage());
                            return Mono.empty();
                        }), 1)
                .subscribe();
        assignmentLoop = Flux.interval(Duration.ZERO, Duration.ofMillis(props.assignmentPollMs()))
                .onBackpressureDrop()
                .concatMap(tick -> refreshAssignments()
                        .onErrorResume(e -> {
                            log.warn("Assignment refresh failed: {}", e.getMessage());
                            return Mono.empty();
                        }), 1)
                .subscribe();
        log.info("Worker {} joined: heartbeat every {}ms", workerId, props.heartbeatIntervalMs());
    }

    private Mono<Void> refreshAssignments() {
        return coordination.assignments(workerId).collectList()
                .doOnNext(assignments -> {
                    Set<Integer> shards = new TreeSet<>();
                    long epoch = 0;
                    for (Assignment assignment : assignments) {
                        shards.add(assignment.shard());
                        epoch = Math.max(epoch, assignment.epoch());
                    }
                    Set<Integer> previous = owned.getAndSet(Set.copyOf(shards));
                    assignmentEpoch.set(epoch);
                    metrics.setShardsOwned(shards.size());
                    metrics.setLeaseEpoch(epoch);
                    if (!previous.equals(shards)) {
                        log.info("Worker {} shard ownership changed (epoch {}): {} -> {}",
                                workerId, epoch, previous, shards);
                    }
                })
                .then();
    }

    @Override
    public Set<Integer> ownedShards() {
        return owned.get();
    }

    @Override
    public long epoch() {
        return assignmentEpoch.get();
    }

    public String workerId() {
        return workerId;
    }

    @Override
    public void stop() {
        if (heartbeatLoop != null) {
            heartbeatLoop.dispose();
            heartbeatLoop = null;
        }
        if (assignmentLoop != null) {
            assignmentLoop.dispose();
            assignmentLoop = null;
        }
        owned.set(Set.of());
        metrics.setShardsOwned(0);
    }

    @Override
    public boolean isRunning() {
        return heartbeatLoop != null && !heartbeatLoop.isDisposed();
    }
}
