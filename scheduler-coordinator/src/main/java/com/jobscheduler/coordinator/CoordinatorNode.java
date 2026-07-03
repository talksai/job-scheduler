package com.jobscheduler.coordinator;

import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.ShardAssignmentStore;
import com.jobscheduler.store.repo.ShardAssignmentStore.ShardAssignmentRow;
import com.jobscheduler.store.repo.WorkerRegistryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The control plane. Every node runs one of these; whoever wins the Postgres
 * lease is THE coordinator until it stops renewing. The leader's duty loop:
 * sweep live workers, compute a balanced shard map, write only the deltas —
 * every write stamped with the leader's epoch and rejected by the store if a
 * newer leader has taken over (fencing). A failed renewal or a fenced write
 * means we were deposed: step down immediately, never fight the successor.
 */
public class CoordinatorNode implements SmartLifecycle {

    public static final String COORDINATOR_ROLE = "coordinator";

    private static final Logger log = LoggerFactory.getLogger(CoordinatorNode.class);

    private final CoordinationService coordination;
    private final WorkerRegistryStore registry;
    private final ShardAssignmentStore assignments;
    private final CoordinationProperties props;
    private final SchedulerMetrics metrics;
    private final int totalShards;

    private volatile Leadership leadership;
    private volatile Disposable loop;

    public CoordinatorNode(CoordinationService coordination, WorkerRegistryStore registry,
                           ShardAssignmentStore assignments, CoordinationProperties props,
                           SchedulerMetrics metrics, int totalShards) {
        this.coordination = coordination;
        this.registry = registry;
        this.assignments = assignments;
        this.props = props;
        this.metrics = metrics;
        this.totalShards = totalShards;
    }

    @Override
    public void start() {
        loop = Flux.interval(Duration.ZERO, Duration.ofMillis(props.electionIntervalMs()))
                .onBackpressureDrop()
                .concatMap(tick -> duty()
                        .onErrorResume(e -> {
                            log.error("Coordinator duty failed", e);
                            return Mono.empty();
                        }), 1)
                .subscribe();
    }

    private Mono<Void> duty() {
        Leadership current = leadership;
        if (current == null) {
            return coordination.campaign(COORDINATOR_ROLE)
                    .flatMap(won -> {
                        leadership = won;
                        metrics.setLeader(true);
                        metrics.setLeaseEpoch(won.epoch());
                        log.info("Won coordinator lease: epoch={} holder={}", won.epoch(), won.holder());
                        return rebalance(won);
                    });
        }
        return coordination.renew(current)
                .flatMap(renewed -> {
                    if (!renewed) {
                        stepDown("lease renewal rejected (deposed or expired)");
                        return Mono.empty();
                    }
                    return rebalance(current);
                });
    }

    private Mono<Void> rebalance(Leadership leader) {
        return registry.allWorkers().collectList()
                .zipWith(assignments.all().collectList())
                .flatMap(tuple -> {
                    Instant now = Instant.now();
                    Duration workerTtl = Duration.ofSeconds(props.workerTtlSeconds());
                    Map<String, Instant> heartbeats = new HashMap<>();
                    tuple.getT1().forEach(w -> heartbeats.put(w.workerId(), w.lastHeartbeat()));
                    List<String> workers = heartbeats.entrySet().stream()
                            .filter(entry -> entry.getValue().isAfter(now.minus(workerTtl)))
                            .map(Map.Entry::getKey)
                            .sorted()
                            .toList();
                    if (workers.isEmpty()) {
                        return Mono.empty();
                    }
                    Map<Integer, ShardAssignmentRow> current = new HashMap<>();
                    tuple.getT2().forEach(row -> current.put(row.shard(), row));

                    // write when the owner changes OR the row still carries a predecessor's
                    // epoch — a fresh leader restamps everything so every stale-token write
                    // (epoch strictly lower) is fenced from then on
                    return Flux.range(0, totalShards)
                            .filter(shard -> {
                                ShardAssignmentRow row = current.get(shard);
                                return row == null
                                        || !desiredOwner(workers, shard).equals(row.workerId())
                                        || row.epoch() < leader.epoch();
                            })
                            .takeWhile(shard -> leadership != null) // stop writing once deposed
                            .concatMap(shard -> assignments.assign(shard, desiredOwner(workers, shard), leader.epoch())
                                    .flatMap(accepted -> {
                                        if (!accepted) {
                                            metrics.fencingRejected();
                                            stepDown("assignment write fenced by a newer epoch");
                                            return Mono.empty();
                                        }
                                        return Mono.just(shard);
                                    }))
                            .collectList()
                            .doOnNext(changed -> {
                                if (!changed.isEmpty()) {
                                    metrics.rebalanced(changed.size());
                                    log.info("Rebalanced {} shard(s) across {} worker(s) at epoch {}: {}",
                                            changed.size(), workers.size(), leader.epoch(), changed);
                                    recordWorkerFailovers(changed, current, workers, heartbeats, workerTtl, now);
                                }
                            })
                            .then();
                });
    }

    private static String desiredOwner(List<String> sortedWorkers, int shard) {
        return sortedWorkers.get(shard % sortedWorkers.size());
    }

    /**
     * Times worker failover: how long a dead worker's shards sat with it after the
     * point its silence made it officially dead (last heartbeat + TTL) until this
     * reassignment — the worker-failover SLO.
     */
    private void recordWorkerFailovers(List<Integer> changedShards, Map<Integer, ShardAssignmentRow> previous,
                                       List<String> liveWorkers, Map<String, Instant> heartbeats,
                                       Duration workerTtl, Instant now) {
        changedShards.stream()
                .map(previous::get)
                .filter(row -> row != null && !liveWorkers.contains(row.workerId()))
                .map(ShardAssignmentRow::workerId)
                .distinct()
                .forEach(deadWorker -> {
                    Instant declaredDead = heartbeats.get(deadWorker) != null
                            ? heartbeats.get(deadWorker).plus(workerTtl)
                            : null;
                    if (declaredDead != null && declaredDead.isBefore(now)) {
                        metrics.workerFailover(Duration.between(declaredDead, now));
                        log.info("Worker {} shards reassigned {} ms after death detection",
                                deadWorker, Duration.between(declaredDead, now).toMillis());
                    }
                });
    }

    private void stepDown(String reason) {
        if (leadership != null) {
            log.warn("Stepping down as coordinator (epoch {}): {}", leadership.epoch(), reason);
            leadership = null;
            metrics.setLeader(false);
        }
    }

    public boolean isLeader() {
        return leadership != null;
    }

    @Override
    public void stop() {
        if (loop != null) {
            loop.dispose();
            loop = null;
        }
        stepDown("lifecycle stop");
    }

    @Override
    public boolean isRunning() {
        return loop != null && !loop.isDisposed();
    }
}
