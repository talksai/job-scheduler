package com.jobscheduler.coordinator;

import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.LeaseStore;
import com.jobscheduler.store.repo.LeaseStore.LeaseRow;
import com.jobscheduler.store.repo.ShardAssignmentStore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CoordinationService on a plain Postgres lease — no ZooKeeper, no etcd.
 * Winning the lease increments its epoch; the epoch is the monotonic fencing
 * token stamped onto every assignment and fire. Each instance tracks the
 * highest epoch it has observed so stale tokens can be rejected locally too.
 */
public class PostgresLeaseCoordination implements CoordinationService {

    private final LeaseStore leases;
    private final ShardAssignmentStore assignments;
    private final String holderId;
    private final long leaseTtlSeconds;
    private final SchedulerMetrics metrics;
    private final AtomicLong highestSeenEpoch = new AtomicLong();

    public PostgresLeaseCoordination(LeaseStore leases, ShardAssignmentStore assignments,
                                     String holderId, long leaseTtlSeconds) {
        this(leases, assignments, holderId, leaseTtlSeconds, null);
    }

    public PostgresLeaseCoordination(LeaseStore leases, ShardAssignmentStore assignments,
                                     String holderId, long leaseTtlSeconds, SchedulerMetrics metrics) {
        this.leases = leases;
        this.assignments = assignments;
        this.holderId = holderId;
        this.leaseTtlSeconds = leaseTtlSeconds;
        this.metrics = metrics;
    }

    @Override
    public Mono<Leadership> campaign(String role) {
        // read first: a losing campaign still learns the incumbent epoch (fencing),
        // and a winning one can time the failover from the predecessor's expiry
        return leases.read(role)
                .map(Optional::of).defaultIfEmpty(Optional.empty())
                .flatMap(previous -> {
                    previous.ifPresent(row -> observe(row.epoch()));
                    return leases.tryAcquire(role, holderId, leaseTtlSeconds)
                            .map(epoch -> {
                                observe(epoch);
                                previous.ifPresent(this::recordFailover);
                                return new Leadership(role, holderId, epoch);
                            });
                });
    }

    private void recordFailover(LeaseRow previous) {
        // expires_at in the past means the predecessor died/paused and the role sat
        // leaderless until now — that gap is the coordinator-failover SLO
        if (metrics != null && previous.holder() != null && previous.expiresAt() != null
                && previous.expiresAt().isBefore(Instant.now())) {
            metrics.coordinatorFailover(Duration.between(previous.expiresAt(), Instant.now()));
        }
    }

    @Override
    public Mono<Boolean> renew(Leadership leadership) {
        return leases.renew(leadership.role(), leadership.holder(), leadership.epoch(), leaseTtlSeconds);
    }

    @Override
    public Flux<Assignment> assignments(String workerId) {
        return assignments.assignedTo(workerId)
                .doOnNext(row -> observe(row.epoch()))
                .map(row -> new Assignment(row.shard(), row.epoch()));
    }

    @Override
    public boolean isFenced(long token) {
        return token < highestSeenEpoch.get();
    }

    public String holderId() {
        return holderId;
    }

    private void observe(long epoch) {
        highestSeenEpoch.accumulateAndGet(epoch, Math::max);
    }
}
