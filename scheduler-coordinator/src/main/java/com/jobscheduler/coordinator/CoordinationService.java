package com.jobscheduler.coordinator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Coordination primitive per ARCHITECTURE §6. Keeps the coordinator swappable:
 * PostgresLeaseCoordination is the primary impl; etcd or Raft-backed impls can
 * slot in behind the same contract.
 */
public interface CoordinationService {

    /**
     * Campaign for leadership of a role; emits the won Leadership (epoch = the
     * fencing token) or completes empty when someone else holds a live lease.
     */
    Mono<Leadership> campaign(String role);

    /** Heartbeat an existing leadership; false means deposed — step down. */
    Mono<Boolean> renew(Leadership leadership);

    /** Shard ranges this worker owns, each stamped with the issuing leader's token. */
    Flux<Assignment> assignments(String workerId);

    /** True when the token is stale (lower than the highest epoch seen) and must be rejected. */
    boolean isFenced(long token);
}
