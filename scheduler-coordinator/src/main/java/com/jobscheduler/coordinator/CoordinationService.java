package com.jobscheduler.coordinator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Coordination primitive per ARCHITECTURE §6. Keeps the coordinator swappable:
 * M3 delivers PostgresLeaseCoordination (advisory-lock lease, epoch = fencing token);
 * etcd or Raft-backed impls can slot in behind the same contract.
 */
public interface CoordinationService {

    /** Campaign for leadership of a role; the returned epoch is the fencing token. */
    Mono<Leadership> campaign(String role);

    /** Shard ranges this worker owns, each stamped with the issuing leader's token. */
    Flux<Assignment> assignments(String workerId);

    /** True when the token is stale (lower than the highest epoch seen) and must be rejected. */
    boolean isFenced(long token);
}
