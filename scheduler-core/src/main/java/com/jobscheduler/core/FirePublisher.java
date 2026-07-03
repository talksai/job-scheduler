package com.jobscheduler.core;

import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Port implemented by the execution module. enqueueAll runs INSIDE the fire
 * transaction (transactional outbox — the event becomes durable atomically with
 * the claim and schedule advance); onCommitted runs after the tx commits and
 * may trigger the actual broker publish.
 */
public interface FirePublisher {

    /** Durably enqueue the fire events; participates in the caller's transaction. */
    Mono<Void> enqueueAll(List<FiredJob> fired);

    /** Post-commit hook, e.g. wake the outbox poller. */
    default Mono<Void> onCommitted(List<FiredJob> fired) {
        return Mono.empty();
    }
}
