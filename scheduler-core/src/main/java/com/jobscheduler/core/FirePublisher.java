package com.jobscheduler.core;

import reactor.core.publisher.Mono;

import java.util.List;

/** Port implemented by the execution module (Kafka in M0, outbox-backed from M2). */
public interface FirePublisher {

    Mono<Void> publishAll(List<FiredJob> fired);
}
