package com.jobscheduler.coordinator;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.stream.IntStream;

/**
 * M0 stand-in: a single node that trivially "leads" and owns every shard at epoch 0.
 * Replaced by PostgresLeaseCoordination in M3.
 */
@Component
public class StaticSingleNodeCoordination implements CoordinationService {

    private final int shardCount;

    public StaticSingleNodeCoordination() {
        this(16);
    }

    public StaticSingleNodeCoordination(int shardCount) {
        this.shardCount = shardCount;
    }

    @Override
    public Mono<Leadership> campaign(String role) {
        return Mono.just(new Leadership(role, "single-node", 0L));
    }

    @Override
    public Flux<Assignment> assignments(String workerId) {
        return Flux.fromStream(IntStream.range(0, shardCount).mapToObj(shard -> new Assignment(shard, 0L)));
    }

    @Override
    public boolean isFenced(long token) {
        return false;
    }
}
