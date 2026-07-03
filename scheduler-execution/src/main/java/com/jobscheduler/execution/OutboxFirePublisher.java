package com.jobscheduler.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.core.FiredJob;
import com.jobscheduler.core.FirePublisher;
import com.jobscheduler.store.repo.JobClaimStore;
import com.jobscheduler.store.repo.OutboxStore;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/**
 * M2 publish path: the event goes into the outbox table INSIDE the fire
 * transaction (no dual-write window), and the execution flips to FIRED in the
 * same tx. The OutboxPoller does the actual Kafka produce after commit.
 */
@Component
public class OutboxFirePublisher implements FirePublisher {

    private final OutboxStore outbox;
    private final JobClaimStore store;
    private final KafkaProperties props;
    private final ObjectMapper mapper;
    private final OutboxPoller poller;

    public OutboxFirePublisher(OutboxStore outbox, JobClaimStore store, KafkaProperties props,
                               ObjectMapper mapper, OutboxPoller poller) {
        this.outbox = outbox;
        this.store = store;
        this.props = props;
        this.mapper = mapper;
        this.poller = poller;
    }

    @Override
    public Mono<Void> enqueueAll(List<FiredJob> fired) {
        Instant firedAt = Instant.now();
        return Flux.fromIterable(fired)
                .concatMap(job -> {
                    JobFiredEvent event = new JobFiredEvent(job.jobId(), job.type(), job.payload(),
                            job.fireEpoch(), job.scheduledAt(), firedAt, job.traceId());
                    return outbox.enqueue(job.jobId().toString(), props.topic(),
                                    job.jobId().toString(), toJson(event), job.traceId())
                            .then(store.markFired(job.jobId(), job.fireEpoch()));
                })
                .then();
    }

    @Override
    public Mono<Void> onCommitted(List<FiredJob> fired) {
        poller.drainNow();
        return Mono.empty();
    }

    private String toJson(JobFiredEvent event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unserializable job event " + event.eventKey(), e);
        }
    }
}
