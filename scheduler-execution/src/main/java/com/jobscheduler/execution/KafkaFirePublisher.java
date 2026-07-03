package com.jobscheduler.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.core.FiredJob;
import com.jobscheduler.core.FirePublisher;
import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.JobClaimStore;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.List;

/**
 * M0 publish path: direct produce after the claim tx commits, then mark the
 * execution FIRED. M2 replaces the direct produce with the transactional outbox.
 */
@Component
public class KafkaFirePublisher implements FirePublisher {

    private final KafkaSender<String, String> sender;
    private final JobClaimStore store;
    private final KafkaProperties props;
    private final SchedulerMetrics metrics;
    private final ObjectMapper mapper;

    public KafkaFirePublisher(KafkaSender<String, String> sender, JobClaimStore store,
                              KafkaProperties props, SchedulerMetrics metrics, ObjectMapper mapper) {
        this.sender = sender;
        this.store = store;
        this.props = props;
        this.metrics = metrics;
        this.mapper = mapper;
    }

    @Override
    public Mono<Void> publishAll(List<FiredJob> fired) {
        Instant firedAt = Instant.now();
        return sender.send(Flux.fromIterable(fired).map(job -> toRecord(job, firedAt)))
                .concatMap(result -> {
                    if (result.exception() != null) {
                        return Mono.error(result.exception());
                    }
                    FiredJob job = result.correlationMetadata();
                    metrics.eventPublished();
                    metrics.jobFired(job.scheduledAt(), firedAt);
                    return store.markFired(job.jobId(), job.fireEpoch());
                })
                .then();
    }

    private SenderRecord<String, String, FiredJob> toRecord(FiredJob job, Instant firedAt) {
        JobFiredEvent event = new JobFiredEvent(job.jobId(), job.type(), job.payload(),
                job.fireEpoch(), job.scheduledAt(), firedAt);
        return SenderRecord.create(
                new ProducerRecord<>(props.topic(), job.jobId().toString(), toJson(event)), job);
    }

    private String toJson(JobFiredEvent event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unserializable job event " + event.eventKey(), e);
        }
    }
}
