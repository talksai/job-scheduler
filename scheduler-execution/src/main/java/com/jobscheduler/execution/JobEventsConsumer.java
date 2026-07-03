package com.jobscheduler.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.JobClaimStore;
import com.jobscheduler.store.repo.ProcessedEventStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Idempotent consumer of job-events + the retry topic. Per event, one tx:
 * insert the event key into processed_event (first delivery wins) and, only on
 * first delivery, apply the effect (execution FIRED → COMPLETED). Replays are
 * counted as dedup hits and acked without re-applying. Offsets are acknowledged
 * strictly AFTER the idempotent write commits, so a crash before the ack only
 * ever causes a redelivery — never a lost or double-applied effect. Failed
 * events hop to the retry topic with an incremented x-attempt header and
 * dead-letter once attempts are exhausted.
 */
@Component
public class JobEventsConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JobEventsConsumer.class);
    private static final String ATTEMPT_HEADER = "x-attempt";
    private static final String ERROR_HEADER = "x-error";

    private final KafkaProperties props;
    private final KafkaSender<String, String> sender;
    private final SchedulerMetrics metrics;
    private final ObjectMapper mapper;
    private final ProcessedEventStore processedEvents;
    private final JobClaimStore claimStore;
    private final TransactionalOperator tx;

    private volatile Disposable subscription;

    public JobEventsConsumer(KafkaProperties props, KafkaSender<String, String> sender,
                             SchedulerMetrics metrics, ObjectMapper mapper,
                             ProcessedEventStore processedEvents, JobClaimStore claimStore,
                             TransactionalOperator tx) {
        this.props = props;
        this.sender = sender;
        this.metrics = metrics;
        this.mapper = mapper;
        this.processedEvents = processedEvents;
        this.claimStore = claimStore;
        this.tx = tx;
    }

    @Override
    public void start() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, props.consumerGroup());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ReceiverOptions<String, String> options = ReceiverOptions.<String, String>create(config)
                .subscription(List.of(props.topic(), props.retryTopic()));

        subscription = KafkaReceiver.create(options).receive()
                .concatMap(record -> handle(record)
                        .onErrorResume(error -> routeFailure(record, error))
                        // ack only after the idempotent write committed (or the failure
                        // was routed) — commit-after-write, at-least-once upstream
                        .then(Mono.fromRunnable(() -> record.receiverOffset().acknowledge())), 1)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(10)))
                .subscribe();
        log.info("Consumer started on {} + {} (group {})", props.topic(), props.retryTopic(), props.consumerGroup());
    }

    private Mono<Void> handle(ConsumerRecord<String, String> record) {
        return Mono.fromCallable(() -> mapper.readValue(record.value(), JobFiredEvent.class))
                .flatMap(event -> processedEvents.markProcessed(event.eventKey())
                        .flatMap(firstDelivery -> firstDelivery
                                ? claimStore.markCompleted(event.jobId(), event.fireEpoch()).thenReturn(true)
                                : Mono.just(false))
                        .as(tx::transactional)
                        .doOnNext(firstDelivery -> {
                            if (firstDelivery) {
                                metrics.eventConsumed();
                                log.debug("Applied event {} from {}", event.eventKey(), record.topic());
                            } else {
                                metrics.eventDedup();
                                log.debug("Dedup hit for event {} from {}", event.eventKey(), record.topic());
                            }
                        }))
                .then();
    }

    private Mono<Void> routeFailure(ReceiverRecord<String, String> record, Throwable error) {
        int attempt = attemptOf(record) + 1;
        boolean exhausted = attempt >= props.maxAttempts();
        String target = exhausted ? props.dlqTopic() : props.retryTopic();
        log.warn("Event {} failed (attempt {}), routing to {}: {}", record.key(), attempt, target, error.getMessage());

        ProducerRecord<String, String> out = new ProducerRecord<>(target, record.key(), record.value());
        out.headers().add(ATTEMPT_HEADER, Integer.toString(attempt).getBytes(StandardCharsets.UTF_8));
        out.headers().add(ERROR_HEADER, String.valueOf(error.getMessage()).getBytes(StandardCharsets.UTF_8));

        if (exhausted) {
            metrics.eventDeadLettered();
        } else {
            metrics.eventRetried();
        }
        return sender.send(Mono.just(SenderRecord.create(out, record.key()))).then();
    }

    private static int attemptOf(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(ATTEMPT_HEADER);
        if (header == null) {
            return 0;
        }
        try {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
            subscription = null;
        }
    }

    @Override
    public boolean isRunning() {
        return subscription != null && !subscription.isDisposed();
    }
}
