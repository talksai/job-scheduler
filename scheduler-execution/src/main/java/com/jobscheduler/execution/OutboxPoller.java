package com.jobscheduler.execution;

import com.jobscheduler.core.SchedulerProperties;
import com.jobscheduler.observability.SchedulerMetrics;
import com.jobscheduler.store.repo.OutboxMessage;
import com.jobscheduler.store.repo.OutboxStore;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Publishes outbox rows to Kafka at-least-once. Each drain runs one tx:
 * lock a batch (SKIP LOCKED) → produce → markPublished → commit. A crash after
 * the produce but before the commit re-publishes the batch later — consumer
 * dedup absorbs it. Runs on an interval plus a post-commit wake-up from the
 * fire path so events don't wait a full poll cycle.
 */
@Component
public class OutboxPoller implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxStore outbox;
    private final KafkaSender<String, String> sender;
    private final TransactionalOperator tx;
    private final SchedulerProperties props;
    private final SchedulerMetrics metrics;
    private final Sinks.Many<Long> wake = Sinks.many().multicast().onBackpressureBuffer(1, false);

    private volatile Disposable loop;

    public OutboxPoller(OutboxStore outbox, KafkaSender<String, String> sender, TransactionalOperator tx,
                        SchedulerProperties props, SchedulerMetrics metrics) {
        this.outbox = outbox;
        this.sender = sender;
        this.tx = tx;
        this.props = props;
        this.metrics = metrics;
    }

    @Override
    public void start() {
        loop = Flux.merge(
                        Flux.interval(Duration.ofMillis(props.outboxPollIntervalMs())),
                        wake.asFlux())
                .onBackpressureDrop()
                .concatMap(tick -> drainUntilEmpty()
                        .onErrorResume(e -> {
                            log.error("Outbox drain failed", e);
                            return Mono.empty();
                        }), 1)
                .subscribe();
        log.info("Outbox poller started: every {}ms, batch {}", props.outboxPollIntervalMs(), props.outboxBatchSize());
    }

    /** Post-commit hint from the fire path; a no-op when a drain is already queued. */
    public void drainNow() {
        wake.tryEmitNext(0L);
    }

    private Mono<Void> drainUntilEmpty() {
        return drainOnce().flatMap(published -> published < props.outboxBatchSize()
                ? updateLagGauge()
                : drainUntilEmpty());
    }

    private Mono<Integer> drainOnce() {
        return outbox.lockUnpublished(props.outboxBatchSize())
                .collectList()
                .flatMap(batch -> batch.isEmpty()
                        ? Mono.just(0)
                        : publish(batch).thenReturn(batch.size()))
                .as(tx::transactional)
                .doOnNext(published -> {
                    if (published > 0) {
                        metrics.eventsPublished(published);
                        log.debug("Published {} outbox event(s)", published);
                    }
                });
    }

    private Mono<Void> publish(List<OutboxMessage> batch) {
        return sender.send(Flux.fromIterable(batch)
                        .map(msg -> {
                            ProducerRecord<String, String> record =
                                    new ProducerRecord<>(msg.topic(), msg.messageKey(), msg.payload());
                            if (msg.traceId() != null) {
                                record.headers().add("x-trace-id", msg.traceId().getBytes(StandardCharsets.UTF_8));
                            }
                            return SenderRecord.create(record, msg.id());
                        }))
                .concatMap(result -> result.exception() != null
                        ? Mono.<Long>error(result.exception())
                        : Mono.just(result.correlationMetadata()))
                .collectList()
                .flatMap(outbox::markPublished);
    }

    private Mono<Void> updateLagGauge() {
        return outbox.countUnpublished().doOnNext(metrics::setOutboxUnpublished).then();
    }

    @Override
    public void stop() {
        if (loop != null) {
            loop.dispose();
            loop = null;
        }
    }

    @Override
    public boolean isRunning() {
        return loop != null && !loop.isDisposed();
    }
}
