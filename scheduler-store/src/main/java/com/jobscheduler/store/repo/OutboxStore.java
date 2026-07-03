package com.jobscheduler.store.repo;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

/**
 * Transactional outbox (ARCHITECTURE §3.2): rows are written in the SAME tx as
 * the execution state change, so there is no dual-write window. The poller
 * publishes them at-least-once; consumer dedup makes the effect exactly-once.
 */
@Component
public class OutboxStore {

    private static final String FETCH_UNPUBLISHED_SQL = """
            SELECT id, topic, message_key, payload, trace_id
            FROM outbox
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """;

    private final DatabaseClient db;

    public OutboxStore(DatabaseClient db) {
        this.db = db;
    }

    /** Must run inside the caller's transaction — that is the whole point. */
    public Mono<Void> enqueue(String aggregateId, String topic, String messageKey, String payload, String traceId) {
        return db.sql("INSERT INTO outbox (aggregate_id, topic, message_key, payload, trace_id) " +
                        "VALUES (:aggregateId, :topic, :messageKey, :payload, :traceId)")
                .bind("aggregateId", aggregateId)
                .bind("topic", topic)
                .bind("messageKey", messageKey)
                .bind("payload", payload)
                .bind("traceId", traceId)
                .fetch().rowsUpdated()
                .then();
    }

    /**
     * Locks a batch for publishing; SKIP LOCKED keeps concurrent pollers (multiple
     * workers, M3+) from double-draining. Locks hold only within the caller's tx,
     * so fetch → Kafka send → markPublished must share one transaction.
     */
    public Flux<OutboxMessage> lockUnpublished(int batchSize) {
        return db.sql(FETCH_UNPUBLISHED_SQL)
                .bind("batchSize", batchSize)
                .map(row -> new OutboxMessage(
                        row.get("id", Long.class),
                        row.get("topic", String.class),
                        row.get("message_key", String.class),
                        row.get("payload", String.class),
                        row.get("trace_id", String.class)))
                .all();
    }

    public Mono<Void> markPublished(Collection<Long> ids) {
        return db.sql("UPDATE outbox SET published_at = now() WHERE id = ANY(:ids)")
                .bind("ids", ids.toArray(Long[]::new))
                .fetch().rowsUpdated()
                .then();
    }

    public Mono<Long> countUnpublished() {
        return db.sql("SELECT count(*) AS lag FROM outbox WHERE published_at IS NULL")
                .map(row -> row.get("lag", Long.class))
                .one();
    }
}
