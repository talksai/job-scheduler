package com.jobscheduler.store.repo;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Shard → worker assignments, fenced by the leader's epoch: a write stamped
 * with a LOWER epoch than the stored row is silently rejected (0 rows), which
 * is what stops a deposed coordinator — paused GC, network partition — from
 * clobbering its successor's assignments (Kleppmann's fencing-token fix).
 */
@Component
public class ShardAssignmentStore {

    private static final String ASSIGN_SQL = """
            INSERT INTO shard_assignment (shard, worker_id, epoch)
            VALUES (:shard, :workerId, :epoch)
            ON CONFLICT (shard) DO UPDATE
            SET worker_id = EXCLUDED.worker_id, epoch = EXCLUDED.epoch, assigned_at = now()
            WHERE shard_assignment.epoch <= EXCLUDED.epoch
            """;

    private final DatabaseClient db;

    public ShardAssignmentStore(DatabaseClient db) {
        this.db = db;
    }

    /** @return false when fenced — the caller's epoch is stale and it must step down. */
    public Mono<Boolean> assign(int shard, String workerId, long epoch) {
        return db.sql(ASSIGN_SQL)
                .bind("shard", shard)
                .bind("workerId", workerId)
                .bind("epoch", epoch)
                .fetch().rowsUpdated()
                .map(rows -> rows == 1);
    }

    public Flux<ShardAssignmentRow> all() {
        return db.sql("SELECT shard, worker_id, epoch FROM shard_assignment ORDER BY shard")
                .map(row -> new ShardAssignmentRow(
                        row.get("shard", Short.class).intValue(),
                        row.get("worker_id", String.class),
                        row.get("epoch", Long.class)))
                .all();
    }

    public Flux<ShardAssignmentRow> assignedTo(String workerId) {
        return db.sql("SELECT shard, worker_id, epoch FROM shard_assignment WHERE worker_id = :workerId ORDER BY shard")
                .bind("workerId", workerId)
                .map(row -> new ShardAssignmentRow(
                        row.get("shard", Short.class).intValue(),
                        row.get("worker_id", String.class),
                        row.get("epoch", Long.class)))
                .all();
    }

    public record ShardAssignmentRow(int shard, String workerId, long epoch) {
    }
}
