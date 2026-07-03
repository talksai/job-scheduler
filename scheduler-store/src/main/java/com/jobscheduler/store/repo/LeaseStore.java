package com.jobscheduler.store.repo;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * The coordinator lease (ARCHITECTURE §3.3): leader election without external
 * consensus. Acquiring an expired/free lease atomically increments epoch — the
 * monotonic fencing token. A TTL lock alone is not safe (a paused holder can
 * resume after expiry); every downstream write is guarded by the epoch instead.
 */
@Component
public class LeaseStore {

    private static final String ACQUIRE_SQL = """
            UPDATE coordinator_lease
            SET holder = :holder, epoch = epoch + 1, expires_at = :expiresAt
            WHERE role = :role AND (holder IS NULL OR expires_at IS NULL OR expires_at < now())
            RETURNING epoch
            """;

    private static final String RENEW_SQL = """
            UPDATE coordinator_lease
            SET expires_at = :expiresAt
            WHERE role = :role AND holder = :holder AND epoch = :epoch AND expires_at >= now()
            """;

    private final DatabaseClient db;

    public LeaseStore(DatabaseClient db) {
        this.db = db;
    }

    /** Emits the new epoch when the lease is won; empty when someone else holds it. */
    public Mono<Long> tryAcquire(String role, String holder, long ttlSeconds) {
        return db.sql(ACQUIRE_SQL)
                .bind("role", role)
                .bind("holder", holder)
                .bind("expiresAt", Instant.now().plusSeconds(ttlSeconds))
                .map(row -> row.get("epoch", Long.class))
                .one();
    }

    /** Heartbeat; false means deposed (epoch moved on or lease already expired) — step down. */
    public Mono<Boolean> renew(String role, String holder, long epoch, long ttlSeconds) {
        return db.sql(RENEW_SQL)
                .bind("role", role)
                .bind("holder", holder)
                .bind("epoch", epoch)
                .bind("expiresAt", Instant.now().plusSeconds(ttlSeconds))
                .fetch().rowsUpdated()
                .map(rows -> rows == 1);
    }

    public Mono<LeaseRow> read(String role) {
        return db.sql("SELECT holder, epoch, expires_at FROM coordinator_lease WHERE role = :role")
                .bind("role", role)
                .map(row -> new LeaseRow(
                        row.get("holder", String.class),
                        row.get("epoch", Long.class),
                        row.get("expires_at", Instant.class)))
                .one();
    }

    public record LeaseRow(String holder, long epoch, Instant expiresAt) {
    }
}
