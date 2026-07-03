package com.jobscheduler.store.repo;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

/** Worker membership: heartbeats in, liveness out. A silent worker is a dead worker. */
@Component
public class WorkerRegistryStore {

    private final DatabaseClient db;

    public WorkerRegistryStore(DatabaseClient db) {
        this.db = db;
    }

    public Mono<Void> heartbeat(String workerId) {
        return db.sql("INSERT INTO worker_registry (worker_id) VALUES (:workerId) " +
                        "ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat = now()")
                .bind("workerId", workerId)
                .fetch().rowsUpdated()
                .then();
    }

    /** Workers whose heartbeat is fresher than the TTL, in stable (sorted) order. */
    public Flux<String> activeWorkers(Duration ttl) {
        return db.sql("SELECT worker_id FROM worker_registry WHERE last_heartbeat > :cutoff ORDER BY worker_id")
                .bind("cutoff", Instant.now().minus(ttl))
                .map(row -> row.get("worker_id", String.class))
                .all();
    }

    /** Every registered worker with its last heartbeat — dead ones included (failover timing). */
    public Flux<WorkerHeartbeat> allWorkers() {
        return db.sql("SELECT worker_id, last_heartbeat FROM worker_registry ORDER BY worker_id")
                .map(row -> new WorkerHeartbeat(
                        row.get("worker_id", String.class),
                        row.get("last_heartbeat", Instant.class)))
                .all();
    }

    public record WorkerHeartbeat(String workerId, Instant lastHeartbeat) {
    }
}
