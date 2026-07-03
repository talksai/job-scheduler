package com.jobscheduler.store.repo;

import com.jobscheduler.store.domain.JobState;
import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;
import io.r2dbc.spi.Readable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Claim-path SQL. Row locks from {@code FOR UPDATE SKIP LOCKED} only hold within the
 * surrounding transaction, so callers must run {@link #lockDueJobs} and the per-job
 * writes inside one {@code TransactionalOperator} scope.
 */
@Component
public class JobClaimStore {

    private static final String LOCK_DUE_SQL = """
            SELECT id, type, payload, schedule_type, cron_expression, fixed_rate_seconds,
                   fire_at, missed_fire_policy, shard, next_fire_at, state, version, created_at
            FROM job
            WHERE state = 'PENDING' AND next_fire_at <= now()
            ORDER BY next_fire_at
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """;

    private static final String CLAIM_SQL = """
            INSERT INTO execution (job_id, fire_epoch, status, claimed_by, claimed_at, attempt)
            VALUES (:jobId, :fireEpoch, 'CLAIMED', :workerId, now(), 1)
            ON CONFLICT (job_id, fire_epoch) DO NOTHING
            """;

    private final DatabaseClient db;

    public JobClaimStore(DatabaseClient db) {
        this.db = db;
    }

    public Flux<Job> lockDueJobs(int batchSize) {
        return db.sql(LOCK_DUE_SQL)
                .bind("batchSize", batchSize)
                .map(JobClaimStore::toJob)
                .all();
    }

    /** One winner per (jobId, fireEpoch) — the exactly-once anchor. */
    public Mono<Boolean> tryClaim(UUID jobId, long fireEpoch, String workerId) {
        return db.sql(CLAIM_SQL)
                .bind("jobId", jobId)
                .bind("fireEpoch", fireEpoch)
                .bind("workerId", workerId)
                .fetch().rowsUpdated()
                .map(rows -> rows == 1);
    }

    public Mono<Void> reschedule(UUID jobId, Instant nextFireAt) {
        return db.sql("UPDATE job SET next_fire_at = :next, version = version + 1 WHERE id = :id")
                .bind("next", nextFireAt)
                .bind("id", jobId)
                .fetch().rowsUpdated()
                .then();
    }

    public Mono<Void> complete(UUID jobId) {
        return db.sql("UPDATE job SET state = 'COMPLETED', next_fire_at = NULL, version = version + 1 WHERE id = :id")
                .bind("id", jobId)
                .fetch().rowsUpdated()
                .then();
    }

    public Mono<Boolean> cancel(UUID jobId) {
        return db.sql("UPDATE job SET state = 'CANCELLED', next_fire_at = NULL, version = version + 1 " +
                        "WHERE id = :id AND state = 'PENDING'")
                .bind("id", jobId)
                .fetch().rowsUpdated()
                .map(rows -> rows > 0);
    }

    public Mono<Void> markFired(UUID jobId, long fireEpoch) {
        return db.sql("UPDATE execution SET status = 'FIRED' WHERE job_id = :jobId AND fire_epoch = :fireEpoch")
                .bind("jobId", jobId)
                .bind("fireEpoch", fireEpoch)
                .fetch().rowsUpdated()
                .then();
    }

    private static Job toJob(Readable row) {
        Job job = new Job();
        job.setId(row.get("id", UUID.class));
        job.setType(row.get("type", String.class));
        job.setPayload(row.get("payload", String.class));
        job.setScheduleType(ScheduleType.valueOf(row.get("schedule_type", String.class)));
        job.setCronExpression(row.get("cron_expression", String.class));
        job.setFixedRateSeconds(row.get("fixed_rate_seconds", Long.class));
        job.setFireAt(row.get("fire_at", Instant.class));
        job.setMissedFirePolicy(MissedFirePolicy.valueOf(row.get("missed_fire_policy", String.class)));
        job.setShard(row.get("shard", Short.class));
        job.setNextFireAt(row.get("next_fire_at", Instant.class));
        job.setState(JobState.valueOf(row.get("state", String.class)));
        job.setVersion(row.get("version", Long.class));
        job.setCreatedAt(row.get("created_at", Instant.class));
        return job;
    }
}
