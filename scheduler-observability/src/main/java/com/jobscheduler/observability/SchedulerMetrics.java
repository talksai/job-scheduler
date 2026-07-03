package com.jobscheduler.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/** All scheduler meters in one place so metric names stay consistent with the SLO doc. */
public class SchedulerMetrics {

    public static final String JOBS_CREATED = "scheduler.jobs.created";
    public static final String JOBS_FIRED = "scheduler.jobs.fired";
    public static final String FIRE_LATENCY = "scheduler.fire.latency";
    public static final String FIRES_SKIPPED = "scheduler.fires.skipped";
    public static final String FIRES_CATCHUP = "scheduler.fires.catchup";
    public static final String WHEEL_TIMERS = "scheduler.wheel.timers";
    public static final String WHEEL_HYDRATED = "scheduler.wheel.hydrated";
    public static final String DB_PENDING = "scheduler.db.pending";
    public static final String OUTBOX_UNPUBLISHED = "scheduler.outbox.unpublished";
    public static final String LEASE_EPOCH = "scheduler.lease.epoch";
    public static final String COORDINATOR_LEADER = "scheduler.coordinator.leader";
    public static final String SHARDS_OWNED = "scheduler.shards.owned";
    public static final String REBALANCES = "scheduler.assignments.rebalanced";
    public static final String FENCING_REJECTED = "scheduler.fencing.rejected";
    public static final String FAILOVER_COORDINATOR = "scheduler.failover.coordinator";
    public static final String FAILOVER_WORKER = "scheduler.failover.worker";
    public static final String EXECUTION_LAG = "scheduler.execution.lag";
    public static final String EVENTS_PUBLISHED = "scheduler.events.published";
    public static final String EVENTS_CONSUMED = "scheduler.events.consumed";
    public static final String EVENTS_DEDUP = "scheduler.events.dedup";
    public static final String EVENTS_RETRIED = "scheduler.events.retried";
    public static final String EVENTS_DLQ = "scheduler.events.dlq";

    private final Counter jobsCreated;
    private final Counter jobsFired;
    private final Timer fireLatency;
    private final Counter firesSkipped;
    private final Counter firesCatchup;
    private final Counter timersHydrated;
    private final AtomicLong wheelTimers = new AtomicLong();
    private final AtomicLong dbPending = new AtomicLong();
    private final AtomicLong outboxUnpublished = new AtomicLong();
    private final AtomicLong leaseEpoch = new AtomicLong();
    private final AtomicLong coordinatorLeader = new AtomicLong();
    private final AtomicLong shardsOwned = new AtomicLong();
    private final Counter rebalances;
    private final Counter fencingRejected;
    private final Timer coordinatorFailover;
    private final Timer workerFailover;
    private final Timer executionLag;
    private final Counter eventsPublished;
    private final Counter eventsConsumed;
    private final Counter eventsDedup;
    private final Counter eventsRetried;
    private final Counter eventsDlq;

    public SchedulerMetrics(MeterRegistry registry) {
        this.jobsCreated = Counter.builder(JOBS_CREATED)
                .description("Jobs accepted via the REST API").register(registry);
        this.jobsFired = Counter.builder(JOBS_FIRED)
                .description("Fires claimed and executed").register(registry);
        this.fireLatency = Timer.builder(FIRE_LATENCY)
                .description("Scheduled-vs-actual fire latency for on-time fires (scheduling accuracy SLO)")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        this.firesSkipped = Counter.builder(FIRES_SKIPPED)
                .description("Late fires dropped by the SKIP missed-fire policy").register(registry);
        this.firesCatchup = Counter.builder(FIRES_CATCHUP)
                .description("Late fires executed by FIRE_ONCE_IMMEDIATELY / FIRE_ALL catch-up").register(registry);
        this.timersHydrated = Counter.builder(WHEEL_HYDRATED)
                .description("Timers loaded into the wheel from Postgres").register(registry);
        Gauge.builder(WHEEL_TIMERS, wheelTimers, AtomicLong::get)
                .description("Timers currently parked in the in-memory wheel").register(registry);
        Gauge.builder(DB_PENDING, dbPending, AtomicLong::get)
                .description("PENDING jobs in Postgres (durable timers)").register(registry);
        Gauge.builder(OUTBOX_UNPUBLISHED, outboxUnpublished, AtomicLong::get)
                .description("Outbox rows awaiting publish (outbox lag)").register(registry);
        Gauge.builder(LEASE_EPOCH, leaseEpoch, AtomicLong::get)
                .description("Highest coordinator lease epoch seen (the fencing token)").register(registry);
        Gauge.builder(COORDINATOR_LEADER, coordinatorLeader, AtomicLong::get)
                .description("1 when this node holds the coordinator lease").register(registry);
        Gauge.builder(SHARDS_OWNED, shardsOwned, AtomicLong::get)
                .description("Shards currently assigned to this worker").register(registry);
        this.rebalances = Counter.builder(REBALANCES)
                .description("Shard assignment changes written by the coordinator").register(registry);
        this.fencingRejected = Counter.builder(FENCING_REJECTED)
                .description("Writes rejected because they carried a stale fencing token").register(registry);
        this.coordinatorFailover = Timer.builder(FAILOVER_COORDINATOR)
                .description("Lease-expiry to new-leader-elected duration (coordinator failover SLO)")
                .register(registry);
        this.workerFailover = Timer.builder(FAILOVER_WORKER)
                .description("Worker-death detection to shard-reassignment duration (worker failover SLO)")
                .register(registry);
        this.executionLag = Timer.builder(EXECUTION_LAG)
                .description("Fired-to-effect-applied end-to-end lag (outbox publish + Kafka + consume)")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        this.eventsPublished = Counter.builder(EVENTS_PUBLISHED)
                .description("Events published to Kafka").register(registry);
        this.eventsConsumed = Counter.builder(EVENTS_CONSUMED)
                .description("Events consumed and applied (first delivery)").register(registry);
        this.eventsDedup = Counter.builder(EVENTS_DEDUP)
                .description("Duplicate deliveries absorbed by processed_event dedup").register(registry);
        this.eventsRetried = Counter.builder(EVENTS_RETRIED)
                .description("Events routed to the retry topic").register(registry);
        this.eventsDlq = Counter.builder(EVENTS_DLQ)
                .description("Events dead-lettered (DLQ rate SLO)").register(registry);
    }

    public void jobCreated() {
        jobsCreated.increment();
    }

    /** Late (catch-up) fires are counted but excluded from the accuracy SLO timer. */
    public void jobFired(Instant scheduledAt, Instant firedAt, boolean late) {
        jobsFired.increment();
        if (late) {
            firesCatchup.increment();
        } else {
            fireLatency.record(Duration.between(scheduledAt, firedAt).abs());
        }
    }

    public void fireSkipped() {
        firesSkipped.increment();
    }

    public void timersHydrated(int count) {
        timersHydrated.increment(count);
    }

    public void setWheelTimers(long count) {
        wheelTimers.set(count);
    }

    public void setDbPending(long count) {
        dbPending.set(count);
    }

    public void setLeaseEpoch(long epoch) {
        leaseEpoch.accumulateAndGet(epoch, Math::max);
    }

    public void setLeader(boolean leader) {
        coordinatorLeader.set(leader ? 1 : 0);
    }

    public void setShardsOwned(long count) {
        shardsOwned.set(count);
    }

    public void rebalanced(int changes) {
        rebalances.increment(changes);
    }

    public void fencingRejected() {
        fencingRejected.increment();
    }

    public void coordinatorFailover(Duration downtime) {
        coordinatorFailover.record(downtime);
    }

    public void workerFailover(Duration reassignmentDelay) {
        workerFailover.record(reassignmentDelay);
    }

    public void executionLag(Duration lag) {
        executionLag.record(lag.abs());
    }

    public void eventsPublished(int count) {
        eventsPublished.increment(count);
    }

    public void setOutboxUnpublished(long count) {
        outboxUnpublished.set(count);
    }

    public void eventConsumed() {
        eventsConsumed.increment();
    }

    public void eventDedup() {
        eventsDedup.increment();
    }

    public void eventRetried() {
        eventsRetried.increment();
    }

    public void eventDeadLettered() {
        eventsDlq.increment();
    }
}
