package com.jobscheduler.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;

/** All scheduler meters in one place so metric names stay consistent with the SLO doc. */
public class SchedulerMetrics {

    public static final String JOBS_CREATED = "scheduler.jobs.created";
    public static final String JOBS_FIRED = "scheduler.jobs.fired";
    public static final String FIRE_LATENCY = "scheduler.fire.latency";
    public static final String EVENTS_PUBLISHED = "scheduler.events.published";
    public static final String EVENTS_CONSUMED = "scheduler.events.consumed";
    public static final String EVENTS_RETRIED = "scheduler.events.retried";
    public static final String EVENTS_DLQ = "scheduler.events.dlq";

    private final Counter jobsCreated;
    private final Counter jobsFired;
    private final Timer fireLatency;
    private final Counter eventsPublished;
    private final Counter eventsConsumed;
    private final Counter eventsRetried;
    private final Counter eventsDlq;

    public SchedulerMetrics(MeterRegistry registry) {
        this.jobsCreated = Counter.builder(JOBS_CREATED)
                .description("Jobs accepted via the REST API").register(registry);
        this.jobsFired = Counter.builder(JOBS_FIRED)
                .description("Fires claimed and executed").register(registry);
        this.fireLatency = Timer.builder(FIRE_LATENCY)
                .description("Scheduled-vs-actual fire latency (scheduling accuracy SLO)")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        this.eventsPublished = Counter.builder(EVENTS_PUBLISHED)
                .description("Events published to Kafka").register(registry);
        this.eventsConsumed = Counter.builder(EVENTS_CONSUMED)
                .description("Events consumed successfully").register(registry);
        this.eventsRetried = Counter.builder(EVENTS_RETRIED)
                .description("Events routed to the retry topic").register(registry);
        this.eventsDlq = Counter.builder(EVENTS_DLQ)
                .description("Events dead-lettered (DLQ rate SLO)").register(registry);
    }

    public void jobCreated() {
        jobsCreated.increment();
    }

    public void jobFired(Instant scheduledAt, Instant firedAt) {
        jobsFired.increment();
        fireLatency.record(Duration.between(scheduledAt, firedAt).abs());
    }

    public void eventPublished() {
        eventsPublished.increment();
    }

    public void eventConsumed() {
        eventsConsumed.increment();
    }

    public void eventRetried() {
        eventsRetried.increment();
    }

    public void eventDeadLettered() {
        eventsDlq.increment();
    }
}
