package com.jobscheduler.core;

import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;
import org.springframework.scheduling.support.CronExpression;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/** Pure schedule math. All cron evaluation is UTC. */
public final class ScheduleCalculator {

    private ScheduleCalculator() {
    }

    public static boolean isValidCron(String expression) {
        return expression != null && CronExpression.isValidExpression(expression);
    }

    public static Instant firstFire(ScheduleType type, String cron, Long fixedRateSeconds, Instant fireAt, Instant now) {
        return switch (type) {
            case ONE_SHOT -> fireAt;
            case CRON -> nextCron(cron, now);
            // fireAt doubles as an optional explicit start for fixed-rate jobs
            case FIXED_RATE -> fireAt != null ? fireAt : now.plusSeconds(fixedRateSeconds);
        };
    }

    /**
     * The next occurrence strictly after now, anchored to the original cadence:
     * a fixed-rate job that fired late keeps its scheduled grid (scheduled + k*rate),
     * it does not drift to now + rate. Null means the job is done (one-shot).
     */
    public static Instant nextFutureOccurrence(Job job, Instant scheduled, Instant now) {
        return switch (job.getScheduleType()) {
            case ONE_SHOT -> null;
            case CRON -> nextCron(job.getCronExpression(), now.isAfter(scheduled) ? now : scheduled);
            case FIXED_RATE -> {
                long rate = job.getFixedRateSeconds();
                if (!now.isAfter(scheduled)) {
                    yield scheduled.plusSeconds(rate);
                }
                long k = Duration.between(scheduled, now).getSeconds() / rate + 1;
                yield scheduled.plusSeconds(k * rate);
            }
        };
    }

    /**
     * Occurrences from {@code from} (inclusive) through {@code now}, capped — the
     * FIRE_ALL catch-up set. The cap bounds a single catch-up transaction; the
     * caller resumes from the occurrence after the last one returned.
     */
    public static List<Instant> occurrencesThrough(Job job, Instant from, Instant now, int cap) {
        List<Instant> occurrences = new ArrayList<>();
        Instant t = from;
        while (t != null && !t.isAfter(now) && occurrences.size() < cap) {
            occurrences.add(t);
            t = switch (job.getScheduleType()) {
                case ONE_SHOT -> null;
                case FIXED_RATE -> t.plusSeconds(job.getFixedRateSeconds());
                case CRON -> nextCron(job.getCronExpression(), t);
            };
        }
        return occurrences;
    }

    /** The occurrence after the given one on the job's cadence; null when done. */
    public static Instant occurrenceAfter(Job job, Instant occurrence) {
        return switch (job.getScheduleType()) {
            case ONE_SHOT -> null;
            case FIXED_RATE -> occurrence.plusSeconds(job.getFixedRateSeconds());
            case CRON -> nextCron(job.getCronExpression(), occurrence);
        };
    }

    private static Instant nextCron(String cron, Instant after) {
        ZonedDateTime next = CronExpression.parse(cron).next(ZonedDateTime.ofInstant(after, ZoneOffset.UTC));
        return next == null ? null : next.toInstant();
    }
}
