package com.jobscheduler.core;

import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;
import org.springframework.scheduling.support.CronExpression;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

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
     * Next fire after a completed fire; null means the job is done (one-shot).
     * If the computed next instant is already in the past the job fires once
     * immediately — per-job missed-fire policies (SKIP / FIRE_ALL) land in M1.
     */
    public static Instant nextAfterFire(Job job, Instant scheduledFire, Instant now) {
        return switch (job.getScheduleType()) {
            case ONE_SHOT -> null;
            case CRON -> nextCron(job.getCronExpression(), now);
            case FIXED_RATE -> {
                Instant next = scheduledFire.plusSeconds(job.getFixedRateSeconds());
                yield next.isAfter(now) ? next : now;
            }
        };
    }

    private static Instant nextCron(String cron, Instant now) {
        ZonedDateTime next = CronExpression.parse(cron).next(ZonedDateTime.ofInstant(now, ZoneOffset.UTC));
        return next == null ? null : next.toInstant();
    }
}
