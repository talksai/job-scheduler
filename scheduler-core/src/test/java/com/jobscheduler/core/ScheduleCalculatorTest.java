package com.jobscheduler.core;

import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ScheduleCalculatorTest {

    private static final Instant T = Instant.parse("2026-07-03T10:00:00Z");

    private static Job fixedRate(long rateSeconds) {
        Job job = new Job();
        job.setScheduleType(ScheduleType.FIXED_RATE);
        job.setFixedRateSeconds(rateSeconds);
        return job;
    }

    private static Job cron(String expression) {
        Job job = new Job();
        job.setScheduleType(ScheduleType.CRON);
        job.setCronExpression(expression);
        return job;
    }

    private static Job oneShot() {
        Job job = new Job();
        job.setScheduleType(ScheduleType.ONE_SHOT);
        return job;
    }

    @Test
    void firstFireUsesExplicitStartForFixedRate() {
        assertThat(ScheduleCalculator.firstFire(ScheduleType.FIXED_RATE, null, 60L, T.plusSeconds(5), T))
                .isEqualTo(T.plusSeconds(5));
        assertThat(ScheduleCalculator.firstFire(ScheduleType.FIXED_RATE, null, 60L, null, T))
                .isEqualTo(T.plusSeconds(60));
    }

    @Test
    void nextFutureOccurrenceKeepsFixedRateAnchor() {
        // fired 130s late on a 60s cadence -> next stays on the grid at +180s
        assertThat(ScheduleCalculator.nextFutureOccurrence(fixedRate(60), T, T.plusSeconds(130)))
                .isEqualTo(T.plusSeconds(180));
        // on-time fire -> simply +rate
        assertThat(ScheduleCalculator.nextFutureOccurrence(fixedRate(60), T, T))
                .isEqualTo(T.plusSeconds(60));
    }

    @Test
    void nextFutureOccurrenceForCronAndOneShot() {
        assertThat(ScheduleCalculator.nextFutureOccurrence(cron("0 * * * * *"), T, T.plusSeconds(125)))
                .isEqualTo(T.plusSeconds(180));
        assertThat(ScheduleCalculator.nextFutureOccurrence(oneShot(), T, T.plusSeconds(10))).isNull();
    }

    @Test
    void occurrencesThroughEnumeratesMissedFires() {
        assertThat(ScheduleCalculator.occurrencesThrough(fixedRate(60), T, T.plusSeconds(130), 100))
                .containsExactly(T, T.plusSeconds(60), T.plusSeconds(120));
        assertThat(ScheduleCalculator.occurrencesThrough(cron("0 * * * * *"), T, T.plusSeconds(125), 100))
                .containsExactly(T, T.plusSeconds(60), T.plusSeconds(120));
        assertThat(ScheduleCalculator.occurrencesThrough(oneShot(), T, T.plusSeconds(500), 100))
                .containsExactly(T);
    }

    @Test
    void occurrencesThroughHonoursCap() {
        List<Instant> capped = ScheduleCalculator.occurrencesThrough(fixedRate(1), T, T.plusSeconds(3600), 5);
        assertThat(capped).hasSize(5).startsWith(T).endsWith(T.plusSeconds(4));
        // resume point after a capped batch
        assertThat(ScheduleCalculator.occurrenceAfter(fixedRate(1), T.plusSeconds(4)))
                .isEqualTo(T.plusSeconds(5));
    }
}
