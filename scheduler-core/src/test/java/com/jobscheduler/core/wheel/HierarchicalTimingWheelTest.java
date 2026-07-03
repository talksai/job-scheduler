package com.jobscheduler.core.wheel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HierarchicalTimingWheelTest {

    private static final long TICK = 100;
    private static final int SIZE = 8;

    private static TimerEntry entry(long deadlineMs) {
        return new TimerEntry(UUID.randomUUID(), deadlineMs);
    }

    @Test
    void expiresEntryAtItsTick() {
        HierarchicalTimingWheel wheel = new HierarchicalTimingWheel(TICK, SIZE, 0);
        TimerEntry e = entry(300);
        assertThat(wheel.add(e)).isTrue();

        assertThat(wheel.advanceTo(299)).isEmpty();
        assertThat(wheel.advanceTo(300)).containsExactly(e);
        assertThat(wheel.size()).isZero();
    }

    @Test
    void alreadyDueAddReturnsFalse() {
        HierarchicalTimingWheel wheel = new HierarchicalTimingWheel(TICK, SIZE, 1000);
        assertThat(wheel.add(entry(1000))).isFalse();
        assertThat(wheel.add(entry(950))).isFalse();
        assertThat(wheel.add(entry(1100))).isTrue();
    }

    @Test
    void farDeadlineCascadesFromOverflowWheel() {
        HierarchicalTimingWheel wheel = new HierarchicalTimingWheel(TICK, SIZE, 0);
        // beyond tick*size = 800ms horizon -> overflow wheel
        TimerEntry far = entry(1500);
        assertThat(wheel.add(far)).isTrue();
        assertThat(wheel.size()).isEqualTo(1);

        assertThat(wheel.advanceTo(1400)).isEmpty();
        assertThat(wheel.advanceTo(1500)).containsExactly(far);
    }

    @Test
    void veryFarDeadlineTraversesTwoOverflowLevels() {
        HierarchicalTimingWheel wheel = new HierarchicalTimingWheel(TICK, SIZE, 0);
        // beyond 800ms * 8 = 6400ms second-level horizon
        TimerEntry veryFar = entry(50_000);
        assertThat(wheel.add(veryFar)).isTrue();

        assertThat(wheel.advanceTo(49_900)).isEmpty();
        assertThat(wheel.advanceTo(50_000)).containsExactly(veryFar);
    }

    @Test
    void wrapAroundEntryWaitsForItsRotation() {
        HierarchicalTimingWheel wheel = new HierarchicalTimingWheel(TICK, SIZE, 0);
        TimerEntry near = entry(200);
        TimerEntry oneRotationLater = entry(200 + TICK * SIZE); // same bucket index, next rotation
        wheel.add(near);
        wheel.add(oneRotationLater);

        assertThat(wheel.advanceTo(200)).containsExactly(near);
        assertThat(wheel.advanceTo(900)).isEmpty();
        assertThat(wheel.advanceTo(1000)).containsExactly(oneRotationLater);
    }

    @Test
    void manyRandomTimersAllExpireWithinOneTickOfDeadline() {
        long start = 12_345; // deliberately not tick-aligned
        HierarchicalTimingWheel wheel = new HierarchicalTimingWheel(TICK, SIZE, start);
        Random random = new Random(42);
        Map<UUID, Long> deadlines = new HashMap<>();
        for (int i = 0; i < 2_000; i++) {
            long deadline = start + 1 + random.nextInt(30_000); // spans several overflow levels
            TimerEntry e = entry(deadline);
            if (wheel.add(e)) {
                deadlines.put(e.jobId(), deadline);
            }
        }
        assertThat(wheel.size()).isEqualTo(deadlines.size());

        List<TimerEntry> expired = new ArrayList<>();
        for (long now = start; now <= start + 31_000; now += 70) { // uneven steps on purpose
            for (TimerEntry e : wheel.advanceTo(now)) {
                expired.add(e);
                // bucket granularity: never later than deadline + one tick, never
                // earlier than one tick before it (the service compensates sub-tick)
                assertThat(now + TICK).isGreaterThanOrEqualTo(e.deadlineMs());
                assertThat(now - 70).isLessThan(e.deadlineMs() + TICK);
            }
        }
        assertThat(expired).hasSize(deadlines.size());
        assertThat(wheel.size()).isZero();
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new HierarchicalTimingWheel(0, 8, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HierarchicalTimingWheel(100, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
