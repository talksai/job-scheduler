package com.jobscheduler.core.wheel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Hierarchical timing wheel (Varghese &amp; Lauck — the Kafka/Netty approach).
 *
 * <p>Level 0 buckets span one tick each; deadlines beyond {@code tickMs * wheelSize}
 * go to a lazily-created overflow wheel whose tick is this wheel's full interval.
 * When the clock crosses an interval boundary the overflow bucket cascades back
 * down and its entries land in precise level-0 buckets. add() and advanceTo()
 * are O(1) amortized per entry — no priority queue, no per-tick scan of all timers.
 *
 * <p>Volatile by design: the durable far horizon lives in Postgres (idx_due) and
 * the wheel is rebuilt from it on startup. NOT thread-safe — callers synchronize.
 */
public final class HierarchicalTimingWheel {

    private final long tickMs;
    private final int wheelSize;
    private final long intervalMs;
    private final ArrayDeque<TimerEntry>[] buckets;

    private long currentTick;
    private int localSize;
    private HierarchicalTimingWheel overflow;

    @SuppressWarnings("unchecked")
    public HierarchicalTimingWheel(long tickMs, int wheelSize, long startTimeMs) {
        if (tickMs <= 0 || wheelSize <= 1) {
            throw new IllegalArgumentException("tickMs must be > 0 and wheelSize > 1");
        }
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.intervalMs = tickMs * wheelSize;
        this.buckets = new ArrayDeque[wheelSize];
        for (int i = 0; i < wheelSize; i++) {
            buckets[i] = new ArrayDeque<>();
        }
        this.currentTick = startTimeMs / tickMs;
    }

    /**
     * @return false when the deadline is already due — the caller must fire it
     *         immediately instead of parking it in the wheel.
     */
    public boolean add(TimerEntry entry) {
        long deadlineTick = entry.deadlineMs() / tickMs;
        if (deadlineTick <= currentTick) {
            return false;
        }
        if (deadlineTick < currentTick + wheelSize) {
            buckets[(int) (deadlineTick % wheelSize)].add(entry);
            localSize++;
            return true;
        }
        if (overflow == null) {
            overflow = new HierarchicalTimingWheel(intervalMs, wheelSize, currentTimeMs());
        }
        // Beyond the fine horizon the coarse bucket is always strictly future,
        // so the overflow add cannot report already-due.
        return overflow.add(entry);
    }

    /** Advance the clock to nowMs and return every expired entry. */
    public List<TimerEntry> advanceTo(long nowMs) {
        List<TimerEntry> expired = new ArrayList<>();
        long targetTick = nowMs / tickMs;
        while (currentTick < targetTick) {
            currentTick++;
            ArrayDeque<TimerEntry> bucket = buckets[(int) (currentTick % wheelSize)];
            if (!bucket.isEmpty()) {
                List<TimerEntry> drained = new ArrayList<>(bucket);
                bucket.clear();
                localSize -= drained.size();
                for (TimerEntry entry : drained) {
                    if (entry.deadlineMs() / tickMs <= currentTick) {
                        expired.add(entry);
                    } else {
                        add(entry); // a future rotation wrapped onto this index — repark
                    }
                }
            }
            if (overflow != null && currentTick % wheelSize == 0) {
                for (TimerEntry entry : overflow.advanceTo(currentTick * tickMs)) {
                    if (!add(entry)) {
                        expired.add(entry);
                    }
                }
            }
        }
        return expired;
    }

    public int size() {
        return localSize + (overflow == null ? 0 : overflow.size());
    }

    public long currentTimeMs() {
        return currentTick * tickMs;
    }
}
