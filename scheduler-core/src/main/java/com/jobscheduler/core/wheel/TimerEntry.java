package com.jobscheduler.core.wheel;

import java.util.UUID;

/** One pending fire in the wheel. deadlineMs is the scheduled fire instant in epoch millis. */
public record TimerEntry(UUID jobId, long deadlineMs) {
}
