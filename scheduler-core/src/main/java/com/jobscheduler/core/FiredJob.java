package com.jobscheduler.core;

import java.time.Instant;
import java.util.UUID;

/** A claimed fire that must now be published. (jobId, fireEpoch) is the exactly-once key. */
public record FiredJob(UUID jobId, String type, String payload, long fireEpoch, Instant scheduledAt) {
}
