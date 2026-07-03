package com.jobscheduler.core;

import java.time.Instant;
import java.util.UUID;

/**
 * A claimed fire that must now be published. (jobId, fireEpoch) is the exactly-once key.
 * late marks catch-up fires (missed-fire policies) that are excluded from the accuracy SLO.
 * traceId follows the fire through outbox, Kafka and the consumer for log correlation.
 */
public record FiredJob(UUID jobId, String type, String payload, long fireEpoch, Instant scheduledAt,
                       boolean late, String traceId) {
}
