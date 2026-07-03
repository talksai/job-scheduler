package com.jobscheduler.execution;

import java.time.Instant;
import java.util.UUID;

/** The wire event on job-events. eventKey (jobId:fireEpoch) is the consumer-side dedup key. */
public record JobFiredEvent(UUID jobId, String type, String payload, long fireEpoch,
                            Instant scheduledAt, Instant firedAt) {

    public String eventKey() {
        return jobId + ":" + fireEpoch;
    }
}
