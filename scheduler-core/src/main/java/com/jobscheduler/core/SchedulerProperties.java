package com.jobscheduler.core;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        @DefaultValue("worker-local") String workerId,
        @DefaultValue("16") int shards,
        @DefaultValue("100") long tickMs,
        @DefaultValue("64") int wheelSize,
        @DefaultValue("5000") long hydrationIntervalMs,
        @DefaultValue("60000") long hydrationWindowMs,
        @DefaultValue("10000") int hydrationBatchSize,
        @DefaultValue("5000") long missedFireThresholdMs,
        @DefaultValue("100") int maxCatchupFires,
        @DefaultValue("true") boolean wheelEnabled) {
}
