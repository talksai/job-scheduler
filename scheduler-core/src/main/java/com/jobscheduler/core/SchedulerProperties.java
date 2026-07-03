package com.jobscheduler.core;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        @DefaultValue("worker-local") String workerId,
        @DefaultValue("16") int shards,
        @DefaultValue("500") long pollIntervalMs,
        @DefaultValue("100") int batchSize,
        @DefaultValue("true") boolean pollerEnabled) {
}
