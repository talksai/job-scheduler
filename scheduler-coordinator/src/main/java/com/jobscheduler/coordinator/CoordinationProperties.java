package com.jobscheduler.coordinator;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "scheduler.coordination")
public record CoordinationProperties(
        @DefaultValue("1000") long electionIntervalMs,
        @DefaultValue("10") long leaseTtlSeconds,
        @DefaultValue("1000") long heartbeatIntervalMs,
        @DefaultValue("5") long workerTtlSeconds,
        @DefaultValue("1000") long assignmentPollMs) {
}
