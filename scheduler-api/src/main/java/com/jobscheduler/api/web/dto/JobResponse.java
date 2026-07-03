package com.jobscheduler.api.web.dto;

import com.jobscheduler.store.domain.JobState;
import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.domain.ScheduleType;
import com.jobscheduler.store.entity.Job;

import java.time.Instant;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String type,
        String payload,
        ScheduleType scheduleType,
        String cronExpression,
        Long fixedRateSeconds,
        Instant fireAt,
        MissedFirePolicy missedFirePolicy,
        Short shard,
        Instant nextFireAt,
        JobState state,
        Instant createdAt) {

    public static JobResponse from(Job job) {
        return new JobResponse(job.getId(), job.getType(), job.getPayload(), job.getScheduleType(),
                job.getCronExpression(), job.getFixedRateSeconds(), job.getFireAt(), job.getMissedFirePolicy(),
                job.getShard(), job.getNextFireAt(), job.getState(), job.getCreatedAt());
    }
}
