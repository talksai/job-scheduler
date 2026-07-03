package com.jobscheduler.api.web.dto;

import com.jobscheduler.store.domain.MissedFirePolicy;
import com.jobscheduler.store.domain.ScheduleType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;

public record CreateJobRequest(
        @NotBlank String type,
        String payload,
        @NotNull ScheduleType scheduleType,
        String cronExpression,
        @Positive Long fixedRateSeconds,
        Instant fireAt,
        MissedFirePolicy missedFirePolicy) {
}
