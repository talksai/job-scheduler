package com.jobscheduler.store.repo;

import java.time.Instant;
import java.util.UUID;

/** A hydration candidate: a pending job due within the wheel's window. */
public record DueTimer(UUID jobId, Instant nextFireAt) {
}
