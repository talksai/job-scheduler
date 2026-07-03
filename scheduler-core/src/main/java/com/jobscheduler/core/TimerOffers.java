package com.jobscheduler.core;

import java.time.Instant;
import java.util.UUID;

/**
 * Push-side entry point to the wheel: lets the API hand a freshly created job
 * straight to due-detection instead of waiting for the next hydration sweep.
 */
public interface TimerOffers {

    void offer(UUID jobId, Instant nextFireAt);
}
