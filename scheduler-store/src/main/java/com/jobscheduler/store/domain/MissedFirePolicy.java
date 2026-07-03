package com.jobscheduler.store.domain;

/** What to do with fires that were missed while the scheduler was down. Enforced from M1. */
public enum MissedFirePolicy {
    FIRE_ONCE_IMMEDIATELY,
    SKIP,
    FIRE_ALL
}
