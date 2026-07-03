package com.jobscheduler.coordinator;

import java.util.Set;

/** The shards this worker currently owns, and the fencing token they were granted under. */
public interface ShardOwnership {

    Set<Integer> ownedShards();

    /** The highest epoch across this worker's assignments; 0 until assigned. */
    long epoch();
}
