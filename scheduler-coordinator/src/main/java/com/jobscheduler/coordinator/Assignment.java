package com.jobscheduler.coordinator;

/** A shard owned by a worker, stamped with the fencing token of the leader that issued it. */
public record Assignment(int shard, long epoch) {
}
