-- M3: worker membership + shard assignment, all coordinated through Postgres.

CREATE TABLE worker_registry (
    worker_id      VARCHAR(128) PRIMARY KEY,
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- assignments are stamped with the issuing leader's epoch (the fencing token);
-- writes carrying a lower epoch are rejected (see ShardAssignmentStore)
CREATE TABLE shard_assignment (
    shard       SMALLINT PRIMARY KEY,
    worker_id   VARCHAR(128) NOT NULL,
    epoch       BIGINT       NOT NULL,
    assigned_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- fires carry the fencing token they were claimed under (audit + chaos assertions)
ALTER TABLE execution ADD COLUMN fence_epoch BIGINT;
