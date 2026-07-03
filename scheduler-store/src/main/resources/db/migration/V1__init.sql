-- Schema per ARCHITECTURE.md §2

-- jobs: the schedule definitions
CREATE TABLE job (
    id                 UUID PRIMARY KEY,
    type               VARCHAR(64)  NOT NULL,
    payload            TEXT,
    schedule_type      VARCHAR(16)  NOT NULL,  -- CRON | FIXED_RATE | ONE_SHOT
    cron_expression    VARCHAR(120),
    fixed_rate_seconds BIGINT,
    fire_at            TIMESTAMPTZ,
    missed_fire_policy VARCHAR(32)  NOT NULL DEFAULT 'FIRE_ONCE_IMMEDIATELY',
    shard              SMALLINT     NOT NULL,
    next_fire_at       TIMESTAMPTZ,
    state              VARCHAR(16)  NOT NULL DEFAULT 'PENDING',  -- PENDING | COMPLETED | CANCELLED
    version            BIGINT       NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- the due-index: partial so it only covers fireable rows
CREATE INDEX idx_due ON job (shard, next_fire_at) WHERE state = 'PENDING';

-- executions: one row per (job, fire instant) — the idempotency anchor
CREATE TABLE execution (
    job_id     UUID        NOT NULL,
    fire_epoch BIGINT      NOT NULL,  -- scheduled fire instant, epoch seconds
    status     VARCHAR(16) NOT NULL,  -- CLAIMED | FIRED | FAILED
    claimed_by VARCHAR(128),
    claimed_at TIMESTAMPTZ,
    attempt    INT         NOT NULL DEFAULT 1,
    PRIMARY KEY (job_id, fire_epoch)
);

-- outbox: atomic publish (written in the SAME tx as the execution state change; poller lands in M2)
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(64)  NOT NULL,
    topic        VARCHAR(128) NOT NULL,
    message_key  VARCHAR(128) NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

-- consumer-side dedup
CREATE TABLE processed_event (
    event_key    VARCHAR(160) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- coordinator lease: leader election + fencing token (epoch), no external system needed
CREATE TABLE coordinator_lease (
    role       VARCHAR(32) PRIMARY KEY,
    holder     VARCHAR(128),
    epoch      BIGINT      NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ
);

INSERT INTO coordinator_lease (role, holder, epoch, expires_at)
VALUES ('coordinator', NULL, 0, NULL);
