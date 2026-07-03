-- M4: every fire carries a traceId from claim through outbox to the consumer,
-- surfaced as the x-trace-id Kafka header for cross-component log correlation.
ALTER TABLE outbox ADD COLUMN trace_id VARCHAR(64);
