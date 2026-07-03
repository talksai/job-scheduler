# SLOs — targets & metric mapping

Source of truth for targets: `projects/02-job-scheduler/ARCHITECTURE.md` §4.

| SLO | Target | Prometheus series |
|---|---|---|
| Scheduling accuracy p99 (scheduled→actual fire) | ≤ 500 ms (measured 120 ms @ 1k fires in the M1 test) | `scheduler_fire_latency_seconds{quantile="0.99"}` |
| Exactly-once effect | 100% — zero duplicate side-effects under chaos | `scheduler_events_dedup_total` (absorbed replays) + chaos reconciliation (M5) |
| Coordinator failover, no double-fire | < 10 s | lease epoch gauge + failover timer (M3/M4) |
| Worker failover, no loss | < 15 s | assignment/rebalance metrics (M3/M4) |
| DLQ rate | < 0.1 % | `rate(scheduler_events_dlq_total[5m]) / rate(scheduler_events_consumed_total[5m])` |

## Metrics emitted today (M0–M4)

| Meter | Type | Meaning |
|---|---|---|
| `scheduler.jobs.created` | counter | jobs accepted via REST |
| `scheduler.jobs.fired` | counter | fires claimed + executed |
| `scheduler.fire.latency` | timer (p50/p95/p99) | scheduled-vs-actual latency, on-time fires only |
| `scheduler.fires.skipped` | counter | late fires dropped by the SKIP policy |
| `scheduler.fires.catchup` | counter | late fires executed by FIRE_ONCE_IMMEDIATELY / FIRE_ALL |
| `scheduler.wheel.timers` | gauge | timers parked in the in-memory wheel |
| `scheduler.wheel.hydrated` | counter | timers loaded into the wheel from Postgres |
| `scheduler.db.pending` | gauge | PENDING jobs in Postgres (durable timers) |
| `scheduler.outbox.unpublished` | gauge | outbox rows awaiting publish (outbox lag) |
| `scheduler.events.published` | counter | events produced to `job-events` by the outbox poller |
| `scheduler.events.consumed` | counter | events consumed and applied (first delivery) |
| `scheduler.events.dedup` | counter | duplicate deliveries absorbed by `processed_event` |
| `scheduler.events.retried` | counter | events bounced to `job-events.retry` |
| `scheduler.events.dlq` | counter | events dead-lettered |
| `scheduler.lease.epoch` | gauge | highest coordinator lease epoch seen (fencing token) |
| `scheduler.coordinator.leader` | gauge | 1 when this node holds the coordinator lease |
| `scheduler.shards.owned` | gauge | shards assigned to this worker |
| `scheduler.assignments.rebalanced` | counter | shard assignment changes written by the coordinator |
| `scheduler.fencing.rejected` | counter | writes rejected for carrying a stale fencing token |
| `scheduler.failover.coordinator` | timer | lease-expiry → new-leader-elected gap (SLO < 10 s) |
| `scheduler.failover.worker` | timer | worker-death detection → shard reassignment (SLO < 15 s) |
| `scheduler.execution.lag` | timer (p50/p95/p99) | fired → effect-applied end-to-end lag |

Scrape endpoint: `/actuator/prometheus`.
Dashboards: `deploy/grafana/dashboards/scheduler-overview.json` + `scheduler-coordination.json`.
Alert rules (one per SLO + operational guards): `deploy/prometheus/alerts.yml`.
Tracing: every fire carries a traceId (event field + `x-trace-id` Kafka header) from claim through consumer logs.
