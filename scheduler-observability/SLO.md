# SLOs — targets & metric mapping

Source of truth for targets: `projects/02-job-scheduler/ARCHITECTURE.md` §4.

| SLO | Target | Prometheus series |
|---|---|---|
| Scheduling accuracy p99 (scheduled→actual fire) | ≤ 1 000 ms (M0, poll-based) → ≤ 500 ms (M1 timing wheel) | `scheduler_fire_latency_seconds{quantile="0.99"}` |
| Exactly-once effect | 100% — zero duplicate side-effects under chaos | chaos reconciliation (M5); dedup hits from M2 |
| Coordinator failover, no double-fire | < 10 s | lease epoch gauge + failover timer (M3/M4) |
| Worker failover, no loss | < 15 s | assignment/rebalance metrics (M3/M4) |
| DLQ rate | < 0.1 % | `rate(scheduler_events_dlq_total[5m]) / rate(scheduler_events_consumed_total[5m])` |

## Metrics emitted today (M0)

| Meter | Type | Meaning |
|---|---|---|
| `scheduler.jobs.created` | counter | jobs accepted via REST |
| `scheduler.jobs.fired` | counter | fires claimed + executed |
| `scheduler.fire.latency` | timer (p50/p95/p99) | scheduled-vs-actual fire latency |
| `scheduler.events.published` | counter | events produced to `job-events` |
| `scheduler.events.consumed` | counter | events consumed OK |
| `scheduler.events.retried` | counter | events bounced to `job-events.retry` |
| `scheduler.events.dlq` | counter | events dead-lettered |

Scrape endpoint: `/actuator/prometheus`. Dashboards: `deploy/grafana/dashboards/scheduler-overview.json`.
Remaining SLO meters (outbox lag, partition balance, lease epoch, failover timings) arrive with M2–M4.
