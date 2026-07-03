# Chaos suite (M5)

Fault-injection scripts + invariant assertions land in M5:

- kill a worker mid-execution → assert exactly-once (reconciliation count = expected)
- kill the coordinator → assert failover + no double-fire (fencing)
- induce clock skew → assert scheduling invariants hold

Nothing here yet — see `projects/02-job-scheduler/BUILD-PLAN.md` M5.
