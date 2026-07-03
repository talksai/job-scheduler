# Chaos suite

Automated fault injection against the two-instance compose stack, with invariant
assertions straight from Postgres. Every script exits non-zero on a violated invariant.

```bash
./chaos/run-all.sh          # brings the stack up (with the app2 overlay) and runs all three
```

| Scenario | Fault | Invariants asserted |
|---|---|---|
| [01-worker-kill](01-worker-kill.sh) | `SIGKILL` the non-leader worker while 20 jobs fire | shards reassigned to the survivor (< worker TTL + sweep); every job completes **exactly once** (execution rows == jobs == applied effects, zero duplicate `(job_id, fire_epoch)`); no ghost outbox rows |
| [02-coordinator-kill](02-coordinator-kill.sh) | `SIGKILL` the lease holder while a 2s ticker runs | survivor wins the lease **< 10 s** with epoch **+1**; ticker keeps firing, zero double-fires |
| [03-pause-deposed-leader](03-pause-deposed-leader.sh) | `docker pause` the leader past its lease TTL, then resume | the resumed zombie **cannot** reclaim the lease, the epoch never regresses, it demotes itself, its stale wheel causes zero double-fires, firing liveness resumes |

**On "clock skew":** containers share the host kernel clock, so scenario 03 is the
skew-equivalent — a paused process resumes with a stale view of time (Kleppmann's
GC-pause), which is exactly the failure mode fencing tokens exist to kill.

## Last run (2026-07-03) — all PASS

- Worker kill: shards reassigned **4–6 s** after SIGKILL (failover timer 0.9 s), 20/20 exactly-once.
- Coordinator kill: kill-to-new-leader **4–6 s** (post-expiry election gap 0.2–0.9 s), epoch bumped, no double-fire.
- Deposed leader: fenced on every path, zero double-fires from the stale wheel.
