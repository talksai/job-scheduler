# Chaos suite

Automated fault injection against a **five-worker** compose fleet (worker-1..worker-5,
ports 8080–8084), with invariant assertions straight from Postgres. Every script exits
non-zero on a violated invariant. With four surviving candidates, the new leader after a
kill is nondeterministic — the scripts assert *properties* (someone else leads, the epoch
increased) rather than a specific winner.

```bash
./chaos/run-all.sh          # brings the fleet up (with the app2-app5 overlay) and runs all three
```

| Scenario | Fault | Invariants asserted |
|---|---|---|
| [01-worker-kill](01-worker-kill.sh) | `SIGKILL` a non-leader worker while 20 jobs fire | the victim's shards redistribute across the four survivors (victim owns 0 within the SLO); every job completes **exactly once** (execution rows == jobs == applied effects, zero duplicate `(job_id, fire_epoch)`); no ghost outbox rows; even rebalance after revival |
| [02-coordinator-kill](02-coordinator-kill.sh) | `SIGKILL` the lease holder while a 2s ticker runs | *some* survivor wins the lease **< 10 s** with epoch **+1**; ticker keeps firing, zero double-fires; clean rejoin |
| [03-pause-deposed-leader](03-pause-deposed-leader.sh) | `docker pause` the leader past its lease TTL, then resume | a new leader takes over at epoch+1 and the frozen node's shards redistribute; the resumed zombie **cannot** reclaim the lease, the epoch never regresses, it demotes itself, its stale wheel causes zero double-fires; firing liveness resumes |

**On "clock skew":** containers share the host kernel clock, so scenario 03 is the
skew-equivalent — a paused process resumes with a stale view of time (Kleppmann's
GC-pause), which is exactly the failure mode fencing tokens exist to kill.

## Last run (2026-07-03, 5-worker fleet) — all PASS

- Steady state: shards 4/3/3/3/3, one leader, all epochs equal.
- **Worker kill**: victim's shards spread over the 4 survivors in **6 s** (fleet 4/4/4/4;
  failover timer 0.44 s); 20/20 jobs exactly-once, zero ghost outbox rows; even 4/3/3/3/3
  rebalance after revival.
- **Coordinator kill**: worker-1 won the 4-candidate race in **4 s**, epoch 13→14, ticker
  never double-fired; clean rejoin.
- **Paused leader**: worker-4 took the lease at epoch+1 and absorbed the frozen node's
  shards; the resumed zombie could not reclaim the lease, demoted itself, zero double-fires,
  firing liveness confirmed after the episode.

See `interview-prep/04-run-and-observe.md` for how to watch a run live on the dashboards.
