#!/usr/bin/env bash
# CHAOS 01 — kill a worker (SIGKILL) mid-execution.
# Invariants: its shards reassign to the survivor within the SLO; every job
# completes exactly once (no loss, no duplicates, no ghost outbox rows).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "=== CHAOS 01: kill a worker mid-execution ==="
stack_stable

LEADER=$(leader_worker)
VICTIM=$(other_worker "$LEADER")      # kill the non-leader so this tests pure worker failover
VICTIM_SVC=$(service_of "$VICTIM")
SURVIVOR_PORT=$(port_of "$LEADER")
TYPE="chaos-worker-$(date +%s)"
echo "  leader=$LEADER, victim=$VICTIM ($VICTIM_SVC)"

for i in $(seq 1 20); do create_one_shot "$SURVIVOR_PORT" "$TYPE" $((3 + i % 6)); done
echo "  seeded 20 one-shot jobs due in 3-8s"

sleep 4  # some fired, some in flight, some still pending on the victim's shards
$COMPOSE kill "$VICTIM_SVC" >/dev/null 2>&1
KILL_TS=$(date +%s)
echo "  SIGKILL to $VICTIM_SVC"

wait_until 30 "all $TOTAL_SHARDS shards reassigned to $LEADER" \
  check_count "SELECT count(*) FROM shard_assignment WHERE worker_id='$LEADER' AND shard < $TOTAL_SHARDS" "$TOTAL_SHARDS"
echo "  shards reassigned $(( $(date +%s) - KILL_TS ))s after kill"

wait_until 45 "all 20 jobs COMPLETED on the survivor" \
  check_count "SELECT count(*) FROM job WHERE type='$TYPE' AND state='COMPLETED'" 20
wait_until 45 "all 20 effects applied by the consumer (incl. Kafka group rebalance)" \
  check_count "SELECT count(*) FROM execution e JOIN job j ON j.id=e.job_id WHERE j.type='$TYPE' AND e.status='COMPLETED'" 20

assert_exactly_once "$TYPE" 20
echo "  worker-failover timer (survivor's view): $(metric_of "$SURVIVOR_PORT" scheduler_failover_worker_seconds_max)s"

$COMPOSE up -d "$VICTIM_SVC" >/dev/null 2>&1
wait_until 120 "victim revived and shards rebalanced 8/8" shards_split
echo "=== CHAOS 01 PASS ==="
