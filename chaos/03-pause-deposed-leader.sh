#!/usr/bin/env bash
# CHAOS 03 — pause the leader past its lease TTL, then resume it.
# This is Kleppmann's GC-pause / stale-clock scenario (containers share the host
# kernel clock, so a freeze-and-resume IS the clock-skew equivalent): the deposed
# leader wakes up believing it still leads, and every path it could use to do
# damage must be fenced by the higher epoch.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "=== CHAOS 03: pause the leader past its lease (deposed-leader / clock-skew) ==="
stack_stable

LEADER=$(leader_worker)
LEADER_SVC=$(service_of "$LEADER")
LEADER_PORT=$(port_of "$LEADER")
BYSTANDER=$(other_worker "$LEADER")
API_PORT=$(port_of "$BYSTANDER")
E0=$(lease_epoch)
echo "  leader=$LEADER (epoch $E0); API via $BYSTANDER"

TYPE="chaos-pause-$(date +%s)"
TICKER=$(create_ticker "$API_PORT" "$TYPE" 2)

$COMPOSE pause "$LEADER_SVC" >/dev/null 2>&1
echo "  paused $LEADER_SVC (freeze > lease TTL)"

wait_until 15 "another node takes the lease" leader_changed "$LEADER"
NEW_LEADER=$(leader_worker)
assert_eq "$(lease_epoch)" "$((E0 + 1))" "new fencing token issued"
echo "  new leader: $NEW_LEADER"
wait_until 30 "frozen worker's shards redistributed to the survivors" \
  check_count "SELECT count(*) FROM shard_assignment WHERE worker_id='$LEADER' AND shard < $TOTAL_SHARDS" 0

sleep 3
$COMPOSE unpause "$LEADER_SVC" >/dev/null 2>&1
echo "  unpaused $LEADER_SVC — it resumes with stale epoch $E0 and stale shard ownership"

sleep 6  # give the zombie every chance to do damage
assert_eq "$(leader_worker)" "$NEW_LEADER" "deposed leader did NOT steal the lease back"
assert_eq "$(lease_epoch)" "$((E0 + 1))" "epoch neither regressed nor was re-taken"
assert_eq "$(metric_of "$LEADER_PORT" scheduler_coordinator_leader)" "0.0" "deposed instance demoted itself"

DUPS=$(psql_query "SELECT count(*) FROM (SELECT job_id, fire_epoch FROM execution GROUP BY 1,2 HAVING count(*) > 1) d")
assert_eq "$DUPS" "0" "no double-fire despite the zombie's stale wheel"

# liveness: firing must have resumed and keep progressing after the episode
N=$(exec_count "$TICKER")
wait_until 15 "ticker keeps firing after the episode (>$N occurrences, all exactly-once)" \
  bash -c "[[ \$($COMPOSE exec -T postgres psql -U scheduler -d scheduler -tAc \"SELECT count(*) FROM execution WHERE job_id='$TICKER'\") -ge $((N + 2)) ]]"

cancel_job "$API_PORT" "$TICKER"
wait_until 180 "rejoined worker rebalanced back into the fleet" fair_split
echo "  fleet after rejoin: $(print_split)"
echo "=== CHAOS 03 PASS ==="
