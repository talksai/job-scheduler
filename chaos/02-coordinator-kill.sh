#!/usr/bin/env bash
# CHAOS 02 — kill the coordinator (SIGKILL).
# With four candidates the new leader is nondeterministic — the invariants are:
# SOMEONE else wins the lease at epoch+1 within the failover SLO; a recurring job
# keeps firing with zero duplicate occurrences; the dead node rejoins cleanly.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "=== CHAOS 02: kill the coordinator ==="
stack_stable

LEADER=$(leader_worker)
LEADER_SVC=$(service_of "$LEADER")
BYSTANDER=$(other_worker "$LEADER")   # a surviving node for API calls
API_PORT=$(port_of "$BYSTANDER")
E0=$(lease_epoch)
echo "  leader=$LEADER (epoch $E0); API via $BYSTANDER"

TYPE="chaos-coord-$(date +%s)"
TICKER=$(create_ticker "$API_PORT" "$TYPE" 2)
wait_until 15 "ticker fired at least twice before the kill" \
  bash -c "[[ \$($COMPOSE exec -T postgres psql -U scheduler -d scheduler -tAc \"SELECT count(*) FROM execution WHERE job_id='$TICKER'\") -ge 2 ]]"

$COMPOSE kill "$LEADER_SVC" >/dev/null 2>&1
KILL_TS=$(date +%s)
echo "  SIGKILL to $LEADER_SVC"

wait_until 15 "another node wins the lease" leader_changed "$LEADER"
FAILOVER=$(( $(date +%s) - KILL_TS ))
NEW_LEADER=$(leader_worker)
assert_eq "$(lease_epoch)" "$((E0 + 1))" "fencing token strictly increased"
echo "  new leader: $NEW_LEADER · kill-to-new-leader: ${FAILOVER}s (SLO < 10s)"
[[ $FAILOVER -lt 10 ]] || { echo "  ✗ failover exceeded SLO" >&2; exit 1; }

N1=$(exec_count "$TICKER")
wait_until 20 "ticker fired >= 2 more under the new coordinator" \
  bash -c "[[ \$($COMPOSE exec -T postgres psql -U scheduler -d scheduler -tAc \"SELECT count(*) FROM execution WHERE job_id='$TICKER'\") -ge $((N1 + 2)) ]]"

DUPS=$(psql_query "SELECT count(*) FROM (SELECT job_id, fire_epoch FROM execution GROUP BY 1,2 HAVING count(*) > 1) d")
assert_eq "$DUPS" "0" "no double-fire across the failover"
echo "  coordinator-failover timer (new leader's view): $(metric_of "$(port_of "$NEW_LEADER")" scheduler_failover_coordinator_seconds_max)s"

cancel_job "$API_PORT" "$TICKER"
$COMPOSE up -d "$LEADER_SVC" >/dev/null 2>&1
wait_until 180 "old leader revived and fleet rebalanced evenly" fair_split
echo "  fleet after revival: $(print_split)"
echo "=== CHAOS 02 PASS ==="
