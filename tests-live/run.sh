#!/usr/bin/env bash
# Live Murmur-SQL multi-process node checks with isolated node directories.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
scenario=${1:-all}
binary=${SPEDSQL_BIN:-"$root/tests-live/bin/testnode"}
tags=${SPEDSQL_TAGS:-"sqlite_preupdate_hook sqlite_fts5"}
race_args=()
if [[ ${SPEDSQL_RACE:-0} == 1 ]]; then
  race_args=(-race)
fi
runtime_root=${SPEDSQL_LIVE_RUNTIME:-"$root/tests-live/runtime"}
started_at=$SECONDS

echo "======================================================================"
echo "  MURMUR-SQL LIVE MULTI-PROCESS SCENARIO RUNNER"
echo "======================================================================"

# Reap leftover daemons rooted at OUR runtime dir on the way out (Ctrl-C,
# inter-scenario kills, CI cancel). Scoped by --config path so parallel
# runs with their own SPEDSQL_LIVE_RUNTIME roots are never touched.
# No-op on clean runs (in-test cleanup already reaped everything).
sweep_runtime_daemons() {
  local pids
  pids=$(pgrep -f "testnode agent --config $runtime_root" 2>/dev/null || true)
  if [[ -z "$pids" ]]; then
    return 0
  fi
  # shellcheck disable=SC2086
  kill -TERM $pids 2>/dev/null || true
  sleep 2
  pids=$(pgrep -f "testnode agent --config $runtime_root" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086
    kill -KILL $pids 2>/dev/null || true
  fi
}
trap sweep_runtime_daemons EXIT INT TERM

# Compile testnode fixture binary to ensure it matches current sources & tags
echo "Compiling testnode fixture binary at $binary with tags: '$tags'..."
mkdir -p "$(dirname "$binary")"
(cd "$root" && go build "${race_args[@]}" -tags "$tags" -o "$binary" ./tests-live/harness/testnode)

scenario_timeout() {
  # go test timeouts sized for each scenario's longest routine form
  # (the soak target extends durations via env; see below).
  case "$1" in
    soak-slo) echo "3h" ;;
    long-running-five-node) echo "75m" ;;
    files-soak) echo "15m" ;;
    snapshot-resync) echo "10m" ;;
    crash-recovery|chaos-load) echo "6m" ;;
    allow-nodes|benchmark) echo "8m" ;;
    compression) echo "15m" ;;
    *) echo "10m" ;;
  esac
}

run_scenario() {
  local scn=$1
  echo ""
  echo ">>> RUNNING LIVE SCENARIO: $scn"
  # Under -race the compression matrix exceeds 10 m; pass -short so
  # TestPebbleCompressionSizes skips and the suite finishes cleanly.
  local short_args=()
  if [[ ${#race_args[@]} -gt 0 && "$scn" == "compression" ]]; then
    short_args=(-short)
  fi
  (cd "$root" && go test "${race_args[@]}" -tags "$tags" -v -count=1 -timeout="$(scenario_timeout "$scn")" "${short_args[@]}" "./tests-live/$scn")
  echo ">>> SCENARIO COMPLETED: $scn"
}

ALL_SCENARIOS="addrpolicy allow-nodes api-mtls backup-restore benchmark bridge-two-streams chaos-load churn-retirement compression crash-recovery crdt-contention encryption files-bridge files-soak gc-balance graceful-shutdown highlow large-payload loadshare long-running-five-node migration-crash overload-budgets partial-mesh partition pause-resume plumtree-live rekey release-upgrade rolling-restart scale-mesh schema-evolution snapshot-resync soak-slo subscribe three-node-sync version-skew views write-priority"

# Release-gate subset, run by CI on every push/PR against the freshly built
# daemon binary: smoke, encryption, backup/restore, partitions, version
# skew, schema upgrades, release upgrades (previous-release binaries),
# High/Low, files, scale mesh, snapshot resync, GC balance, rolling
# restart, crash recovery, discovery mesh, migration crash,
# pause/resume, and graceful shutdown. Fast suites first.
GATE_SCENARIOS="api-mtls three-node-sync encryption backup-restore partition version-skew schema-evolution release-upgrade highlow files-bridge files-soak scale-mesh subscribe loadshare migration-crash pause-resume partial-mesh rolling-restart snapshot-resync gc-balance crash-recovery chaos-load graceful-shutdown"

case "$scenario" in
  all)
    for scn in $ALL_SCENARIOS; do
      run_scenario "$scn"
    done
    ;;
  gate)
    for scn in $GATE_SCENARIOS; do
      run_scenario "$scn"
    done
    ;;
  soak)
    # Long acceptance runs, scheduled separately (CI cron + manual dispatch):
    # ten-minute file soak, two-hour write SLO, one-hour five-node mesh.
    SPEDSQL_FILES_SOAK_DURATION_SECONDS=600 SPEDSQL_FILES_SOAK_INTERVAL_SECONDS=60 run_scenario "files-soak"
    SPEDSQL_SLO_DURATION_SECONDS=7200 SPEDSQL_SLO_SETTLE_SECONDS=300 run_scenario "soak-slo"
    SPEDSQL_FIVE_NODE_DURATION_SECONDS=3600 SPEDSQL_FIVE_NODE_SETTLE_SECONDS=300 run_scenario "long-running-five-node"
    ;;
  stress)
    # Concurrency-repetition lane, scheduled separately (CI cron + manual
    # dispatch), NOT per-PR: the 1-in-15 deadlocks this repo has hit (session
    # close/send ABBA, dual AcceptStream race) only reproduce under
    # repetition. SPEDSQL_STRESS_COUNT overrides the repeat count.
    stress_count=${SPEDSQL_STRESS_COUNT:-5}
    for scn in rolling-restart partial-mesh crash-recovery partition snapshot-resync; do
      echo ""
      echo ">>> STRESS $scn x$stress_count"
      (cd "$root" && go test "${race_args[@]}" -tags "$tags" -count="$stress_count" -timeout="$(scenario_timeout "$scn")" "./tests-live/$scn")
    done
    echo ""
    echo ">>> STRESS rolling-restart x2 under -race"
    (cd "$root" && go test -race -tags "$tags" -count=2 -timeout=15m "./tests-live/rolling-restart")
    ;;
  *)
    if [[ " $ALL_SCENARIOS " == *" $scenario "* ]]; then
      run_scenario "$scenario"
    else
      echo "Unknown scenario: $scenario" >&2
      echo "Available scenarios: $ALL_SCENARIOS all gate soak stress" >&2
      exit 1
    fi
    ;;
esac

echo ""
echo "======================================================================"
echo "  ALL REQUESTED LIVE SCENARIOS PASSED (Elapsed: $((SECONDS - started_at))s)"
echo "======================================================================"
