#!/usr/bin/env bash
# Live SPeD-SQL multi-process node checks with isolated node directories.
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
echo "  SPeD-SQL LIVE MULTI-PROCESS SCENARIO RUNNER"
echo "======================================================================"

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
  (cd "$root" && go test "${race_args[@]}" -tags "$tags" -v -count=1 -timeout="$(scenario_timeout "$scn")" "./tests-live/$scn")
  echo ">>> SCENARIO COMPLETED: $scn"
}

ALL_SCENARIOS="addrpolicy allow-nodes api-mtls backup-restore benchmark bridge-two-streams chaos-load churn-retirement compression crash-recovery crdt-contention encryption files-bridge files-soak gc-balance highlow large-payload loadshare long-running-five-node overload-budgets partial-mesh partition plumtree-live rekey release-upgrade rolling-restart scale-mesh schema-evolution snapshot-resync soak-slo subscribe three-node-sync version-skew views write-priority"

# Release-gate subset, run by CI on every push/PR against the freshly built
# daemon binary: smoke, encryption, backup/restore, partitions, version
# skew, schema upgrades, release upgrades (previous-release binaries),
# High/Low, files, scale mesh, snapshot resync, GC balance, rolling
# restart, and crash recovery. Fast suites first.
GATE_SCENARIOS="api-mtls three-node-sync encryption backup-restore partition version-skew schema-evolution release-upgrade highlow files-bridge files-soak scale-mesh subscribe loadshare rolling-restart snapshot-resync gc-balance crash-recovery chaos-load"

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
  *)
    if [[ " $ALL_SCENARIOS " == *" $scenario "* ]]; then
      run_scenario "$scenario"
    else
      echo "Unknown scenario: $scenario" >&2
      echo "Available scenarios: $ALL_SCENARIOS all gate soak" >&2
      exit 1
    fi
    ;;
esac

echo ""
echo "======================================================================"
echo "  ALL REQUESTED LIVE SCENARIOS PASSED (Elapsed: $((SECONDS - started_at))s)"
echo "======================================================================"
