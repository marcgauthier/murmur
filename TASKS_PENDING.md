# SPeD-SQL Pending Tasks

This document contains the list of tasks awaiting implementation.

## Multi-Agent Workflow
1. **Claiming a Task**: When an AI agent selects a task, it MUST delete the task from `TASKS_PENDING.md` and move it to [TASKS_INPROGRESS.md](TASKS_INPROGRESS.md), stating the name of the agent (and model) and the start timestamp before modifying code.
2. **Exclusivity**: Never pick up, modify, or collide with any task currently listed in [TASKS_INPROGRESS.md](TASKS_INPROGRESS.md).
3. **Completion**: When the task is implemented and all relevant tests pass, the agent MUST remove the task from [TASKS_INPROGRESS.md](TASKS_INPROGRESS.md) and move it to [TASKS_COMPLETED.md](TASKS_COMPLETED.md), marked with `[x]`, completion timestamp (`YYYY-MM-DD HH:MM:SS TZ`), a summary of changes, and the agent name/model.
4. **Cancellation**: If a task cannot be completed or work is aborted, remove it from [TASKS_INPROGRESS.md](TASKS_INPROGRESS.md) and return it to `TASKS_PENDING.md`.

## Pending Tasks


### Cleared areas

No pending items remain in the areas below; design links are kept for reference.

- Core Priorities and Testing
- Query Engine and Public API ([Overview](architecture/overview.md), [API and configuration](architecture/api-and-configuration.md), [Transactions](architecture/transactions.md#17-alternative-write-optimization))
- Membership and Dissemination ([Membership and transport](architecture/membership-and-transport.md), [Replication and dissemination](architecture/replication-and-dissemination.md))
- Synchronization, Overload, and Writer Scheduling ([Synchronization and overload](architecture/synchronization-and-overload.md))
- Snapshots, Retention, and Restore Safety ([Snapshots, backup, and restore](architecture/snapshots-backup-and-restore.md))
- High/Low Domain Bridge, Optional Extension ([High/Low replication](architecture/high-low-replication.md))
- Encrypted File Objects, Optional Extension ([Encrypted file replication](architecture/file-replication.md))
- Observability and Optional Service Adapters ([Runtime and diagnostics](architecture/runtime-and-diagnostics.md), [Query and search](architecture/query-and-search.md#reactive-query-subscriptions))
- Hardening, Benchmarking, and Release Acceptance ([Testing and acceptance](architecture/testing.md), [Benchmarks](architecture/benchmarks.md), [Versioning and release](architecture/versioning-and-release.md), [Storage](architecture/storage.md))

### Known follow-ups (from release-gate verification, 2026-09-28)

- CHAOS-HEAL-STATUS-STALL: `tests-live/chaos-load` hung once (~1/6 runs)
  with a `/v1/status` GET stuck 5+ minutes during heal while writes kept
  succeeding; one daemon later ignored SIGTERM (shutdown wedged). Daemons
  were idle-but-alive afterwards. Not reproduced under isolation (3/3
  passes since). Mitigation applied: the test's status poll now uses a
  5s-timeout client so a recurrence fails fast at the 30s assertion
  instead of the 6-minute `go test` timeout. Still needs product-side
  diagnosis (status-path contention under heal storm, shutdown hang).
- LIVE-RUNNER-ISOLATION: concurrent `run.sh` invocations on one checkout
  corrupt each other's runtime dirs (observed); documented
  `SPEDSQL_LIVE_RUNTIME` isolation. A locking or auto-isolation mechanism
  in the harness would remove the footgun.

### Sequencing notes

[Architecture index](architecture/README.md) · [Project README](README.md)

- Outstanding core work and the GALVANIZE comparison live in [capability gaps](architecture/capability-gaps.md); implement in the [documented delivery order](architecture/capability-gaps.md#delivery-order).
- Snapshot resync hardening is complete (stall watchdog, busy deferral, per-peer progress diagnostics, dual-requester live test, CI live gate); see TASKS_COMPLETED.md SNAPSHOT-PROGRESS-001 and LIVE-GATE-001.
- Each extension requires its subsystem acceptance scenarios before status is updated.

(Historical note: the original phase-by-phase bootstrap sequence was removed on 2026-09-27 as fully superseded; it remains recoverable from this file's git history.)


  All the tests-live must be redone I want an application to be created with the go package. this application must be use by multiples process, each with  their subfolder node1, node2, node3, etc.  The tests-live must replicate how application works, they are not for code testing.
  The test must be complex like the ../GALVANIZE version have a look at the rust code if required.

### Live Multi-Process Application Tests (tests-live)

6. `encryption` — DONE (verified multi-process, see TASKS_COMPLETED.md).
7. `files-bridge` — DONE (multi-process redo, see TASKS_COMPLETED.md).
8. `files-soak` — DONE (multi-process redo, see TASKS_COMPLETED.md).
9. `highlow` — DONE (multi-process redo, see TASKS_COMPLETED.md).
10. `large-payload` — DONE (multi-process redo, see TASKS_COMPLETED.md).
11. `loadshare` — DONE (multi-process redo, see TASKS_COMPLETED.md).
12. `partition` — DONE (verified multi-process, see TASKS_COMPLETED.md).
13. `rekey` — DONE (multi-process redo, see TASKS_COMPLETED.md).
15. `subscribe` — DONE (multi-process redo, see TASKS_COMPLETED.md).
16. `three-node-sync` — DONE (verified multi-process, see TASKS_COMPLETED.md).
17. `views` — DONE (multi-process redo, see TASKS_COMPLETED.md).
18. `benchmark` — DONE (multi-process redo, see TASKS_COMPLETED.md).
19. `highlow-faults` — DONE (covered by tests-live/highlow forgery test, see TASKS_COMPLETED.md).
20. `highlow-schema` — DONE (covered by tests-live/highlow schema-hold test, see TASKS_COMPLETED.md).
22. `write-priority` — DONE (multi-process redo, see TASKS_COMPLETED.md).
23.–29. `backup-restore`, `bridge-two-streams`, `churn-retirement`, `overload-budgets`, `partial-mesh`, `plumtree-live`, `schema-evolution` — DONE (implemented and verified 2026-09-28, see TASKS_COMPLETED.md LIVE-SUITES-VERIFY-001).
