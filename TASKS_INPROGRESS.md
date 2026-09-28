# SPeD-SQL In-Progress Tasks

This document tracks tasks currently being actively worked on by AI agents.

## Multi-Agent Workflow
1. **Claiming a Task**: When an AI agent selects a task, it MUST delete the task from `TASKS_PENDING.md` and add it here, stating the name of the agent (and model) and the start timestamp.
2. **Exclusivity**: No agent may work on, modify, or collide with any task currently listed in this file.
3. **Completion**: When the task is implemented and all tests pass, the agent MUST remove the task from this file and move it to `TASKS_COMPLETED.md`, marked with `[x]`, completion timestamp (`YYYY-MM-DD HH:MM:SS TZ`), a summary of changes, and the agent name/model.
4. **Cancellation**: If a task cannot be completed or work is aborted, remove it from this file and return it to `TASKS_PENDING.md`.

## Tasks Currently In Progress
<!-- Format:
- [ ] <Task description> — Agent: <agent_name> (<model>), Started: <YYYY-MM-DD HH:MM:SS TZ>
-->



- [ ] 23. `snapshot-resync` — New live suite: stale node rejoins via snapshot after log retention expiry. — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
- [ ] 24. `backup-restore` — New live suite: backup, fresh-identity restore, rejoin, reseed rejection. — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
- [ ] 25. `swim-discovery` — New live suite: cluster forms from partial seeds without static full mesh. — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
- [ ] 26. `schema-evolution` — New live suite: rolling additive migration across a live mesh. — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
- [ ] 27. `overload-budgets` — New live suite: app-level staging caps and overload rejection. — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
- [ ] 28. `churn-retirement` — New live suite: suspect/dead/refutation plus retirement vs GC gating. — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
- [ ] 29. `plumtree-live` — New live suite: dissemination mode over a real multi-process mesh. — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
- [ ] 30. `bridge-two-streams` — New live suite: bridge two-streams live suite (see task 30). — Agent: Muse Code (Muse Spark), Started: 2026-09-28 00:20:00 UTC
