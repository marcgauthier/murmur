# Repository guidance

## Main goal of SPeD-SQL
Create a very fast local sql database with encryption at rest on disk via KV store and that replicated quickly via QUIC with TLS, The code must work, it must be tested with live test not just test for code. 


## Keep documentation current

- When behavior, public APIs, configuration, schema rules, replication, storage,
  recovery, security, or operational requirements change, update the affected
  documents in `architecture/` as part of the same change.
- Update `README.md` when setup, usage examples, repository layout, or implementation
  status changes. Keep examples accurate and distinguish implemented behavior from
  planned capabilities.
- Update `TASKS_PENDING.md` and acceptance criteria when a planned capability is
  implemented, changed, or removed.
- AI agents must maintain the architecture task checklist in `TASKS_PENDING.md`.
  When a feature is implemented and its relevant acceptance checks pass, remove
  its item from `TASKS_PENDING.md` and move it via `TASKS_INPROGRESS.md` to
  `TASKS_COMPLETED.md`; do not leave completed checkboxes in pending tasks. For
  partial implementations, retain or rewrite the item in `TASKS_PENDING.md` to
  describe only the remaining work. Add newly adopted unimplemented requirements,
  and keep related architecture documents and README status consistent with the
  task list.
- Use `architecture/README.md` as the documentation index. Add or update its links
  when documents are added, renamed, moved, or reorganized.
- Keep architecture documents focused on their topics. Cross-reference related
  documents instead of duplicating requirements or recreating a root monolith.
- Preserve numbered section identities when possible; if a section is renamed or
  moved, update affected cross-references and table-of-contents links.
- Before finishing documentation changes, check that relative links and section
  anchors resolve, Markdown fences are balanced, and no obsolete document paths
  remain in active references.

## Coordinate tasks to avoid multi-agent collisions

AI agents MUST coordinate work through `TASKS_PENDING.md`, `TASKS_INPROGRESS.md`, and `TASKS_COMPLETED.md`.

### Task claiming protocol

Before modifying source code, documentation, tests, configuration, or any other project file, an agent MUST claim a task using the following process:

1. Read `TASKS_PENDING.md`, `TASKS_INPROGRESS.md`, and `TASKS_COMPLETED.md`.

2. Select a task from `TASKS_PENDING.md`.
   - Never select a task already actively assigned in `TASKS_INPROGRESS.md`.
   - Prefer a task that has no existing claim.
   - A task MUST be identified consistently, preferably using its task ID or exact task title.

3. Add a temporary claim for the task to `TASKS_INPROGRESS.md`.
   - Do NOT remove the task from `TASKS_PENDING.md` yet.
   - Do NOT modify project files yet.
   - The claim MUST contain:
     - task ID/title
     - agent name
     - model used
     - claim timestamp
     - status `CLAIMED`

Example:

`- [ ] TASK-012 | CLAIMED | agent=codex | model=gpt-5.6 | claimed=2026-09-27 09:15:22.184 EDT`

4. After writing the claim, the agent MUST wait at least **60 seconds** before beginning work.

5. After the 60-second claim period, the agent MUST re-read:
   - `TASKS_PENDING.md`
   - `TASKS_INPROGRESS.md`
   - `TASKS_COMPLETED.md`

6. Check for competing claims for the same task.

   If only one claim exists, that agent wins the task.

   If multiple agents claimed the same task, the claim with the **earliest claim timestamp wins**.

   If two claims have exactly the same timestamp, use the agent name as a deterministic tie-breaker; the alphabetically first agent name wins.

7. Losing agents MUST:
   - remove only their own claim from `TASKS_INPROGRESS.md`;
   - make no changes related to that task;
   - return to `TASKS_PENDING.md`;
   - select another available task;
   - repeat the complete claim process, including the 60-second wait.

8. The winning agent MUST verify that:
   - the task still exists in `TASKS_PENDING.md`;
   - the task has not already been completed;
   - no earlier valid claim exists.

9. After winning the claim:
   - remove the task from `TASKS_PENDING.md`;
   - change its `TASKS_INPROGRESS.md` status from `CLAIMED` to `IN_PROGRESS`;
   - record the work start timestamp;
   - only then begin modifying project files or writing code.

Example:

`- [ ] TASK-012 | IN_PROGRESS | agent=codex | model=gpt-5.6 | claimed=2026-09-27 09:15:22.184 EDT | started=2026-09-27 09:16:25 EDT`

### Claim safety rules

- A `CLAIMED` entry does **not** mean work may begin. Work may begin only after the 60-second collision check succeeds and the entry is changed to `IN_PROGRESS`.
- Never modify files for a task while its status is only `CLAIMED`.
- Never work on a task assigned as `IN_PROGRESS` to another agent.
- Never overwrite or remove another agent's claim.
- An agent may remove only its own losing, abandoned, or stale claim.
- If the task disappears from `TASKS_PENDING.md` during the 60-second waiting period, the agent MUST remove its claim and select another task.
- If the task appears in `TASKS_COMPLETED.md` during the waiting period, the agent MUST remove its claim immediately.
- Before beginning work, always perform the final re-read. Never rely on the contents of the files as they existed before the 60-second wait.
- Use the local system clock including timezone for timestamps.
- Include milliseconds in claim timestamps to reduce ties.

### Completing a task

When a task is completed:

1. Create or update tests covering the code changed by the task.
2. Run the tests relevant to the changed code.
3. Do NOT run the entire codebase test suite unless:
   - the change affects shared/core functionality;
   - targeted tests cannot adequately validate the change; or
   - the task specifically requires the complete test suite.
4. Ensure all relevant acceptance checks pass.
5. Remove the task from `TASKS_INPROGRESS.md`.
6. Add it to `TASKS_COMPLETED.md` marked `[x]`.

The completed entry MUST include:
- completion timestamp (`YYYY-MM-DD HH:MM:SS TZ`);
- agent name;
- model used;
- brief summary of the implementation;
- tests/checks performed.

Example:

`- [x] TASK-012 | completed=2026-09-27 10:04:12 EDT | agent=codex | model=gpt-5.6 | Added replicated transaction batching and targeted tests. Tests: ./internal/replication/... PASS`

### Aborted or failed tasks

If work is abandoned or cannot be completed:

- remove the task from `TASKS_INPROGRESS.md`;
- return it to `TASKS_PENDING.md`;
- preserve any useful notes about the reason it was returned;
- do not move it to `TASKS_COMPLETED.md`.

If partially completed code should not remain in the repository, revert those changes before releasing the task.

### Task file consistency

At all times:

- A task may be `PENDING`, `CLAIMED`, `IN_PROGRESS`, or `COMPLETED`.
- `CLAIMED` is the only state in which the same task may temporarily appear in both `TASKS_PENDING.md` and `TASKS_INPROGRESS.md`.
- Once a claim becomes `IN_PROGRESS`, the task MUST no longer appear in `TASKS_PENDING.md`.
- A completed task MUST exist only in `TASKS_COMPLETED.md`.
- Keep `TASKS_PENDING.md`, `TASKS_INPROGRESS.md`, `TASKS_COMPLETED.md`, and `README.md` consistent with the actual implementation state.

## Keep github.com/marcgauthier/spedsql current

For substantial code changes:

- commit completed and tested work;
- synchronize with the latest `main` before pushing;
- resolve any conflicts without overwriting work from other agents;
- re-check the task coordination files after synchronization;
- push the completed change to the `main` branch.

Never commit or push a task while its status is only `CLAIMED`.

## Coordinate tasks in TASKS_PENDING.md to avoid multi-agent collision

- Before starting work, an agent MUST add a claim for the task to `TASKS_INPROGRESS.md` with agent name, model, and timestamp. Do not modify code yet.
- Wait 60 seconds, then re-read `TASKS_INPROGRESS.md`.
- If multiple agents claimed the same task, the earliest timestamp wins.
- Losing agents MUST remove their own claim and select another task.
- The winning agent removes the task from `TASKS_PENDING.md`, marks it `IN_PROGRESS`, then starts work.
- Never work on a task already marked `IN_PROGRESS` by another agent.
- When complete, move it from `TASKS_INPROGRESS.md` to `TASKS_COMPLETED.md` with `[x]`, completion timestamp, agent/model, summary, and tests run.
- If aborted, remove the claim and return the task to `TASKS_PENDING.md`.
- After changes, create/run tests for the affected code only, not the entire codebase unless needed.
- Keep `TASKS_PENDING.md`, `TASKS_INPROGRESS.md`, `TASKS_COMPLETED.md`, and `README.md` consistent.

## Keep github.com/marcgauthier/spedsql current

- When doing large code change commit and push to github on main branch.
