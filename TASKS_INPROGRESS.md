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
