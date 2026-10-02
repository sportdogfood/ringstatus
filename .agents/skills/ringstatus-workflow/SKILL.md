---
name: ringstatus-workflow
description: Use only when explicitly invoked with mode 1 factual advice, mode 2 advice plus project planning, or mode 3 planning plus code, tests, review, and handoff. Do not activate for ordinary RingStatus requests.
---
# RingStatus Workflow

## Select the scope

Use only after explicit user selection. Accept `$ringstatus-workflow mode 1`, `mode 2`, or `mode 3`, or an explicit equivalent. If invoked without a mode, ask which mode; never default to implementation. Keep selection for the current task only; a new task needs a new choice. Without invocation, leave normal operation unchanged. Never promote a mode because advice or a plan suggests code.

| Mode | Deliverable | Write boundary |
|---|---|---|
| 1 — Advice | Factual advice with authoritative evidence and limitations | Read-only; no project files or external mutations |
| 2 — Plan | Mode 1 plus project plan, acceptance criteria, dependencies, and handoff | Plan in chat; write only a specifically requested plan file, never implementation |
| 3 — Build | Plan, authorized implementation, actual tests, independent code review, and handoff | Only approved target/files; publish/deploy only when authorized |

Treat cycled synthetic stages, SMS-in/out, and monitors as products of mode 3, not workflow modes. Do not build them merely because this workflow is invoked.

## Apply shared instructions

1. Inspect the actual target, applicable AGENTS.md, task sources, available tools, and Git status when relevant. Do not substitute another project or infer access. Preserve unrelated changes.
2. Verify uncertain/current claims against official documentation or actual target evidence. Separate confirmed facts, inference, assumptions, and blockers; never present a proposed feature as tested.
3. Ground and rebalance at startup, phase boundaries, handoffs, enhancements, resumption/compaction, observed conflict or scope uncertainty, and completion. Recheck goal, target, selected mode, acceptance criteria, ownership, evidence, and next authorized action. Scale the checkpoint to the mode: keep it brief and read-only in mode 1, include it in the mode 2 response or authorized plan file, and record it in the mode 3 project plan. These checkpoints are mandatory inside this workflow, not an automatic drift detector or hook.
4. Keep one lead accountable. In modes 1 and 2, the lead performs the task and may add one read-only investigator only for a named independent question that materially affects the answer or plan. Do not create implementation, testing, review, evidence, or controller lanes for advice or planning. Mode 3 may add bounded agents as defined in `references/build-cadence.md`. Supply every agent the exact target, applicable instructions, read/write limits, expected evidence, and stop conditions. Use native agent tools only; disclose missing capabilities.
5. Preserve the user's objective. Stop the same repair path after two failed fixes and report the blocker; do not redesign or switch targets without authorization. Side questions do not silently replace the task.
6. Return at most two prose sentences plus a compact table where needed. Give criterion-specific PASS / FAIL / BLOCKED with evidence for verification. Instruction validation is not application or client verification.

## Execute the selected mode

- Mode 1: inspect/research, reconcile sources, give factual advice, and stop without creating a project or plan unless the user explicitly selects mode 2.
- Mode 2: perform mode 1, then specify objective, target, boundaries, required stages, optional lanes, acceptance tests, dependencies, unresolved items, and implementation handoff; stop before code. Mode 2 does not authorize mode 3.
- Mode 3: read references/build-cadence.md and execute plan → implementation → testing → independent review → evidence → handoff. Record disabled optional product lanes and reasons before implementation; never disable shared grounding or required correctness checks. Use published existing tools/patterns; do not add hooks, agent runtimes, config changes, or enforcement code unless separately requested.

For modes 1 and 2, keep checkpoints in the answer or authorized plan file; do not create separate logs, controller artifacts, test machinery, or build handoffs. If inspected failure evidence supports an instruction correction, change the narrowest workflow-specific rule, replace obsolete or conflicting text instead of stacking another rule, check metadata and references for conflict, and exercise one bounded example. Do not move opt-in behavior into `AGENTS.md` unless the owner explicitly wants it to govern ordinary project work.

Read references/support-and-limits.md before claiming platform support or unattended operation. Installing this skill does not prove discovery or invocation in the user's Codex client.
