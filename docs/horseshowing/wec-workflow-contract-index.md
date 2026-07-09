# WEC Workflow Contract Index

This index lists the current WEC workflow contract documents and what each one owns.

## Current Contracts

| Document | Purpose | Status | When To Use | Active / WIP |
|---|---|---|---|---|
| `docs/horseshowing/wec-full-workflow-lock.md` | Master workflow lock for stages, table handoffs, required fields/helpers, cron ownership, and WIP lanes. | Current master contract | Use first when deciding where a lane/table/cron belongs. | Active |
| `docs/horseshowing/ringstatus-scheduling-routing-agent-contract.md` | Routing Agent authority, lane routing rules, proof standards, drift controls, session ownership, and manual-correction/hot-patch lane requirements. | Current routing contract | Use before assigning work to specialist agents or interpreting cross-lane blockers. | Active |
| `docs/horseshowing/ringstatus-scheduling-specialist-agent-prompt-pack.md` | Reusable prompts for Routing, Core, Preflight, Stage 4S, Live, Time Engine, Results, Alerts, Publish, Endpoints, and Hot Patch agents. | Current prompt pack | Use when starting or correcting specialist-agent sessions. | Active |
| `docs/horseshowing/wec-workflow-log-contract.md` | Dedicated contract for system execution audit rows and stage handoff proof. | Current supporting contract | Use when designing or implementing `workflow_log`. | WIP |
| `docs/horseshowing/wec-alert-message-contract-draft.md` | Draft contract for alerts/messages as queued event records, separate from sending. | Draft | Use when returning to message/alert design. | WIP |
| `docs/horseshowing/wec-barn-entry-audit-contract.md` | Operator-facing reconciliation lane for barn/user expected entries versus current WEC mapping. | Draft contract | Use when designing the expected-entry audit or hot-add isolation lane. | WIP |
| `docs/horseshowing/wec-catalyst-step1-step4-stack-contract.md` | Locked Catalyst-owned baseline stack contract for Step 1 through Step 4. | Locked prior contract | Use when verifying the baseline cadence stack. | Active |
| `ringstatus-data/catalyst-workspaces/horseshowing/docs/core_1_4_next_day_preflight_contract.md` | Core 1-4 outside-lane next-day source acquisition, bounded probe/parse, Step 4 projection, and blocker classification. | Current supporting contract | Use before focus-day changes and before deploying Core fixes for day-change failures. | Active |
| `docs/horseshowing/wec-2026-07-08-night-core-1-4-handoff.md` | Night handoff for Core 1-4 progress, Stage 4S sync blocker, outside test lanes, and next focus-day change checklist. | Current handoff | Use for the July 9 focus-day change review and staging mirror follow-up. | Active |
| `docs/horseshowing/wec-catalyst-step6-results-contract.md` | Current Results lane ownership, wake, candidate scope, retry, and proof contract. | Current contract | Use when verifying or extending Results ingestion. | Active |

## Ownership Summary

- `wec-full-workflow-lock.md` is the master map.
- `ringstatus-scheduling-routing-agent-contract.md` owns routing-agent authority, routing decisions, and drift controls.
- `ringstatus-scheduling-specialist-agent-prompt-pack.md` owns reusable specialist-agent prompts.
- `wec-workflow-log-contract.md` owns execution/handoff audit design.
- `wec-alert-message-contract-draft.md` owns future message and alert semantics.
- `wec-barn-entry-audit-contract.md` owns barn/user expected-entry reconciliation.
- `wec-catalyst-step1-step4-stack-contract.md` owns the proven baseline stack.
- `core_1_4_next_day_preflight_contract.md` owns proactive Core next-day preflight and blocker classification.
- `wec-2026-07-08-night-core-1-4-handoff.md` owns the current night handoff, Stage 4S scheduler blocker, and next date-change checklist.
- `wec-catalyst-step6-results-contract.md` owns the current Results lane.

## Use Rule

Start with the master workflow lock. If a row points to a dedicated contract, use the dedicated contract for implementation detail.
