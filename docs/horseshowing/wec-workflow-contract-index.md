# WEC Workflow Contract Index

This index lists the current WEC workflow contract documents and what each one owns.

## Current Concept Reference

| Document | Purpose | Status | When To Use |
|---|---|---|---|
| `docs/horseshowing/wec-lanewise-workflow-concept-2026-07-10.md` | Canonical record of the ringwise, classwise, entrywise, riderwise, Live, Time Engine, alerts, helpers, and output concept discussed on July 10. | Current concept; not an implementation contract | Read first for target-workflow discussion. Do not infer implementation approval from it. |

## Current Contracts

| Document | Purpose | Status | When To Use | Active / WIP |
|---|---|---|---|---|
| `docs/horseshowing/wec-full-workflow-lock.md` | Master workflow lock for stages, table handoffs, required fields/helpers, cron ownership, and WIP lanes. | Current master contract | Use first when deciding where a lane/table/cron belongs. | Active |
| `docs/horseshowing/ringstatus-scheduling-routing-agent-contract.md` | Routing Agent authority, cleaner target workflow, lane routing rules, proof standards, drift controls, session ownership, and manual-correction/hot-patch lane requirements. | Current routing contract | Use before assigning work to specialist agents or interpreting cross-lane blockers. | Active |
| `docs/horseshowing/ringstatus-scheduling-specialist-agent-prompt-pack.md` | Reusable prompts for Routing, Core Build, Preflight, Stage 4S, Live, Time Engine, Rider Results, Alerts, Publish, Endpoints, and Hot Patch agents. | Current prompt pack | Use when starting or correcting specialist-agent sessions. | Active |
| `docs/horseshowing/wec-workflow-log-contract.md` | Dedicated contract for system execution audit rows and stage handoff proof. | Current supporting contract | Use when designing or implementing `workflow_log`. | WIP |
| `docs/horseshowing/wec-alert-message-contract-draft.md` | Draft contract for alerts/messages as queued event records, separate from sending. | Draft | Use when returning to message/alert design. | WIP |
| `docs/horseshowing/wec-barn-entry-audit-contract.md` | Operator-facing reconciliation lane for barn/user expected entries versus current WEC mapping. | Draft contract | Use when designing the expected-entry audit or hot-add isolation lane. | WIP |
| `docs/horseshowing/wec-catalyst-step1-step4-stack-contract.md` | Locked Catalyst-owned baseline stack contract for Step 1 through Step 4. | Locked prior contract | Use when verifying the baseline cadence stack. | Active |
| `ringstatus-data/catalyst-workspaces/horseshowing/docs/core_1_4_next_day_preflight_contract.md` | Core outside-lane next-day source acquisition, bounded probe/parse, runtime projection, and blocker classification. | Current supporting contract | Use before focus-day changes and before deploying Core fixes for day-change failures. | Active |
| `docs/horseshowing/wec-2026-07-08-night-core-1-4-handoff.md` | Night handoff for Core progress, Stage 4S sync blocker, outside test lanes, and next focus-day change checklist. | Current handoff | Use for the July 9 focus-day change review and staging mirror follow-up. | Active |
| `docs/horseshowing/wec-catalyst-step6-results-contract.md` | Prior broad Results lane ownership, wake, candidate scope, retry, and proof contract. | Prior / compatibility | Use only when broad class-results machinery is intentionally reintroduced or audited. | Compatibility |

## Ownership Summary

- `wec-full-workflow-lock.md` is the master map.
- `ringstatus-scheduling-routing-agent-contract.md` owns routing-agent authority, cleaner target workflow, routing decisions, and drift controls.
- `ringstatus-scheduling-specialist-agent-prompt-pack.md` owns reusable specialist-agent prompts.
- `wec-workflow-log-contract.md` owns execution/handoff audit design.
- `wec-alert-message-contract-draft.md` owns future message and alert semantics.
- `wec-barn-entry-audit-contract.md` owns barn/user expected-entry reconciliation.
- `wec-catalyst-step1-step4-stack-contract.md` owns the proven baseline stack.
- `core_1_4_next_day_preflight_contract.md` owns proactive Core next-day preflight and blocker classification.
- `wec-2026-07-08-night-core-1-4-handoff.md` owns the current night handoff, Stage 4S scheduler blocker, and next date-change checklist.
- `wec-catalyst-step6-results-contract.md` owns the prior broad Results lane only when explicitly revived.

## Cleaner Workflow Rule

The current target workflow treats `update_schedule` as the Stage 1 source of truth for day/ring/class structure, `get_rings` as the Live current-state updater, and `rider_results` as the business result lane. `get_ring_days`, hot `get_orders`, and broad class-results machinery are compatibility/deferred surfaces unless a current lane contract proves they are needed.

## Use Rule

For target-workflow concept discussion, start with `wec-lanewise-workflow-concept-2026-07-10.md`.

For current production implementation, start with the master workflow lock. If a row points to a dedicated contract, use the dedicated contract for implementation detail. A concept document does not authorize production changes.
