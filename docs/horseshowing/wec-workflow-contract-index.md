# WEC Workflow Contract Index

This index lists the current WEC workflow contract documents and what each one owns.

## Current Contracts

| Document | Purpose | Status | When To Use | Active / WIP |
|---|---|---|---|---|
| `docs/horseshowing/wec-full-workflow-lock.md` | Master workflow lock for stages, table handoffs, required fields/helpers, cron ownership, and WIP lanes. | Current master contract | Use first when deciding where a lane/table/cron belongs. | Active |
| `docs/horseshowing/wec-workflow-log-contract.md` | Dedicated contract for system execution audit rows and stage handoff proof. | Current supporting contract | Use when designing or implementing `workflow_log`. | WIP |
| `docs/horseshowing/wec-alert-message-contract-draft.md` | Draft contract for alerts/messages as queued event records, separate from sending. | Draft | Use when returning to message/alert design. | WIP |
| `docs/horseshowing/wec-barn-entry-audit-contract.md` | Operator-facing reconciliation lane for barn/user expected entries versus current WEC mapping. | Draft contract | Use when designing the expected-entry audit or hot-add isolation lane. | WIP |
| `docs/horseshowing/wec-catalyst-step1-step4-stack-contract.md` | Locked Catalyst-owned baseline stack contract for Step 1 through Step 4. | Locked prior contract | Use when verifying the baseline cadence stack. | Active |
| `docs/horseshowing/wec-catalyst-step6-results-contract.md` | Locked Catalyst-first Step 6 results lane contract. | Locked prior contract | Use when verifying or extending results probing. | Active |

## Ownership Summary

- `wec-full-workflow-lock.md` is the master map.
- `wec-workflow-log-contract.md` owns execution/handoff audit design.
- `wec-alert-message-contract-draft.md` owns future message and alert semantics.
- `wec-barn-entry-audit-contract.md` owns barn/user expected-entry reconciliation.
- `wec-catalyst-step1-step4-stack-contract.md` owns the proven baseline stack.
- `wec-catalyst-step6-results-contract.md` owns the proven results lane.

## Use Rule

Start with the master workflow lock. If a row points to a dedicated contract, use the dedicated contract for implementation detail.
