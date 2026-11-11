# New Base and Workflow Handoff

Use one copy of this record for the receiving agent. Fill it from verified sources. Use **Unverified**, **Unassigned** or **Not applicable** for missing or inapplicable items. The owner authorizes the assignment and destination before it is sent. [Setup procedure](</C:/Users/gombc/.codex/visualizations/2026/10/07/01a11632-62fd-73b1-997e-8be4119185de/New Base and Workflow Procedure.md>)

## Copyable handoff record

- **Assignment and acceptance.** [Outcome, exact first task, completion evidence, authorized scope and exclusions.]
- **Target and owners.** [Airtable workspace, base name, ID and direct link. Source and receiving chat IDs, coordinator, writer, runner, downstream owners and authorized README location. Record model and effort only if known.]
- **Baseline and approval.** [Current working source, release or commit, observed behavior and date, original approval and later corrections. Exact allowed change, protected data and interfaces, active stops, remaining permission and rejected approaches that matter.]
- **Base and README.** [System START HERE, workflow, current task, Agents registry and Resume here. Reuse the seven headings in the live README template. Current status lists only approved or assigned open work with status, date last confirmed open, and evidence.] [Agents README template](https://airtable.com/appZahVgD156cMAe3/tblV2xNTpeEahfu13)
- **Workflow contract.** [Trigger, cadence and time zone, input and keys, primary route and usable-result criteria, approved fallback order, stop and escalation, outputs and consumers. State who does each step and where evidence is recorded.]
- **Instructions and skills.** Read the applicable global and target instructions, urgent instruction and brief. Use [Instruction Loader](</C:/Users/gombc/.agents/skills/instruction-loader/SKILL.md>) at startup when invoked in this handoff. List required skills and why they apply. Record sources as read, supplied or missing. Check access. Save any authorized instruction copy or link in the agent's existing source document and reopen it. [Global AGENTS.md](</C:/Users/gombc/.codex/AGENTS.md>) · [RingStatus AGENTS.md](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/AGENTS.md>) · [Global Instructions Brief](</C:/Users/gombc/.codex/GLOBAL-INSTRUCTIONS-BRIEF.md>) · [Urgent Instructions](</C:/Users/gombc/.codex/visualizations/2026/10/07/01a11632-62fd-73b1-997e-8be4119185de/Urgent Instruction on Replies and Coding.md>)
- **Evidence and next action.** [Last verified primary result, fallback result, normal scheduled result and downstream readback, each with date and exact evidence. List known failures, pending checks, active workers or reminders and their current owners. State one next authorized action and its acceptance.]
- **Receiver confirmation.** Confirm actual target, access, owner and active stops. Report the instruction path and heading after readback. Record delivery, acknowledgment and verified completion separately. Continue the first authorized action when executable.

## WEF Data worked example

This is a verification handoff for the existing collector and cleanup lanes. Fill in the receiving chat and live task after assignment.

### Current status

The evidence below is the last evidence reviewed for this example. Refresh the owning checkpoint and confirm active stops at intake. Add only current approved or assigned open tasks with their last confirmed date and evidence.

| Check | Evidence reviewed |
| --- | --- |
| Primary path | **Unverified for this handoff.** Confirm primary-route evidence from the actual response or run log. |
| Fallback path | Decoder documents the 8 November recovery release and header2 sample proof. Confirm the route used by the Data run before attributing it to fallback. [Decoder README](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/workers/wef-schedules-decoded/README.md>) |
| Downstream collector | 8 November at 2:25 p.m. Eastern. Normal run `2ce3cb0e-d12e-4478-b5dc-239e0d5d8e28` saved 419 records with zero collection errors. [Collector acceptance](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/evidence/class-detail-filter-20261108/acceptance.json>) |
| Cleanup | 9 November 10 a.m. Eastern check. Event `log-check\|2026-11-09\|3`, run `1794236869626`. Readback showed zero deletions across four tables below threshold. This evidence covers the under-threshold branch. [Cleanup acceptance](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/evidence/cleanup-keep500-20261108/verification-1000-20261109/actual-result.json>) |

### Purpose and ownership

Verify the existing WEF Data collector and cleanup. Records coordinates. The Records guide lists `wef-data-fast-collector` at chat `01a19273-bdf8-71b2-8503-cc8079efc129` as the existing Data owner. Decoder owns the shared proxy and fallback. Verify the actual receiving chat and any change in ownership before transfer. [Records operating guide](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-records-coordinator/README.md>)

### Inputs and outputs

Data uses base `appdl16HJ5ETm8N69`. Shows supplies canonical show and day identity. Decoder supplies the decoded response through the approved proxy. Data owns its collector saves, linker and cleanup. Verify owned tables, keys and consumers against the current Data source. [WEF Data README](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/README.md>) · [Decoder README](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/workers/wef-schedules-decoded/README.md>)

### How it works

**Collection cadence and fallback contract.** Day 0 runs every three minutes when its date is today in Eastern time and hourly otherwise. Days 1–3 run in order on the existing 30-minute cycle. Use the approved primary route first and Decoder's documented fallback if the response is unusable. Stop at the first usable response. Check actual collector saves and record the route evidence separately. On failure, preserve the exact error and evidence and follow the existing escalation route. [WEF Data README](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/README.md>) · [Records operating guide](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-records-coordinator/README.md>) · [Decoder README](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/workers/wef-schedules-decoded/README.md>)

**Cleanup contract.** Records sends one alert per eligible event. Data validates the event ID and runs its existing triggerless cleanup task once. A new show can trigger cleanup. A new show-day alone does not. The daily clear at or after 7 p.m. Eastern clears eligible completed logs. For a checked log table at 1,000 or more rows, delete the oldest eligible records toward 500 total. Preserve active, current and pending records, reference tables and cleanup history. Protected records can leave the total above 500. [WEF Data README](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/README.md>) · [Records operating guide](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-records-coordinator/README.md>)

**Linker contract.** Data owns its independent linker. Use its current source and handoff evidence for links. This verification assignment covers collector and cleanup. [WEF Data README](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/README.md>)

### Handoffs

Records supplies the genuine cleanup event ID and alert evidence. Data records receipt, acknowledgment, run ID, actual counts and errors. Verify the task result, `cleanupV2`, and native and local readback where applicable. Keep unrelated sending stopped. Manual runs, repairs, event replays and cadence changes require separate authorization. [Records operating guide](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-records-coordinator/README.md>)

### Future

Use the next normal collector run and next genuine cleanup alert for any assigned verification. A below-threshold check does not verify the keep-500 deletion branch. Use the current owning evidence before reporting that branch or any other check complete.

If the approved workflow fails, stop the affected path and record the exact error and evidence. Continue independent approved work. After failed verification, code changes require the owner's separate **APPROVED TO EDIT**.

### Development

Read the Data README and local instructions, then the Records and Decoder guides. Preserve the existing implementation and verify through the assigned normal route. Use the global brief and urgent rules for concise, factual replies, relevant prior decisions, simple work and completion evidence. [Data AGENTS.md](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-data/AGENTS.md>) · [Global AGENTS.md](</C:/Users/gombc/.codex/AGENTS.md>) · [RingStatus AGENTS.md](</C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/AGENTS.md>) · [Global Instructions Brief](</C:/Users/gombc/.codex/GLOBAL-INSTRUCTIONS-BRIEF.md>) · [Urgent Instructions](</C:/Users/gombc/.codex/visualizations/2026/10/07/01a11632-62fd-73b1-997e-8be4119185de/Urgent Instruction on Replies and Coding.md>)

**First action.** Confirm the README and owning task you used, the last verified collector run and cleanup event, and any active stop. When authorized, verify the next normal collector run and genuine cleanup alert as they occur. Record each result against its own acceptance.
