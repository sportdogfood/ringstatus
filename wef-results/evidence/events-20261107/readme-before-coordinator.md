# WEF Results coordinator

Document version: **1.6**
Updated: **1 November 2026 — America/New_York**
Alert-driven X/Y workflow and reconciliation prepared as disabled drafts; package/runtime unchanged.

## Current sole responsibilities

Watch the existing `wef-data.trips` and `wef-data.results` tables inside the `wef-results` base (`appK9IX59jFwUkB2u`). These are the local input tables for the new responsibility contract; this documentation does not authorize a second source collector or writes to upstream Data/Live bases.

Use `class-status`, `placing`, `gone-in` and the class-sourced `results_verified` available through `wef-data.trips`. Verify actual field/path compatibility before any separately approved implementation; do not invent a JSON path or substitute a similarly named field.

### Eligible scope

Process only records belonging to the matching `wef-shows.show-days` record where **`day-priority = 0`**. Derive customer, show and competition date dynamically from that eligible show-day and its existing relationships/keys. Match the relevant trip/result records by exact identities. The previously supplied key was an example only: **never hardcode a customer/show/date key** or replace this filter with a separate assumed TODAY flag.

### Two separate record triggers

| Record | Create a new record when, within eligible scope | Required content/source |
|---|---|---|
| X | `results_verified = 1` and at least one result value is nonzero | All matching nonzero results and instances from `wef-data.results`, plus horse, rider and entry identity. |
| Y | `gone-in = 1` and at least one of `score1`, `time-one`, `faults-one`, `time-fault-one` is nonzero | Nonzero values from those fields on the matching trip, plus horse, rider and entry identity. |

X and Y create separate new records for their respective qualifying conditions. At least one nonzero value is sufficient for each rule. Missing values are not zero.

Find the horse, rider and entry **within this same base**, using existing local tables, links and exact source IDs. Preserve those links/IDs and the trip/result association; names are not identity. No external directory, name-based matching or newly invented entry table is authorized.

The approved simple workflow uses the existing snapshot table and its fields. Keep original triggered records; flag duplicates or changed source values as Conflict for resolution. No record deletion or automatic repair.

### Cadence and activation boundary

Source alerts create X/Y records when their qualifying conditions occur. **Polling does not create X/Y results.** Reconciliation runs every **10 minutes** to organize records, identify conflicts and mark expectations **Expected**, **Complete** or **Conflict**. **Complete** means the qualifying alert created its result record and that record was read back. Expected means the result has not yet been created. Conflict identifies duplicates or inconsistent information without deletion or source repair.

Both paths use dynamic `day-priority = 0` scope. Reconciliation preserves separate expectation rows in `snapshot` (`expected|X|<trip-key>` / `expected|Y|<trip-key>`); qualifying alerts create new result rows (`X|<trip-key>` / `Y|<trip-key>`). Result details and horse/rider/entry identities use existing JSON fields; `check-status` holds the readable state. A repeated alert reuses an existing result record; inconsistent output is flagged instead of overwritten. These are routine implementation choices for the owner's approved simple workflow, not extra schema or acceptance gates.

**Do not activate anything.** The owner authorized creating the alert/reconciliation workflow as disabled native Airtable automation drafts, its shared script, offline checks and documentation. No business records are created during preparation, no source tables are written and no task is enabled or run. Package/runtime versions and schema remain unchanged. Maintain the standard `snapshot`, `run-logs`, `heartbeat`, `errs-log` and `code-reviews` responsibilities; no additional domain tables are approved.

## Current implementation boundary

Module: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-results. Its README is the detailed source for table IDs, prior build evidence and remaining work.

The earlier per-class snapshot implementation and historical 54-test review evidence remain preserved. The new native alert/reconciliation workflow uses `wef-results/workflow.js` and the existing tables/fields. Historical checks do not prove this workflow. No business execution or activation is authorized. New offline checks and independent review are tracked separately in `wef-results/evidence/workflow-20261101/`.

Current delivery: five native Airtable automation drafts are saved, valid and read back as **undeployed**: trip created/changed alerts, result created/changed alerts and ten-minute reconciliation. Saved scripts, triggers and inputs match the reviewed source. All 15 focused offline checks pass; independent correction review COMPLETE / candidate PASS. The initial failed review and three defects are preserved with the separate owner correction approval and passing re-review. Business snapshot/run-log/heartbeat/error tables remain empty. **Nothing activated.** Live alert delivery, concurrent execution, scheduled reconciliation and downstream serving remain unverified.

| Disabled automation | Airtable |
|---|---|
| Trip created alert | https://airtable.com/appK9IX59jFwUkB2u/wflwxdcMXfkhNnQyT |
| Trip changed alert | https://airtable.com/appK9IX59jFwUkB2u/wfl8tZCyB83OBtpDr |
| Result created alert | https://airtable.com/appK9IX59jFwUkB2u/wfltMpaxfnlR2AMUB |
| Result changed alert | https://airtable.com/appK9IX59jFwUkB2u/wflrVQtNEheJV3McU |
| Reconcile Expected / Complete / Conflict | https://airtable.com/appK9IX59jFwUkB2u/wflqhnoFBMPmbRRyI |

Evidence: module `evidence/workflow-20261101/delivery-verification.json`, `automation-readbacks.json` and `correction-1/review/`. The shared implementation is `workflow.js`; original runner, package and schema hashes are unchanged. No new fields or tables. Do not turn on or test the business automations without authorization.

## Procedures and private continuity

Installed operating guidance: RingStatus Operating Skills **0.2.8**, plugin `plugins_6ac394a82a7881919216880c81992363`, under `C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.8/skills/`. Preserve each skill's invocation policy; a reference does not invoke a procedure. Applicable guidance includes Request Router, Instruction Loader, Rebalancer, Handoff, Code Review, Context Compact and Complaint Reporting; use only the procedure relevant to the authorized task.

Original human instructions plus later corrections, cancellations, restrictions and explicit approvals define current scope. Agent notes cannot add or remove authority. Proceed with routine approved choices without repeated permission. Exclude optional unapproved additions; propose genuinely necessary extra scope and obtain missing explicit approval before implementation, retain unmet acceptance and continue independent approved work. Preserve all edit, failed-verification, runner, retry and deployment stops.

The coordinator retains requirements, decisions, ownership and downstream acceptance. Keep the approved outcome together through required verification and authorized delivery or agreed handoff. Use one designated writer for dependent/shared resources and preserve required independent review. Return concise findings, failures, pending decisions and exact evidence links; inspect underlying proof rather than accepting summaries, test counts or sent messages. Check persistent specialist ownership alongside native-agent inventory. No new chat, agent, monitor or task is implied.

Preserve source clocks, exact identities and producer/consumer/fixture compatibility; reuse valid targeted evidence. Complaint reporting requires separate write authority, one canonical outage, scoped measurements with unknowns left unknown, and authorized-write readback; restoration does not close complaints. Native automatic compaction remains distinct from private checkpoint preparation and verified continuity; no manual owner compaction command is required.

Installed loading evidence, November 1: complete v0.2.8 reads and hash/output readback are saved in `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/coordination-rules-20261101/coordinators/results/` (`load-receipt.json`, `command-log.json`, `distribution.json`, `ownership-evidence.json`, `evidence-readback.json`). The unchanged Complaint Reporting body was fully read from installed v0.2.6, with proof in `.git/ringstatus-control/complaint-reporting-release-20261101/coordinators/results/`; Context Compact was fully read from installed v0.2.5, with proof in `.git/ringstatus-control/complaint-skills-20261031/installed-adoption-20261101/results/`. Do not describe those older reads as fresh v0.2.8 reads. Current persistent ownership evidence and inventories identify no Results-owned recipients; the historical reviewer was not restarted. Loading proves only the recorded reads, not procedure execution, business acceptance or future compliance.

Private checkpoint: C:/Users/gombc/.codex/context-archives/01a169d7-0bcd-7ca1-8b39-4e124da8b363. Keep the full continuing Results role and latest owner corrections; historical skills exercises/old snapshots are not a new assignment. This README does not auto-load procedures, schedule work or authorize product changes.
