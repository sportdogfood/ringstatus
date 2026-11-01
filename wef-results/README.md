# WEF Results

Document version: **1.2**
Updated: **31 October 2026 — America/New_York**
Owner: **wef-results-coordinator**
Change: current responsibilities, two triggers, dynamic priority-zero scope and10-minute cadence documented. Package/runtime version unchanged.

## Current sole responsibilities

Watch the existing `wef-data.trips` and `wef-data.results` tables inside the `wef-results` base (`appK9IX59jFwUkB2u`). These are the local input tables for the new responsibility contract; this documentation does not authorize a second source collector or writes to upstream Data/Live bases.

Use `class-status`, `placing`, `gone-in` and the class-sourced `results_verified` available through `wef-data.trips`. Verify actual field/path compatibility before any separately approved implementation; do not invent a JSON path or substitute a similarly named field.

### Eligible scope

Process only records belonging to the matching `wef-shows.show-days` record where **`day-priority = 0`**. Derive customer, show and competition date dynamically from that eligible show-day and its existing relationships/keys. Match the relevant trip/result records by exact identities. The previously supplied key was an example only: **never hardcode a customer/show/date key** or replace this filter with a separate assumed TODAY flag.

### Two separate record triggers

| Record | Trigger within eligible scope | Required content/source |
|---|---|---|
| Y | `gone-in = 1` | `score1`, `time-one`, `faults-one`, `time-fault-one` from the matching trip, plus horse, rider and entry identity. |
| X | `results_verified = 1` | Look up matching `wef-data.results` and roll up **nonzero** `score1`, `time-one`, `faults-one`, `time-fault-one`, plus horse, rider and entry identity. |

Y and X are separate records for the two trigger conditions. Do not impose the previous combined Completed/verified/visible class-snapshot gate on Y: it would erase the owner's distinct `gone-in = 1` responsibility. Nonzero filtering is specified for X; do not silently remove zero-valued Y fields. Missing values are not zero.

Find the horse, rider and entry **within this same base**, using existing local tables, links and exact source IDs. Preserve those links/IDs and the trip/result association; names are not identity. No external directory, name-based matching or newly invented entry table is authorized.

Still to discuss before implementation: whether multiple nonzero X values are listed or summed; destination record layout and the duplicate/repeat rules for Y/X. No rollup operation, once-per-trip rule, correction/retraction behavior or new schema is silently selected by this document.

### Cadence and activation boundary

The owner's specified Results cadence is **every 10 minutes**, processing only matching show-days where **`day-priority = 0`**. The priority-zero scope is dynamic on each evaluation. This is the agreed responsibility/cadence requirement, **not an activated schedule or proof of execution**.

**Do not activate anything.** This update authorizes README changes only: no code, package/runtime version, schema, records, credentials, task registration, task enablement, collector run, source request or scheduled cadence configuration is changed. Maintain the standard `snapshot`, `run-logs`, `heartbeat`, `errs-log` and `code-reviews` responsibilities; no additional domain tables are approved.

## Existing implementation and historical acceptance

The earlier implementation is a per-class snapshot reader using external Data/Live evidence and Shows authority. It does **not** implement the newly specified local-table Y/X record contract or the10-minute cadence. The current responsibilities above supersede earlier descriptions of what to build next; the old code is retained, not silently rewritten.

| Results-owned table | Existing ID |
|---|---|
| run-logs | tblXVlQ8VGETl8RLB |
| heartbeat | tbld5k6m2wiKjy3pY |
| errs-log | tbllVvYbHdcAKmFzG |
| code-reviews | tbl0p1Me0zVdz1pzF |
| snapshot | tblraM6Oo9pxUxKzV |

Existing schemas are in `schema.json`; `meta` is preserved. Airtable remains the only durable business store. No new index/archive/queue/revision table is approved. Original source clocks and evidence must remain distinct from publication/check times; no silent timestamp renewal, value repair, blind write retries or deletion.

Dated previous-build evidence:54offline tests, three sanitized retained-source-shape replays and syntax checks passed; independent re-review COMPLETE/candidate PASS after separately approved count/UUID corrections. Report: `evidence/build-20261030/correction-1/independent-review/review.md`; passing administrative record `recFuD6Uxk6fWhv3l`. The initial failed review and candidate remain preserved. Those checks apply to that earlier candidate, **not** the new Y/X requirements. README1.2 is a later documentation-only change, not part of the prior frozen review packet; no new code review or test run is claimed.

Last verified build boundary: no Results task registered/activated or business workflow run. Last inspected business tables were empty; two administrative review records existed. This dated evidence is not a new operational inspection. Live population, actual credentials/scheduling, repeat scheduled behavior and downstream serving remain unverified.

Prepared launchers `run-hidden.vbs` -> `run.ps1` -> `entry.js` remain inactive. Their previous configuration/freshness policy is not adopted as the new local Y/X contract. Keep other modules' collection, archives and schedules untouched. Timing/Records coordination does not transfer Results writing/runtime ownership; Publish/Messages own serving/delivery, which is not assigned here.

## Operating guidance and next action

Apply current owner/global/repository instructions and the applicable canonical skills: `C:/Users/gombc/.agents/skills/instruction-loader/SKILL.md`, `C:/Users/gombc/.agents/skills/rebalance/SKILL.md`, `C:/Users/gombc/.agents/skills/code-review/SKILL.md`, and `C:/Users/gombc/.codex/skills/context-compact/SKILL.md`. Relevant producer/consumer/fixture rules: instruction-loader/references/recurring-delivery-checks.md. Preserve code review logs/receipts and private full-role checkpoints; guidance does not automatically load, schedule, authorize fixes or prove runtime behavior.

Next action: discuss the remaining rollup/layout/duplicate rules, then obtain implementation approval for the smallest change against the retained code. Preserve the explicit no-activation stop. No task, business execution, source repair or extra upgrade follows from this README update.
