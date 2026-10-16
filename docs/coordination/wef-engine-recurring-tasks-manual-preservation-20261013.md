# WEF recurring responsibilities and manual preservation

Discovery continuation, 13 October 2026. No application code, base records, configuration or production processes changed or executed. This is a task inventory, not a proposed new execution order or a final stage design.

## Source checkpoint

Inspected approved checkout: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus.
HEAD: f5e895b6d3af5b87d1d2f0d8524061a5e7f171f9.
The twelve core JS files checked with git status were clean at inspection. This identifies the local source inspected, not proof that every deployed/scheduled copy matches it today. Existing source history and older runtime evidence remain in wef-engine-identification-plan-20261010.md and wef-engine-live-trace-20261011.md.

## Actual recurring responsibilities identified

Paths and line anchors refer to the checkpoint above. Multiple responsibilities in one file are intentional inventory rows, not instructions to split the file.

| Task | Existing trigger and input | Work and consumer | Evidence |
| --- | --- | --- | --- |
| Select active show/date | Existing focused-show controls | Uses selected show/customer/focus_day to establish heartbeat scope; owner input remains valid | tagger.js:1768–1783, 1218–1226 |
| Decide which work is due | Local orchestrator mode/slot and explicit OOG request | Dispatches due scripts; DAY gates some jobs. Independent lanes and dependent failure handling differ | heartbeat_slot_orchestrator.js:1888–1962 |
| Acquire and normalize schedules | Schedule job due; dated source and existing fallback | Builds keyed watch_schedule records and preserves flagged manual start times | schedules_dailyv2.js:2588–2601, 2820–2872 |
| Acquire/match entries and retain schedule association | Trip acquisition due | Builds watch_trips with date/identity keys; retains existing links when a new field is absent; applies time override before update | trips_dailyv2.js:1777–1784, 2861–2892 |
| Enrich matched entries and results | Trip tagger due after applicable upstream success | Existing source/live matching; row update queue applies manual-time guard. Prior trace locates result fields inside this same tagger, not an independent results service | trips_tagger.js:1483–1542; existing overview result trace |
| Calculate class timing | DAY + calculator slot; schedule row and live group | Projects class duration/start/end and evaluates current state. Provides calculation logs and eligible alert events | schedules_calculatorv2.js:1112–1168, 1827–1871 |
| Evaluate alert events | Existing class calculation and configured alert rules | Builds thread_logs candidates, skips previously identified event IDs, writes configured events; event creation is distinct from SMS delivery | schedules_calculatorv2.js:1837–1871 |
| Calculate trip timing/order outputs | Calculator due, trip acquisition or tagger ran, dependent work succeeded | v2 wrapper delegates to existing calculator; compares rs_* outputs and updates changed values, with trip logs | heartbeat_slot_orchestrator.js:1959–1961; trips_calculatorv2.js:1–22; trips_calculator.js:1976–2023 |
| Acquire rendered order of go | Due slot or explicit trips_scrape_oog_now request | Existing scraper matches source order and writes order_of_go/order_set. Request cleared after reported success | heartbeat_slot_orchestrator.js:1964–1993; sgl_browser_enrichment.js:138–174 |
| Refresh live groups and propagate matching state | DAY + live-groups slot | Updates matching watch rows; start-time protection differs from status/count/ring propagation | heartbeat_slot_orchestrator.js:1996–1999; live_groups_daily.js:212–235 |
| Refresh live ring state | DAY + live-rings slot | Existing ring-state ingestion; detailed manual-field precedence not established in this pass | heartbeat_slot_orchestrator.js:2001–2004; live_rings_daily.js:410–431 |
| Refresh live class/order facts | DAY + enabled class-detail slot | Source-specific OOG/Actual_OOG/Gone or Scr/Pos/Gone updates; changed-value logs | heartbeat_slot_orchestrator.js:2006–2012; live_class_detail.js:311–323, 394–418 |
| Prepare and publish selected output | Publisher due and upstreamOk | Uses current publication path. Bulk skips identical content and preserves a prior nonempty dataset against an empty replacement | heartbeat_slot_orchestrator.js:2034–2037; publisher.js:862–899 |
| Record execution/calculation/change evidence | Throughout the corresponding tasks | Orchestrator events, schedule_logs, trip_logs and class changes explain skipped/changed/failed work. Engineering notes/code review are separate maintenance activities | Source anchors above; existing Audit trace in overview |

These responsibilities recur at their existing due conditions, not necessarily every heartbeat. The inspected dispatcher calls schedule calculation before the later live-group/ring/class refresh in the same cycle. That is an observed order, not a finding that it is wrong: calculations can depend on retained prior-cycle state. Moving blocks requires accounting for that state and freshness, not just rearranging stage labels.

Two-way is an incoming-message task outside this heartbeat sequence; its previously located Worker/automation path remains in messages-source-inventory-20261010.md. No new Two-way inspection or delivery proof is claimed here.

## Manual changes: observed protections and limits

| Place / manual action | What the inspected code does | Preservation boundary / next fact needed |
| --- | --- | --- |
| Focused show and focus_day | Tagger consumes the selected date and validates the date range; it requires one selected row for this path | Preserve owner selection as an accepted operating step. This does not prove every other control writer or rollover path |
| watch_schedule estimated_start_time with manual_time_override or historical manual_time_overide | Schedule acquisition omits this field from its update; rendered schedule scraper skips flagged rows | Guards: schedules_dailyv2.js:1893,2845–2852; sgl_browser_enrichment.js:125–127. Other schedule fields are not frozen |
| Schedule calculation with either time flag | Omits estimated_start_time, latest_estimated_start_time and ___latest_estimated_start_time writes | schedules_calculatorv2.js:1144–1164. Its calculation still selects groupRow.estimated_start_time before row.estimated_start_time (1115–1117). Protecting the saved cell is therefore not proof the override controls the projection |
| watch_trips time fields during acquisition | Either flag omits seven fields: estimated_start_time, estimated_end_time, estimated_go_time, estimated_time, actual_time, scheduled_estimated_start_time, latest_estimated_start_time | trips_dailyv2.js:127–135,1100–1109,2878–2892. Applies to identified existing rows in this update path, not a global record lock |
| watch_trips time fields during tagging | Either flag removes the five configured start/end/go/estimated/actual-time fields before queued updates | trips_tagger.js:108–116,162–168,710–719,1509–1520. Acquisition and tagging protect different field sets |
| Live-group propagation into watch records | Correctly spelled manual_time_override protects estimated_start_time | live_groups_daily.js:215–226 checks that spelling only; status, completed/total counts, ring_number and group_name still update. A typo-only flag may be protected in other writers but not this one; current record usage remains unverified |
| Manual heartbeat/watch_schedule link | Existing links are retained only when incoming fields omit that link | trips_dailyv2.js:1777–1784. An explicitly supplied replacement/empty field is not blocked by this helper; this is fallback preservation, not manual priority |
| Manual order_of_go | Rendered OOG task writes matched order, or clears order and records a status for missing/ambiguous/error/under-saddle cases | sgl_browser_enrichment.js:146–172 contains no manual-order guard. Selected rows can lose an edit on the next run. Whether the current view intentionally excludes manually managed rows remains to inspect |
| Manual live order/position/gone values | Class-detail path writes the first writable alias when source numeric value differs | live_class_detail.js:311–335,394–405. No manual-edit exception in this function; view/automation-level protection not verified |
| Manual rs_* calculated output | Calculator recomputes active rs_* output fields for eligible rows and queues differences | trips_calculator.js:63–82,1899,1989–2020. No manual-time flag found in this file. Editing a derived output is not established as a durable correction method |
| Existing published dataset when source is empty | Bulk output retains the prior nonempty dataset | publisher.js:872–875. This prevents destructive empty replacement but is not a manual override and does not certify freshness |

Source guards are evidence of implemented intent, not an executed preservation test. A difference in flag spelling or field sets is a review item, not authorization to rename fields or repair code. When the inspected guard is no longer true, its normal write path becomes eligible again; timing depends on the existing view and cadence. Whether that should be the owner's release mechanism has not been locked.

## Practical interpretation for the small business

Manual correction remains an intended operating option. Before promising that an edit will survive, identify its field and every recurring writer. Reuse any existing protection that actually covers those writers; do not add a new override framework during discovery.

The strongest concrete boundary found here is between source/input cells and derived outputs. Time flags protect selected input writes while calculations and live facts can continue. That allows useful operation, but it must be explained precisely so a human does not correct a field that the next cycle legitimately replaces.

## Next read-only continuation

1. Inspect existing view/filter and field definitions for affected watch_schedule/watch_trips rows, particularly scraper selection, the two time-flag spellings and whether rs_* fields are writable or computed. No records changed.
2. Trace remaining writes to manual-relevant start/order/status/link fields, including Airtable automations, so local guards are not mistaken for whole-system protection.
3. Trace downstream consumers of the manual row time versus live-group time and calculated rs_* outputs. Establish which value actually reaches schedule and alerts.
4. Identify the existing owner correction procedure and correction-release behavior from documentation/evidence. Keep unknown policy explicit; do not invent a mandatory gate or auto-repair.
5. Before any future approved cleanup test, use saved representative inputs and expected working outputs in an isolated copy, including an unchanged repeat, changed live data, preserved correction and restart. Do not run the production engine to fill discovery gaps.

Stage regrouping and splitting task types remain proposals. Current working WEF remains untouched.

