# WEF read-only trace — manually selected show, 11 October 2026

Owner confirmed runtime base `apptdhhNzduxm5gjn`. Supplied `appDN3R51ZPmwgMib` was inspected and found to contain Webflow/content tables; its show table lacks heartbeat/focus_day. The stages register remains `appnOAk2pW8JawiuP`, with the existing two tables, stages and lanes. Do not merge these three identities or change any controls.

Live control readback: `show` table `tbl72LDQ2xD9xpRAC`, record `rec6ERbzfAZvPVMkp`; show ID `200000073`; `2026 ESP Fall I (#261900)`; October 8–11; heartbeat checked; focus_day `2026-10-11`. Owner reports no participating riders for this test. Empty rider-related output therefore needs interpretation against real source responses, not invented participants.

## Observed scheduled path

No script, endpoint, task, automation or publication was manually executed by Codex. Only Airtable reads, local source and existing log tails were inspected. Windows `ringstatus-heartbeat` remains the active scheduled source; the older `ringstatus-pipeline-local` is currently Disabled. Source checkout HEAD is `a5235466eed1b094a26db14ab34b514726abf91d`; runtime logs are in `C:/actions-runner/ringstatus`.

The 14:03 Eastern heartbeat wrapper logged tagger, patterns, cadence and detached orchestrator launch. Its wrapper completion does not alone prove the detached cycle. The child log separately records slot B, DAY, 180-second cadence, show `200000073`, date October 11, and completion at `2026-10-11T18:04:25.822Z`. Airtable heartbeat `reciuMEN1B52JHSp9` independently matches that show/date. The preceding slot A also completed and included trip tagging/calculation and rendered OOG; slot B intentionally omitted those. This is fresh operation evidence for an active scope, replacing the previous no-active-WEF limitation.

## Findings that constrain success claims

| Boundary | Existing log evidence | Meaning |
| --- | --- | --- |
| Schedule acquisition | Dated October 11 and default schedule payloads show 79 rows; schedule output reports 79 updates; one valid show-heartbeat scope. | Current-day acquisition/write activity is present. No new direct fetch was performed for this trace. |
| Stage 6 schedule Timing | `schedules-calculatorv2.log` records NOOP: no rows in the selected view, calc_mode promote. The log calls it heartbeat view although source default is enrich, as documented previously. | Zero exit is not Timing output proof. Next inspect actual configured selection and view membership; do not change filters or fabricate rows. |
| Trips | Two selected tenant payloads (8778 and 19676) are 520/517 bytes; classified `soft_payload_too_small`/`soft_payload_blocked`; `writes_blocked:true`. Wrapper still records exit 0. | Do not call this successful entry acquisition or assume the classification simply means no riders. Read the existing guard/source and runner artifacts before interpreting the response. |
| Publish | Jobs include `missing_table_or_view` and unknown field `is_parent`; existing code retries without field selection. Same cycle records successful commit `b915a54b544df71bb54abe17126048945876aaca` for three trip JSON paths. | Mixed per-job outcomes; process success is not all-output success. The logged commit does not establish current-show content or freshness. |

Source anchors: `trips_dailyv2.js:2526,2609` (blocked acquisition/write summary); `publisher.js:832,1136,1286` (job errors versus final process message); existing dispatcher/source maps in the identification plan. Read-only evidence is not an authorization to repair those paths.

## Exact next read-only pass

1. Trace why the current schedule calculation selector returns no rows while acquisition reports 79 rows; inspect configured view and field selection, not a substitute run.
2. Trace the two small people payload classifications against existing captured artifacts and owner-reported absence of riders. Do not synthesize participants or retry endpoints.
3. Map the observed publication job errors to existing queue records and downstream consumers; distinguish fresh/current content from preserved output.
4. Continue unresolved alert-event → automation → delivery and Results/output consumer links from the previous plan. Keep first-pass conclusions high level; defer detailed start-time/go-time definitions.

Preserve the owner's TODAY/live and tomorrow/staging lock: manual focus is valid, dates are independent, two day scopes only, no October 12 staging outside this show's end. Current code's actual tomorrow support remains unverified. No implementation, new agents, new infrastructure, handoffs or changes to the stages stack are authorized by this trace.

Reference: `wef-engine-discussion-overview-20261010.md` and `wef-engine-identification-plan-20261010.md`. Evidence logs: `runner-pipeline-heartbeat.log`, `epoch-tagger.log`, `heartbeat-slot-orchestrator.log`, `schedules-dailyv2.log`, `schedules-calculatorv2.log`, `trips-dailyv2.log`, `publisher.log` under the runtime directory above. Logs mix local wrapper timestamps and UTC child timestamps; the cycle times above are explicitly converted to America/New_York.
