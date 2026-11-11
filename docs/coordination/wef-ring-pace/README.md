# wef-ring-pace owner

**README version: 2.3 | Updated: November 8, 2026 | Timezone: America/New_York**

## Current status — November 8, 2026

The existing three-minute task still uses the installed snapshot writer and cleaner. The latest inspected normal receipt, at 7:34 p.m. Eastern, reports six Ring snapshots saved and no run errors. The cleanup receipt says `Cleanup not due` and records zero deletions. This is receipt evidence. The saved-record verification below covers the earlier 2:46 and 2:49 p.m. runs.

`wef-ring-pace/linker.js` is in the repository. Its staged caller fills empty show, day, ring, schedule, and current/next group, class and trip links before the single snapshot save. Existing links are preserved. Missing expected links or ambiguous single matches are logged and left empty. The staged cleaner implements the locked contract below. Independent review and both offline checks passed. The installed caller and cleaner still match their previous file hashes. Activation approval and normal-run proof are pending.

Evidence: `C:/Users/gombc/Documents/Codex/staging/wef-ring-cleanup-contract-20261108/` contains `CURRENT-RESULT.md`, `release-manifest.json`, `reviewer-final/review.md`, and `reviewer-final/review-verification.json`.

## New simpler script — activated and normal snapshots verified

Owner-approved new implementation `wef-timing/ring-snapshot.js` (SHA256 6b00215bf788806a297c44272c88dccfcdbf7f39f8d2147c0a51da26dac2baac) activated November 8 at 2:43:55 p.m. Eastern on the unchanged three-minute task. Thirteen approved current/next/end-time fields were created and read back. The only existing runtime changes are Ring read-only access to Live trips and the existing Ring call selecting the new script. Class code and task definition remain unchanged.

One immutable timing-answers snapshot per ring contains today's current and next group, class and trip links. Linking is built into ring-snapshot.js; no separate linker task. Current-group elapsed time divided by gone gives pace; remaining trips multiplied by pace gives end-in and estimated-end-time. The source start is estimated-start-time, explicitly labelled as an estimated basis. No old daily average, historical measurement reader, separate current cache or next-class record. Earlier Completed groups are not substituted for a current group. Zero gone leaves predictions blank; malformed inputs are logged while other rings continue. Early/late compares the projected end with that snapshot's saved schedule end when available.

Normal 2:46 and 2:49 p.m. runs each saved six ring snapshots. All current/next group, class and trip bindings and arithmetic passed independent read-only inspection; the first six snapshots remained unchanged after the second run. International's second snapshot is as of 2:48:06 p.m.: estimated start 1:45:01 p.m., total 36, gone 30, remaining 6, elapsed 63.0907 minutes, pace 2.1030 minutes/trip, end-in 12.6181 minutes, estimated end 3:00:43 p.m., 4.6745 minutes early against its saved schedule end. Five rings had no current group and no invented pace; Ring 6's next group, three classes and two trips appear on its same snapshot.

Existing ring-cleanup.js remains connected after scheduled execution. Both observed runs returned Cleanup not due, deleted zero: this verifies connection and not-due handling, not deletion. Those two shared receipts reported a separate checkpoint clock error. Ring subreceipts and saved outputs were verified independently. No manual business runs, Class changes, new schedules, source repairs or backfill.

Evidence: C:/Users/gombc/Documents/Codex/staging/wef-ring-current-snapshot-20261108/ — deployment-receipt.json, created-fields-readback.json, normal-first-run.json, normal-second-run.json, normal-verification-inspection.json and reviewer-normal/review.md. Creation and activation checks also passed independent review. The stopped verify-normal.cjs assertion compared an earlier saved schedule end with a later changed source; it remains recorded as failed and was not rewritten/rerun. Independent inspection separately confirmed each snapshot's subtraction against its own saved inputs. Earlier sections below preserve historical evidence and do not define the new calculation.

## Locked cleanup contract

Cleanup is delete-only and acts only on supported tables checked in Airtable's `cleanup` table. It runs through the existing normal runner.

| Trigger | What gets deleted |
| --- | --- |
| A log table reaches 1,000 records | Delete the oldest eligible records until 500 remain. If protected records prevent reaching 500, continue on later normal runs. |
| First normal run at or after 7 p.m. Eastern | Clear all eligible records from checked `heartbeat`, `run-logs`, and `errs-log` tables once per daily cutoff. |
| The default show changes | Fully reset checked supported live tables and clear eligible logs. The reset can include rows already collected for the new show. A date change within the same show leaves live records in place. |

## Known trouble — November 8, 2026

- **Installed cleaner differs from the locked contract.** It caps each quick pass at 500 deletions and can stop above 500. Its show-change path waits for a coordinator request and filters by show identity instead of resetting the full checked `timing-answers` table. These are still live differences until the staged correction is activated.
- **Activation and scheduled proof are pending.** The new linker is present and both staged changes passed independent offline checks. Verify the current default show is saved on the first normal wake, then check saved links in Airtable. Prove deletion when a normal cutoff, threshold, or default-show change makes cleanup due. The 7:34 p.m. cleanup receipt was `Cleanup not due` with zero deletions. Earlier successful deletions used earlier code.
- **Earlier offline cleanup check stopped on Windows `EPERM`.** The temporary state-file replacement failed during a large simulated deletion run. The failed check is retained. Revised targeted checks passed independently with smaller stateful fixtures. The original review findings on queued eligibility, unchecked tables and authority changes were addressed in the staged candidate. Default-show initialization still needs normal-run proof.
- **Earlier shared checkpoint clock error.** The 2:46 and 2:49 p.m. shared receipts reported `Host clock not verified against Airtable Date`. Ring snapshots from those runs were independently verified. The later 7:34 p.m. receipt reported no run errors.
- **Stopped schedule comparison.** `verify-normal.cjs` failed when it compared an earlier immutable schedule value with a later changed source. That check remains stopped. Independent read-only inspection verified each saved snapshot against its own inputs.
- **Calculation limits.** Pace uses the estimated start supplied by the source. No current group or zero gone leaves pace and end predictions blank. Five rings had no current group in the independently inspected runs.
- **Historical retention stop.** The separate two-base retention candidate remains stopped after failed verification.

Earlier complaint dispositions and dated evidence remain below.

## Earlier Ring calculations — November 8, 1:49 p.m. Eastern

Ring computes TODAY's as-of pace, minutes early/late, current class and next class. Upcoming waiting contributes no measured pace or initial estimate. Original successful saved Live receipt measurements are used for each group/phase/membership sequence when available; derived snapshots remain context and can provide measurement history only for uncovered sequences. Underway and Completed evidence remains usable without rereads changing the original measured clocks. This scope does not include Class calculation/consumption.

Reviewed source ef2e7d57e3a23e528a55a6fbd2dca67846802ec668ecb5b74ffc9316489abf3c deployed at 1:43:53 p.m. through the unchanged existing task. Normal 1:46 and 1:49 p.m. runs each saved six observations and updated the same six current answers. Actual native GET readback passed against 112 original saved source archives, including every measured endpoint/status/count, elapsed minutes/trips/pace division, selected source counts/raw clocks, schedule comparison, source class names and parsed fields. All thirty-four protected files match; Class, cleanup, task definitions and cadence unchanged. No manual workflow runs, source/record repairs, historical backfill or new schedules. Initial failed numerical proof at 1:28 p.m. remains preserved; it is superseded by this corrected-source proof, not relabeled successful.

The native timing-answers columns now expose calculation group/status/total/gone, source clock, estimated and scheduled starts/ends, comparison end, projected finish/basis, measured minutes/trips/as-of, ring state/offset, current/next names and readable original interval evidence in Eastern time. Each snapshot links only its selected calculation group, that group's class schedule rows and selected-plus-next class context. Historical measurement contributors appear in pace-evidence rather than all being linked to every snapshot. Group total/gone describe the selected group; pace-elapsed-minutes and pace-completed-trips describe compatible intervals across today's ring history.

| Current ring | Measured minutes / trips | Minutes per trip |
| --- | --- | --- |
| International | 188.8427 / 79 | 2.3904 |
| Rost | 117.0269 / 21 | 5.5727 |
| Mogavero | 174.0180 / 46 | 3.7830 |
| Denemethy | 143.9954 / 41 | 3.5121 |
| Ring 6 | 66.0300 / 18 | 3.6683 |
| Source Ring 8 (number 7) | 71.9978 / 12 | 5.9998 |

Actual transition verified: International changed from the last Completed group to Underway group200028632, class805 $20,000 Palm Beach Equine Clinic Grand Prix, total36/gone1 as of1:48:07p.m. Eastern. Its original-source pace2.3904135021097046 projects3:11:47p.m., compared with saved scheduled end3:31:55p.m.:20.1215minutes early. Native current ID stayed the same, selected group link changed, current class source name matched, and the first run's immutable snapshot remained unchanged. Next class is absent in that source snapshot.

Completed-group finish-basis is first_seen_completed: original collector confirmation, not an exact official finish timestamp. All states are as of their displayed source clocks; later scheduled evaluation does not manufacture a fresh source observation. No remaining acceptance blocker for this approved Ring correction. Evidence: C:/Users/gombc/Documents/Codex/staging/wef-ring-pace-arithmetic-20261108/normal-verification.json, active-transition-verification.json, deployment-receipt.json, reviewer-original/, and the corresponding normal-run/native-readback/source-fixture/original-manifest files.

## Earlier November 8 — source links verified; arithmetic failure evidence

Native source links are now present on timing-answers: w-shows, w-days, wef-live.rings, wef-live.groups, wef-data.schedules and wef-live.classes. The existing scheduled writer populates exact matching today's show/day/ring source context, including completed/next groups, nondropped schedules and member classes. These links open supporting current source records; original historical observation evidence remains in the answer payload. Historical answers were not backfilled.

Independently reviewed one-file change deployed at 1:02:47 p.m. Eastern. Normal 1:04 and 1:07 p.m. runs each saved six new observations and updated the same six current answers. Connector GET readback verified all six native link arrays for each saved answer against the source record identities. First-run snapshots remained unchanged. Installed hash matches frozen candidate 7905a847d7289a6370ceed76c263628fe7939f689d49134d2e7ee80d0d8ba819; thirty-four protected files, Class calculator, cleanup and task settings are unchanged. No manual workflow run or new schedule. Evidence: C:/Users/gombc/Documents/Codex/staging/wef-ring-source-links-20261108/normal-verification.json, schema-receipt.json, deployment-receipt.json, reviewer-empty-links/.

The original check's linked-record object expectation failure was corrected under fresh approval. Independent review then found omitted-empty-array responses would cause false write failures; a separately approved Ring-only response normalization corrected this while preserving the shared/Class verifier. Original failure packets remain preserved; fresh independent execution and review passed.

**Numerical pace is not accepted.** Today's read-only source audit confirmed every inspected ring's pace includes waiting while groups were Upcoming, inflating minutes per trip. At the audited 12:52 p.m. evaluation, published/reference observed intervals were International11.0736/2.3746, Rost18.1164/5.5727, Mogavero12.6730/3.7830, Denemethy21.1183/3.5121, Ring6 17.0021/3.6683 and source Ring8 10.2893/5.9998. The reference calculation uses original compatible Underway/Completed observations, excluding Upcoming wait; it is a read-only diagnostic, not deployed replacement code. International's 12:51 source snapshot projected16.89minutes late using inflated pace versus approximately0.50minutes early using the diagnostic interval pace. Completed-group offsets subtract scheduled end from first-seen Completed; completion is known within approximately3minute observation windows, not exact finish timestamps. This links-only change intentionally does not alter arithmetic. Older successful-save/repeat-run claims below do not establish useful numerical pace.

## Historical Ring snapshot delivery — November 7

Deployed at 9:42:58 p.m. Eastern through the unchanged existing wef-timing-data task. Normal 9:43 and 9:46 p.m. runs each saved seven new immutable Ring observations and updated the same seven current answers in timing-answers. Independent connector GET readback verified all fourteen new snapshot IDs, exact payload hashes, day-0 identity, pace/state/current/next data, retained original source/measurement clocks, unchanged first-run observations and the unchanged Ring 4 diagnostic record. Source data remains the saved daytime November 7 evidence; evening evaluation is not a new source observation.

Ring now evaluates every dynamically discovered ring every three minutes, TODAY Eastern only, using synced rings/groups/classes, saved schedules and original successful collector receipt history. Completed groups remain usable. Unchanged measured counts preserve the proven pace and interval clocks. Final Completed-group selection follows group sequence and uses its first matching original Completed receipt; no ring reread is treated as a finish. Questionable inputs are logged per ring and other calculations continue. No fixed ring count, new schedule, schema, dependency or manual workflow run. Current/next classes describe Ring context only.

Fresh independent review COMPLETE/PASS and one independent offline execution passed all thirteen targeted checks. Installed three-file hashes match the frozen release; thirty protected files, Class stage, old core/consume and deployed cleanup remain unchanged. The existing shared Class checkpoint clock failure remains outside Ring acceptance; Ring publication succeeded before it. The previous initial deployment refusal occurred while the existing task was busy, before changes; installation succeeded once the task was idle.

Read evidence: C:/Users/gombc/Documents/Codex/staging/wef-ring-snapshots-20261107/normal-verification.json, deployment-receipt.json, installed-hash-verification.json, normal-first-run.json, normal-second-run.json and reviewer-two-fixes/. This release supersedes older pace-stopped, Underway-only, and hourly/Live-off Ring cadence statements below. Cleanup policy and the stopped historical two-base retention package are unchanged.

## Historical cleanup release — November 7

Deployed at9:11p.m. Eastern through the existing wef-timing-data task. Completed Ring logs, including runs in the existing run-logs table, clear at/after19:00Eastern once daily; the1000/oldest500 check is due every3hours on existing wakes. Genuine coordinator new-show requests clear eligible Ring-owned old-show records. Error records retain their parent completion evidence until deletion is confirmed, including across budget continuation. Active/unfinished operations, retained answer references and synced/control tables remain protected. No new tables/schedules, local-log deletion or Records alerts. Class code/execution is unchanged.

Independent source review and one independent offline execution passed13targeted checks. The writer's earlier offline Windows EPERM state-file rename failure is retained; the unchanged baseline atomic-write method was not patched or retried. Exact review and failure provenance: C:/Users/gombc/Documents/Codex/staging/wef-ring-cleanup-alignment-20261107/reviewer-parent-order/.

Only the approved November3 clock-error stop was preserved/cleared at installation, after confirming pending:null and work:[]. Date-header mismatch is now a cleanup-only logged warning; other lanes remain unchanged. Normal9:13p.m. cleanup completed without errors, recorded the daily check and three-hour check time, and correctly skipped deletion because native run-logs/heartbeat/errs-log each contained0records. A safe skip is not actual deletion proof. Normal9:16p.m. repeat correctly returned Cleanup not due and preserved the next three-hour check at00:13:07Eastern November8. Native tables remained0/0/0; no manual runs or seeded records. Exact proof: release normal-verification.json and RESULT.md.

The cleanup release alone did not establish pace calculations; the later verified Ring snapshot release above does. Older cleanup/cadence sections below retain dated evidence and are superseded only for the cleanup alignment described here.

## Historical cadence and cleanup delivery — November 2

The agreed outcome includes **both cleanup and Ring cadence following Live**. Both implementations are now deployed. Actual new-show deletion and the current Live-off cadence behavior are verified through normal scheduled runs. I previously narrowed delivery to cleanup code while leaving cadence prepared; that delivery omission is now corrected within the verification scope below. A successful install or safely skipped run alone did not establish cleanup; the actual deletion evidence does.

- **Cadence:** deployed November 2 at 3:27 p.m. Eastern. Ring checks the published overall Live status hourly while off, unavailable, on the wrong date or overnight; Live on for TODAY selects three-minute Ring calculations from 5 a.m. to 7 p.m. Eastern. The primary input is Live's actual `live-check-last-run.json`; source run/time/date stay original. The shared task still wakes every three minutes for the unchanged Class path; Ring skips its receipt read and calculation until due. NORMAL 3:28 selected hourly Live-off mode; NORMAL 3:31 retained the original source/check clocks and next check at 4:28 p.m., with zero Ring evaluation/publication. Daily checkpoint and original Live receipt were GET/read back. Current natural off-mode is verified; the full next hour and natural Live-on/TODAY branch were not observed. Their decision/progression cases passed targeted independent offline checks. Evidence: `C:/Users/gombc/Documents/Codex/staging/wef-ring-live-cadence-20261102/RESULT.md`, `normal-verification.json` and `reviewer-timing/`.
- **Cleanup:** actual new-show deletion verified on November 2. The coordinator recovered the genuine Shows discovery event `62dd0e6c-3a67-4462-bef5-75a81c4c9036` with original discovery time `2026-11-02T06:04:20.972Z`, then published the approved request. NORMAL 2:55 and 2:58 p.m. Eastern runs deleted 100 and 38 eligible old Ring answers. Independent exact-ID GET readback confirms all **138** absent; counts fell **214 to 76**, all retained record field hashes match, and retained Class references were checked read-only. The action completed, request was consumed, and no pending work or cleanup stop remains. Airtable cleanup summary reads Success, 138 deleted, zero archived. No manual runs or source repairs. Evidence: `C:/Users/gombc/Documents/Codex/staging/wef-ring-cadence-cleanup-20261102/delete-only/coordinator-request-verification-20261102/actual-deletion-verification.json` and `cleanup-summary-readback.json`.
- **Completed-group pace:** the current selector remains Underway/live only. Genuine November 1 saved snapshots supported measured intervals for all five rings observed that day; evidence is `C:/Users/gombc/Documents/Codex/staging/wef-ring-completed-calculations-20261101/results.json`. Those are saved prior-day calculations on November 2, not proof the deployed workflow publishes Completed pace or current-day results.

Preserve the full owner scope and existing approvals. Cadence and new-show cleanup delivery above do not establish numerical pace or repair the separate Completed-selector gap. Do not invent an alert, eligible records, source-data cause or manual-run substitute. Documentation and complaint maintenance authorize no runtime edits, deployment, record repairs or new tests by themselves. Preserve Class code/execution, source clocks and current/future/synced data. Known complaint records below preserve their actual existing dispositions; this release does not automatically close them.

## Known complaints - November 2

All **14 existing complaints remain Open**. The remediation column records only the existing scoped correction state; it does not close a complaint or certify current service. The delivery, repeated-explanations and Completed-status records received the new November 2 evidence. Other records retain their prior history and state; no duplicate complaints were created.

| Existing complaint | Remediation state |
| --- | --- |
| [Ring date queries deployed with incorrect Date-field comparisons](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/rec3nSWI0sEwb1wKZ) | Verified |
| [Reported Class publishing without stating calculations were unavailable](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/rec9uL24WAmSiCieD) | Proposed |
| [Proposed a fixed five-ring plan for a recurring workflow](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recAqsZuISar7qYgM) | Verified |
| [Left the agreed Live-following cadence unfinished while delivering cleanup](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recC0Oi1veM19KZKT) | Proposed |
| [Repeated explanations and apologies instead of advancing the authorized outcome](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recEr4MSulzxWPraT) | Proposed |
| [Extracted source helpers as an unrequested refactor](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recH3JpN5P357W8bp) | Proposed |
| [Test preparation omitted required source scope field](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recMKoq1jjXZf0A5n) | Verified |
| [Added compressed checkpoint format without establishing scope approval](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recMl8YrhKdUygO3J) | Proposed |
| [Used obsolete Class test expectations and later copied them again](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recN67nugmmySfAuw) | Proposed |
| [Repeated preservation counts instead of addressing usable today-only pace](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recPPNDlhquM4GgJy) | Proposed |
| [Offline transport and review missed Airtable Date semantics](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recZ2E6hVHwN08Pp5) | Verified |
| [Continued involving class-calculator after Ring scope correction](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recnTpm1RygVfVl1A) | Implemented-unverified |
| [Misattributed the two-run requirement to the owner](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/reco9BAv4010FLhWZ) | Proposed |
| [Narrowed implemented status handling to Underway/live groups](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U/recyRABftnIufoidq) | Proposed |

## Historical cleanup release - November 2

**Actual deletion verified at 2:59 p.m. Eastern:** the genuine coordinator request was consumed after 138 eligible old answers were deleted through the normal 2:55/2:58 scheduled runs. Exact-ID absence and retained-record hashes were read back; no manual workflow was run. This supersedes the earlier no-deletion-proof condition below.

Ring's owner-approved **delete-only cleanup is deployed** after the existing shared workflow. The task remains every three minutes; Class code and execution route are unchanged. Only `timing-answers`, `run-logs`, `heartbeat` and `errs-log` are checked in the existing `cleanup` table. All synced Live tables, checkpoints, catalogs, reviews and other references are excluded. No cleanup archive is created or required; existing archives/catalogs remain untouched.

A genuine `wef-timing-coordinator` new-show alert is recorded through the approved `runtime/data/cleanup-request.json` handoff. The runner selects eligible non-current-show records; it does not infer an alert from a changed show value. Independent log cleanup checks the existing 1,000/oldest-500 eligible rule after scheduled work. Minimum age, current/future records, active/unfinished operations, retained Ring/Class references and unresolved errors remain protected. Class is read-only for reference protection. Deletion responses and absence are verified; uncertain deletion stops cleanup while the shared calculation path continues. Current error rows have no established resolution source and remain protected.

Normal **2:22 and 2:25 p.m. Eastern November 2** executions completed cleanup without errors, both correctly skipping deletion: all three log tables contained zero records and no coordinator request was pending. No cleanup stop or unfinished deletion remains. This proves deployed execution and repeat behavior, not a natural deletion event. Shared calculations separately deferred because TODAY is outside the selected show's dates. No manual runs or Class changes occurred. Evidence: `C:/Users/gombc/Documents/Codex/staging/wef-ring-cadence-cleanup-20261102/delete-only/RESULT.md` and `normal-scheduled-verification.json`; independent review/actual offline execution: `reviewer-reference-protections/` in that stage.

The cleanup release did not itself deploy cadence; the later reviewed two-file cadence release above now does. Neither repairs the Completed-group selector. Older archive-required/undeployed-cleanup/cadence-preparation statements below are historical where they conflict with the current sections; the older two-base retention package remains stopped and was not deployed.

## Current responsibility

Read today's saved show/day, ring, group, class, trip and schedule data. Discover rings from the source. Save one immutable as-of snapshot per ring containing its current and next group/class/trip context.

For the current group, divide elapsed minutes since its estimated start by gone. Multiply that pace by total minus gone to estimate minutes remaining and end time. Compare projected end with that snapshot's scheduled end for early/late state. Isolate questionable ring inputs and log them while other rings continue. Current and next links are built into the snapshot writer; no separate linker task.

Class calculations and consumption, collectors, source repairs and messaging remain outside Ring scope. Completed historical groups are not substituted for today's current group by this implementation.

## Cadence and outputs

Existing `wef-timing-data` wakes every three minutes through VBS → PowerShell → `entry-data.js` → `data-runner.js` → `ring-snapshot.js`. Ring writes `ring-observation` rows to `wef-ring-pace` base `appYOykKIy1kAquhP`, `timing-answers` table `tblhIjSjeGs5td23c`. New snapshots do not update old `ring-current` cache rows, write separate next rows or use a history checkpoint. Source as-of comes from the current group's `last-seen`, or ring `last-seen` when there is no current group; preserve `source-updated-at` separately.

Existing `ring-cleanup.js` runs afterward on the same scheduled path. See the locked cleanup contract and known implementation differences above.

## Verified release and remaining gaps

The November 8 normal 2:46 and 2:49 p.m. snapshot inspection is the saved-record proof for the installed Ring writer. The staged linker and cleaner have independent offline proof and await activation. Historical date-filter, history-pace, Live-off cadence and cache claims below do not define this implementation. The inspection verified link bindings and current-group arithmetic. The cleanup calls in those runs were not due. The implementation differences above come from source inspection.

No runtime changes, deployments, tests or manual production runs were performed for this README correction. Separate stopped paths remain stopped. Follow [workspace instructions](WORKSPACE-INSTRUCTIONS.md), applicable AGENTS.md and the latest human scope.

## Historical notes — not current operating status

The dated material below preserves earlier decisions, evidence and stops. Its older “current,” “pending” and ownership statements are superseded by the November 8 scope and verified release above where they conflict. Skill version numbers below are historical installation receipts, not this README's version. This documentation version does not change software, tasks or deployment.

## Current procedures - installed0.2.2 application receipt, October30 2026

Explicitly read this solely Ring-owned entrypoint on resumption; no autoload, scheduling or future-compliance guarantee is claimed. This section supersedes old installed0.2.1 and pending-clarification statements below only where current facts conflict; responsibilities, established cadence, source contracts and retention boundaries remain.

Apply canonical [CodeReview](C:/Users/gombc/.agents/skills/code-review/SKILL.md) when invoked/required: inspect actual code, downstream state and failure paths; independently execute authorized required offline checks, save reviewer-owned report plus exact commands/cwd/runtime/UTC/exit/stdout-stderr, use review-receipt.json and its canonical scripts/verify_review.py. Distinguish inspected/recovered outputs from own executions and offline/release/scheduled proof. A COMPLETE review may find candidateFAIL; neither permits repair/retry or deployment. Missing evidence staysPARTIAL/BLOCKED.

Apply canonical [ContextCompact](C:/Users/gombc/.codex/skills/context-compact/SKILL.md) and its scripts/inspect_context.py at required resumption/handoff/active-work boundaries. Reconcile latest actual own human request before acting; keep state.task_scope objective,target,allowed/excluded effects,nextaction and helper owner-message anchor current. Preserve oldprivate resume/state, oneCURRENTresume and private transcriptarchive; inspect actual semantic evidence then run checkpoint verifier. MechanicalPASS is not semanticproof or a hostcompaction claim.

Installed ringstatus-operating-skills0.2.2, releasepluginrel_6ae52435d3988191a144d87f9807acf6. Full installed procedures, review packet format and both helpers read; four installed/canonical hashes matched. Actual disposable OFFLINE exercise and simulated continuation evidence reside under `.git/ringstatus-control/skill-proof-20261030-v022/timing-subagents/ring/`; adoption-receipt.json records both verifier results. No product code/tests, collectors, tasks, businessdata or live source checks are authorized by this exercise.

Latest own product request explicitly APPROVED Class fallback2.5746onlywhen compatibleRingpaceabsent, review/deploy/normalruns. That clarification is RESOLVED. Current fallback is stagedONLY: writer36/40failed from staleexpected3,two strictfloating comparisons,and outdated copieddependency alignment. Runtime candidate5018658e onlychangesfallback/label/flag; no new runtime defectconfirmedbyRing source inspection. Independentexecution/deploy heldpendingfreshscopedtest-onlyrepairapproval. Preserve legitimate newerShows1.6 sharedfiles,currentnative-sync/finalpace/contracts,Dataauthorityalignment dependency; do not overwrite them with oldfixtures. Retention remainsseparate stopped andEntry paused. No manual productionruns, failedpathretries, blanket approval or extraacceptance scans from this guidance.

## Current procedures and installed skill receipt — October 30, 2026

This is the solely owned Ring guidance entrypoint. Read it explicitly on resumption; it does not load itself, schedule anything, grant product authority or guarantee future compliance. Older source/deployment snapshots below remain historical where they conflict with the accepted native-sync/final-pace release.

Use the stable canonical [code-review procedure](C:/Users/gombc/.agents/skills/code-review/SKILL.md) when invoked or required: independently inspect the actual change and downstream/failure paths; save a reviewer-owned report and exact executed command/cwd/runtime/UTC/exit/stdout-stderr evidence in an authorized location. Label inspected writer logs, recovered outputs, deployment and scheduled evidence separately. Read back report/logs and match frozen hashes. Findings do not authorize repair or failed-path retries; passing tests do not prove integration. No new product review or test is requested by this guidance update.

Apply the stable canonical [context-compact procedure](C:/Users/gombc/.codex/skills/context-compact/SKILL.md) at resumption, material handoff and its active-work checkpoints. Use its [metadata/checkpoint helper](C:/Users/gombc/.codex/skills/context-compact/scripts/inspect_context.py) with this exact session and private thread checkpoint. Preserve old resume/state and private transcript archives; keep one current resume section. Read back current source evidence and verify hashes and semantic continuity. A mechanical PASS is not semantic proof or a claim that compaction occurred. No new monitor or schedule is created.

Installed receipt: ringstatus-operating-skills **0.2.1**, release **pluginrel_6ae515dbc94c8191802aefd9eadde79d**. Full installed code-review, context-compact and helper were read; all three SHA256 values match their canonical copies. Evidence: `.git/ringstatus-control/skill-proof-20261030/timing-subagent-rollout/ring/receipt.json`. Exact session: `C:/Users/gombc/.codex/sessions/2026/10/24/rollout-2026-10-24T14-06-53-01a16f18-1cfe-7663-85a5-71dc2bbe3576.jsonl`. Private checkpoint: `.git/context-recovery/01a16f18-1cfe-7663-85a5-71dc2bbe3576/`.

Current accepted product state is documented in `C:/Users/gombc/Documents/Codex/staging/wef-ring-sync-final-20261030/COORDINATOR-ACCEPTANCE.md`: native synced Live groups in Ring base, TODAY/three-minute uncapped calculation, Completed pace usable with original clocks and local count flags, real Class consumption, Entry disconnected. The reviewed deployment and two normal runs are complete. Retention is separate and stopped; do not revive failed packages or run extra acceptance scans. Latest human said “2.5746 minutes per trip, as new fallback”; Ring asked whether this replaces Class’s fixed three-minute fallback when compatible calculated pace is absent. That clarification is pending; no fallback edit is approved or performed. This guidance delivery changes no calculator, collector, business record, task, cadence, retention or approval boundary.

## Controlling ownership update — October 30 specialist consolidation

Read [Specialist consolidation](SPECIALIST-CONSOLIDATION-20261030.md) first. Ring-pace now owns **ring conditions and input validation**, absorbing the state/condition and validity/freshness specialist evidence. Timing coordinates common policy and Records/Messages handoffs. Class, entry, conflict, Results and message delivery remain with their owners. The source chat IDs, exact evidence paths, superseded proposals, pending decisions and explicit stops are retained in that handoff. This documentation update activates no evaluator or monitor and archives no chats.

The older 11:07 snapshot below is historical where it conflicts with later verified work: current calculations are **today-only Data + Live**; ring answers go to `appYOykKIy1kAquhP/tblhIjSjeGs5td23c`, class answers to `appeqxxJyPGS1iqdm/tblmoBFySYW2TkDHf`. The current launcher reports failure without automatically disabling the normal schedule; pending-write/deletion uncertainty still blocks work. Existing task route remains `wef-timing-data` → hidden VBS → PowerShell → `entry-data.js` → `data-runner.js`. No older Schedules/Engine lane, SQLite plan or one-minute cadence is revived.

The [common freshness/completion contract](../wef-freshness-completion-contract-20261030.md) controls the approved Live-first overlapping-field semantics; desired corrections are not claimed deployed by this README. Current retention remains gated. The latest reconciled retention candidate at `C:/Users/gombc/Documents/Codex/staging/wef-timing-retention-reconciled-20261030/REVIEW-HANDOFF.md` stopped at 151/159 offline passes with eight failures. Fresh approval is required before its correction/rerun; documentation consolidation does not clear that stop. Historical proposed five-minute operational monitoring is rejected and must not be revived. Existing independently approved review schedules are unchanged.

Updated October 30, 2026, 11:07 Eastern. This is owner documentation, not implementation or deployment approval. Read [workspace instructions](WORKSPACE-INSTRUCTIONS.md) on resume.

## Identity, hierarchy and scope

Owner chat: `01a16f18-1cfe-7663-85a5-71dc2bbe3576` (wef-ring-pace). Actual workspace: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`. This dedicated owner folder does not change shared root instructions or ownership of shared `wef-timing` code.

Human owner sets scope and approvals → wef-timing-coordinator `01a169bc-0bd6-77d3-b7b4-283a07061e90` coordinates contracts, integration, independent review and scheduled acceptance → ring-pace owns pace/delay interpretation only. Codex inspects and implements specifically approved repeatable changes; registered runners execute business workflows. A chat agent is not a collector or runner.

Implemented responsibility: schedule-window estimated pace and compatible observed interval throughput, explicit availability/reasons, exact scope/provenance, immutable answer revisions and interval baselines. Exclusive planned responsibility also includes supported ring current/next/completed interpretation, projected group finish, drift and downstream delay. Those additional outputs are NOT established as implemented. Class owner owns occurrence duration/start/end and its fallback; entry owner owns entry go-time/order; state/conflict/freshness owners retain their separate contracts. Results, messaging, publisher and Astro serving are separately owned. Shared runner integration assignments do not transfer their calculation ownership.

## Sources of truth and consumers

| Producer / destination | Exact facts and keys |
| --- | --- |
| Shows `appCaSmL819KK7UFJ` / shows `tblJA5CUon5s1sWib` | Canonical unique default customer/show and selected date: `show-key=customer_id|show_id`, `customer_id`, `show_id`, `is_default`, `start_date`, `end_date`, `selected_date`, `last_checked`. Selected date is not automatically today. |
| Data `appdl16HJ5ETm8N69`, retained local handoff | Pinned `wef-data/runtime/handoff/head-<customer>-<show>.json`; exact run/head/hash and authority fence. Verified complete dated inventories carry customer/show/date/ring/group, `stage-key`, `class-id`, `start-time`, `end-time`, `total-trips`, type, original observation and archive/collection references. No live acquisition by Timing, Schedules fallback or SQLite. |
| Live `appgzosUcVqA4vZdf` | `collections` with successful dated completion, row count, fingerprint and archive-file; archived groups with customer/show/date/ring/group, class roster, total/gone, last-seen, status and source-json. Validated class evidence is consumed by the class owner. Main/JO/R2 are distinct; do not relabel counts or infer actual starts from earliest trips. |
| Ring destination `appYOykKIy1kAquhP` | Base name wef-ring-pace, formerly wef-timing. `timing-answers` `tblhIjSjeGs5td23c`; `engine-checkpoints` `tblfWyTqeyvIvhAjb`. Shared current runner also writes class-timing here; no production destination transfer to class base is established. |

Shows/Data freshness must be under 30 minutes; Live under 10 minutes. Equality expires, future timestamps are rejected. Replay/new publication does not renew the original observation. Data/Live archives prove inputs, not archival of Timing outputs. Missing/stale/partial/unavailable facts must not become zero, fresh readiness or completed operation.

Ring `scope-key` is `data_live|show|date|ring|group|base`; customer identity remains in payload and validated context (not an extra invented scope segment). Runner `answer-key` is scope + run ID + first 16 content-hash characters. Consecutive unchanged content is a no-op; returning A→B→A changes get new revisions. Preserve `answer-kind=ring-pace`, source-lane, show-id/date/ring-number/group-id/phase, availability, pace-method, pace-minutes-per-trip, evaluated-at, observed-at, reason-codes-json, input-refs-json, payload-json and content-hash.

Schedule pace requires one supported class member, positive same-day window/count; multi-class aggregation, breaks/schooling and invalid evidence stay unavailable. Observed pace is elapsed observation minutes / newly completed trips with matching roster, totals and membership; preserve both endpoints. It includes pauses and is not individual trip duration or an invented actual start/end. Compatible observed pace is preferred to schedule estimate. Class timing consumes verified stored pace and owns its labeled three-minute fallback.

Snapshots are immutable as-of evidence, not a serving lease, current-answer API or committed downstream publication. An incorrect pace/identity/freshness claim can contaminate class timing and later entry/conflict/notification decisions. Ring answer saves do not prove Messages, Results or publication handoffs. The manifest/head publication protocol and those integrations remain unverified/unimplemented under current scope.

## Actual workflow and cadence

Read-only task inspection at this update: `wef-timing-data` was Running, every **3 minutes**, `IgnoreNew`, Windows task limit **175 seconds**. Existing route: `wef-timing/run-hidden-data.vbs` → `run-data.ps1` → `entry-data.js` → `data-runner.js`, directly in this repository. Internal deadline 160 seconds, process hard limit 165 seconds. No agent is in this execution path. The old `wef-timing` Schedules/Engine lane is historical and must not be revived.

Each eligible invocation validates Shows and pinned Data, reads verified Live evidence, evaluates every eligible group/class without processing caps, writes changed snapshots, independently reads exact written ID→answer-key associations and fields, then persists checkpoint. It protects pending deletion intents and source changes. Existing calculation scope includes eligible future days; changing to today-only remains a separate decision. Producers collect independently; Timing does not launch them.

Latest saved receipt inspected: `wef-timing/runtime/data/last-run.json`, run `ae7858f9-3f54-47c3-8072-de0c47243f91`, **October 30, 11:04:01–11:04:31 Eastern**. It reports ok=true, 202 groups and 372 classes evaluated, all unavailable/unchanged, zero writes/deletes and `archive_required`. This is a saved receipt, not new independent Airtable acceptance. The 11:07 invocation was running when inspected; its result was not claimed.

Current source has fixed `archiveReady=false`; there is **no active Timing archive cadence**. Every-three-minute checks reporting archive_required are not archival. Logs are `runtime/data/runner.log` and `last-run.json`; `runner.lock` protects overlap. Canonical checkpoint contains baselines, source proof, outcome and any purge intent. Hard failure stops the task through the existing launcher; no blind retry, alternate run or lock removal.

Source checkout HEAD inspected: `ddb39509198b766883848ff156fef65548c7f3e1`. SHA-256 at inspection: data-runner.js `C01DC35E7BA585487EF6843B834AA1FA9442B2D5D2CECABCB8645E996590B867`; pace.js `B81A95C94A1CD18C428978ED89C5273C5035C40685CB0A3490A20994C154BB2D`; api.js `339D39A13364BDD0BEDCCCBC50DF0230B6536D473A391A6C151126967E2A54F0`. These are inspected source hashes, not proof of a running process's loaded version.

## Retention policy and explicit stop

Required policy for both ring and class bases: **24 hours minimum age**, full-record compressed archival with exact IDs/fields/links, verified decompression/hash/one-to-one coverage, durable base-local catalog write/readback before deletion. Protect current/future show-day and operational dates for every show, ambiguous identities, active checkpoints, unresolved evidence and synced Live tables. Preserve exact intents, deletion response and absence proof; uncertain outcomes stop. No archive/index disposal is authorized.

Owned ring operational tables: run-logs `tblpWjnTv7S0CBKLV`, heartbeat `tblOeNxsfjCawlEmu`, errs-log `tbliHowblBdSL51ov`, code-reviews `tblZ9hOKkfGMAPp8v`. Catalog `tbl1qyii3z32hBiq9` exists from prior independent schema readback. Table existence is not writers, review execution or archival implementation. Active/mutable checkpoints, unresolved errors and reviews cannot be deleted merely because they are old. Meta and Live synced references are outside cleanup.

**STOPPED:** two-base retention package `C:/Users/gombc/Documents/Codex/staging/wef-ring-pace-retention-20261030`. Earlier 144 offline passes are dated evidence superseded for readiness by independent failed starvation verification: repeated cleanup passes can revisit the same records indefinitely and miss later records or the class base. Coordinator reported this failure and requested fresh **APPROVED TO EDIT**. Do not patch, deploy, rerun failed verification or resume it under documentation/review authorization. The earlier duplicate-record deletion comparison was fixed in staging; that does not resolve starvation. Staging README/manifests are not release approval.

Class base `appeqxxJyPGS1iqdm` and its catalog `tblLepghPma7hV2Hw` exist; its separate retention pass remains stopped staging, not wired production. Existing class answers still go to ring base. Runtime class GET access was coordinator-reported; class catalog write/delete permissions and two-base archive/deletion/scheduled proof remain gaps. Do not invent a class producer or migrate outputs.

## Review, evidence and next action

Before this setup, local automation inspection found no ring-pace/timing-specific review. Owner-authorized review heartbeat **wef-ring-pace-monday-code-review** is now ACTIVE and readback verified, targeting this exact chat, **Mondays 11:00 America/New_York**, maximum **20 minutes**. First scheduled opportunity: November 2, 2026. Configuration lives at `C:/Users/gombc/.codex/automations/wef-ring-pace-monday-code-review/automation.toml`. Host/app availability is required; no execution has yet been proved.

The heartbeat is strictly read-only: saved evidence/source/task inspection, no tests on stopped work, no file/Airtable writes, no runs, fixes, schema/task/deployment changes, agents or cross-chat messages. Quiet on unchanged/non-actionable state. It does not replace independent review by a different reviewer before release. For own authored code, label owner review accurately; ask coordinator for independent review under the accepted assignment, never claim the writer reviewed itself independently.

Periodic Airtable review-record identity is **not established**; no record was created under this documentation task. Preserve existing code-reviews table. Review results live in this chat unless separately authorized otherwise. Keep findings separate from offline tests, deployment, scheduled acceptance, source freshness and archive integrity. Reuse independent findings only after matching actual reviewed source/tests/contracts; findings never authorize fixes.

Immediate authorized outcome: maintain these owner documents and the bounded review heartbeat; send evidence-based receipt to coordinator. Retention next action is an owner decision on fresh APPROVED TO EDIT, followed by scoped correction, independent review and separately authorized safe deployment/normal scheduled proof. No business changes are authorized by this README.
