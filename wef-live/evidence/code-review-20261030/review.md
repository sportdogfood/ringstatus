# wef-live code review — 30 October 2026

Overall: **PARTIAL; corrective review gate FAIL.** No runtime fixes or deployment authorized/performed. Target Airtable base: **appgzosUcVqA4vZdf** (wef-live). Records is the sole review-table/schema/record writer for this request. Records reported review record creation and exact GET readback PASS: base appgzosUcVqA4vZdf, code-reviews table tbl1jEtJusopJGC6O, record rec5fLj3xEqkYBpxA. Live did not write the Airtable record.

Reviewer: independent publication_review agent; reconciled by Live owner chat. Skill: C:/Users/gombc/.agents/skills/code-review/SKILL.md. Scope: actual deployed Live publication v1, collector/adapter/entry changes, tests, source contracts and saved normal-run/audit evidence. No migration, retirement redesign, ownership change, sibling review, provider/source requests or manual collector invocation.

## Baseline and release

Release publication-v1-20261029, deployed 29 October 20:28:06 Eastern through existing wef-live-collector task. Comparison baseline is evidence/publication-v1-20261029/before (v1.6 collector/adapter/entry); new publisher modules have no preexisting counterpart. Current Git HEAD observed: 1737335ece1634f5652313e677b5d7e42ca96e68. All six deployed files match release hashes; see manifest.json for exact SHA256 and paths. No later material runtime delta detected in those files. Documentation/procedural updates are not a deployed code delta.

## Findings

### P2 / CONFIRMED — Detail availability becomes missing

File/symbol: wef-live/publication.js:28, observations(). Actual source/model uses has_JSON; publication reads has_json. Saved source contains has_JSON=true, while published hasJson becomes null. Consumers lose the agreed explicit availability signal. Independent offline reproduction confirms it. Smallest correction, requiring approval: use actual case-sensitive source property and cover true, false and missing using real spelling. Do not infer it from other counters.

### P2 / CONFIRMED — Early exit underreports unfinished work

File/symbol: wef-live/publication.js:118, publish() pending-write catch/break. It exits without setting stats.deferred for untouched plans. Saved 08:23 Data run reports 37 verified patches, an uncertain write and deferred=0 despite unprocessed targets. Independent fixture leaves five untouched plans with deferred=0. Consequence: operational backlog is understated. Smallest correction, requiring approval: count untouched plans before break and report uncertain pending separately, preserving cursor and later readback-before-retry.

### P2 / CONFIRMED — Independent acquisition changes stop publication

File/symbol: wef-live/publication.js:105,116–118, publish() before/after comparisons. Entire field objects include unrelated acquisition timestamps/raw tokens. A legitimate independent collector update can cause defer or a protected-value failure, retaining a verified overlay as pending and stopping that lane. Independent fixture confirms a last-success update triggers this path. Saved 08:29 run fd29f532-9a26-433c-b794-929a40bc5c38 shows only changed-at, row-json token refresh and dataset-id changes; no demonstrated Live overwrite of those fields.

Evidence intent: runtime/history/publication-schedules-intent/a8ea48138fd8fd8b147269689d8d6fe5b992607e5e6d629a574b9c754674983b.json.gz.

Smallest correction, requiring approval: revalidate identity, actual matching evidence, existing overlay and native links; retain unrelated acquisition changes in audit without classifying them as publication corruption. Actual identity/link changes must still withhold publication. No permission to relax source identity or freshness.

## Operational limitations and intentional guards

- Budget: 45-second independent lanes leave substantial work unprocessed; selected second run deferred 208 Data plans. Cursor rotation supports eventual progress but timely full coverage is unproven. Measure full rotation coverage before acceptance; no automatic budget/cadence/freshness changes.
- Ambiguous group/occurrence attribution and missing destination trip matches are intentional safeguards, not evidence to force joins. Keep withheld data unresolved.
- Earlier unresolved class 200030828/trip 200396076 remains saved proof; latest inspected 08:59 receipt instead records unresolved 200030709/trip 200399391 and 200030710/trip 200399393. These are source identity gaps, not authorization for repairs.
- Known publication acceptance remains incomplete; no two fully successful populated runs established. Whole-class completion is not proved by group status or progress counts.

## Tests and evidence actually checked

- Parent reran full deployed node --test wef-live/test/*.test.js: 80 passed, 0 failed.
- Independent reviewer reran 19 publication tests: all passed; focused in-memory reproductions confirmed the three findings. No runtime code or test fixtures were edited.
- Six deployment hashes verified independently and by parent.
- Previously verified populated runs: 3e77fe62-cc1d-470e-9f1b-593cb603a580 at 08:23 and 90506c25-51f9-4275-8418-29a54b38e325 at 08:26 Eastern. Both collected 175 entries, task exit=0 and matching Partial heartbeat/run-log receipts. 132 immediate verified writes plus one later reconciled write. Exact records/values/protected comparisons: ../publication-v1-20261029/populated-verification-20261030.json. This review inspected that evidence; it did not rerun provider requests.
- Latest inspected saved receipt 03b669b1-043f-445e-bcb9-2e680d47ea7a: 08:59:13–09:01:32 Eastern, collection ok=true, 6 rings/17 groups/27 classes/188 entries, Schedules 34 patches and Data 33, both Partial; no logging errors. At task snapshot 09:05 invocation was running; that is not claimed completed.

## Gate results

| Gate | Result | Evidence/limit |
|---|---|---|
| Exact deployed baseline | PASS | Six hashes match manifest |
| Existing offline regression suite | PASS | 80 tests; independent 19 publication tests |
| Correctness review | FAIL | Three confirmed P2 findings |
| One-field PATCH restriction / lane and Entries boundaries | PASS | Bounded allowlist; no Entries dependency/enrichment; reviewed writes preserve acquisition fields/links |
| Saved populated collection and logging | PARTIAL | Normal runs completed, errors isolated, source identity omissions remain |
| Complete timely publication / two accepted populated runs | PARTIAL | Budget/matching/identity gaps; full rotation not proved |
| Recurring archives protection | PASS for reviewed implementation and prior evidence | Unchanged implementation; prior seven-catalog/hash proof, no new archive acceptance claim |
| Airtable review-record publication | PASS — reported by Records | appgzosUcVqA4vZdf / tbl1jEtJusopJGC6O / rec5fLj3xEqkYBpxA; exact GET readback confirmed by Records |

## Exact next action and preserved boundaries

Records completed the review record and exact GET readback: appgzosUcVqA4vZdf / tbl1jEtJusopJGC6O / rec5fLj3xEqkYBpxA. Recorded result remains Review FAIL (3 P2), 80 tests PASS, populated acceptance PARTIAL; this readback confirmation does not authorize fixes. Live has documented the ongoing review requirement in README.md, scope.md and HANDOFF.md. Do not review unchanged code per invocation or create review monitors.

Any correction requires explicit applicable approval for these Live-only defects, tests, independent review, guarded deployment and subsequent NORMAL scheduled proof. No code/schema/business-data/task/cadence/source calls/manual runs/repairs were authorized by this review. Migration/retirement and Data/Season ownership remain closed. Preserve prior evidence, links and archives; do not force ambiguous or missing identity matches.
