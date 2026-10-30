# WEF code-review and base evidence standard

Owner requested October 30, 2026 for Shows, Live and the local Season coordinator's distinct Data acquisition and Season retention duties.

## Procedure

Use `C:/Users/gombc/.agents/skills/code-review/SKILL.md`. Review the actual authorized changes against the proven baseline, current deployed hashes, requirements, failure behavior and downstream contracts. Independent review is read-only; the writer's summary or a passing test count is not independent review. Reuse a completed independent review only when its exact reviewed runtime/test hashes and relevant contract are unchanged, recording the reuse verification separately.

Review before deployment of authorized material changes and after observed behavior/verification failures. Preserve independently owner-approved recurring reviews. The owner's later October 30 request explicitly authorized periodic reviews: the verified four-review schedule below now applies, preserving Data's existing Monday 09:00 America/New_York timing. No other review timers, resident agents or collector triggers are introduced. Unchanged collector invocations do not require repeated code reviews.

Keep each agent in its own module. The Season coordinator records Data and Season separately, with a local record in each of the four Season bases. Shared implementation review may be referenced by all four records; do not claim four independent reviews or four new scheduled tests when only one shared review occurred. Shows stays locked except explicit approved changes. Entries remains linking-only and is outside this new review-table assignment.

## Table and record contract

Use the matching `code-reviews` table in each base, reusing Data's existing six-field schema:

| Field | Meaning |
| --- | --- |
| routine | Release-specific module/responsibility/review identifier; a real review entry, not a fabricated collector invocation |
| last-review | Actual review date, or reuse-check date explicitly distinguished in notes |
| last-result | Honest code-review result and separate operational acceptance result |
| next-review | Only an actually agreed calendar review date; leave blank for change-triggered review |
| cadence | Agreed review trigger or existing owner-approved cadence; does not itself schedule execution |
| notes | Consistent `wef-code-review-v1` evidence and findings, described below |

Append distinct dated/release review records and preserve prior evidence. Do not overwrite a pending recurring-review setup record to imply that future run already happened. Check for the exact routine identity before creation; read back all saved fields. Unknown write outcomes require reconciliation, not blind duplicate creation.

Notes identify module/responsibility/base, release and exact baseline/hash manifest; reviewer and actual review/reuse time; scope; severity/file/symbol/trigger/consequence/smallest correction for each actionable finding; inspected/executed tests; deployment state; normal scheduled proof; unresolved risks; evidence paths and exact next authorized action. Local paths are same-PC evidence, not hosted download URLs. Do not copy secrets or whole chat transcripts.

Separate PASS/FAIL/PARTIAL/BLOCKED gates for code review, offline checks, deployed version, scheduled acceptance and source coverage/freshness. A successful review does not renew stale source timestamps. Partial publication, missing evidence, intentional withholding and unresolved defects are not universal PASS. A finding does not authorize a fix. Applicable approval/failure-stop boundaries still control all code, deployment, task and business-record actions.

## Verified base destinations

| Responsibility | Base | code-reviews table |
| --- | --- | --- |
| Shows | appCaSmL819KK7UFJ | tblYDJfIh17xCCtIP |
| Live | appgzosUcVqA4vZdf | tbl1jEtJusopJGC6O |
| Data acquisition | appdl16HJ5ETm8N69 | tblrpt7ZqoUGr2VFj (existing) |
| Season trainers | appzZsd3UqMRR2CNc | tbluWs084sVApiWyZ |
| Season riders | appkWkm9rFLrkUvaD | tbl1yR7TBkb9qGKNs |
| Season horses | appETrjio9Yj9Pv17 | tbl6q1qxN2KgdbIaE |
| Season rings | appxMk9wfbBHEvC5e | tblZa00dMCVYFo46K |

The six missing tables were created for administrative review evidence only. Business table schemas, collector source, task/cadence, source services, business data and native links are outside this documentation change. Reviews are not stored as fake heartbeat/run-log collection successes.

## Active periodic reviews - later owner authorization, October 30

All four are ACTIVE thread heartbeat configurations, targeting their existing owning chat, with a 20-minute read-only review bound. Host timezone is Eastern Time (US & Canada). Times below are America/New_York local time, including DST. First scheduled reviews are November 2, 2026; setup/readback is not execution proof. These depend on host/app availability. Existing Data automation identity, target, status and Monday 09:00 timing were preserved; only current skills/instruction guidance was appended to its prompt. The other three were created only after confirming no matching review existed.

| Review | Monday time | Automation ID | Periodic record(s) |
| --- | --- | --- | --- |
| Data acquisition | 09:00 | wef-data-monday-code-review | rec3DITcZqf1uAP1L, existing Pending setup preserved |
| Shows | 09:30 | wef-shows-monday-code-review | recZTORMGcgsAaezQ |
| Live | 10:00 | wef-live-monday-code-review | recYUERW7D8RF2OXS |
| Season retention, all four bases | 10:30 | wef-season-monday-code-review | trainers recd56FCNbiq4Tumf; riders recPrdL0LPFAcDTwa; horses recTSbpVGaYTTMII2; rings recl01Hjtwk9gilnP |

The six new periodic records were GET-read back after activation metadata was appended; each remains Pending first scheduled review, next-review November 2. They are separate from the release review records below. Each future wake may update only its named review metadata, preserving dated history; no product fixes, deployments, source calls, collector invocations, task/cadence changes, repairs, migrations or sideways agent messages. Existing Data and Season reviews are separate responsibilities even though one chat owns both. Configuration does not authorize the review agent to resume unrelated work.

Records coordinates the initial writes/readback for this request. Module owners maintain their own review procedure and future authorized review entries. No business-task scheduling, Data/Season ownership split, Schedules retirement or reader migration follows from this standard.

## Initial review publication - October 30, 2026

All seven release review records were independently GET-read back and matched every saved field. Evidence: [readback manifest](evidence/wef-code-reviews-20261030/readback.json). The prior Data Monday setup record remains Pending for November 2, 09:00 America/New_York; no recurring job was created or changed.

| Scope | Verified Airtable record | Result and boundary |
| --- | --- | --- |
| wef-shows | [recjXg1esmui5DGOs](https://airtable.com/appCaSmL819KK7UFJ/tblYDJfIh17xCCtIP/recjXg1esmui5DGOs) | Review PASS; scheduled acceptance PASS; source freshness Stale |
| wef-live | [rec5fLj3xEqkYBpxA](https://airtable.com/appgzosUcVqA4vZdf/tbl1jEtJusopJGC6O/rec5fLj3xEqkYBpxA) | Review FAIL: 3 P2 defects; offline PASS; populated acceptance PARTIAL |
| wef-data | [receKH1C8dQIxPiDV](https://airtable.com/appdl16HJ5ETm8N69/tblrpt7ZqoUGr2VFj/receKH1C8dQIxPiDV) | PARTIAL: P2 outgoing-archive race; current-show scheduled proof PASS |
| wef-season-trainers | [recyQXvJZbYFCwwsJ](https://airtable.com/appzZsd3UqMRR2CNc/tbluWs084sVApiWyZ/recyQXvJZbYFCwwsJ) | PASS within Season scope; scheduled PASS; upstream/shared/rollover limits recorded |
| wef-season-riders | [rececydkDBe3pq0nO](https://airtable.com/appkWkm9rFLrkUvaD/tbl1yR7TBkb9qGKNs/rececydkDBe3pq0nO) | PASS within Season scope; scheduled PASS; upstream/shared/rollover limits recorded |
| wef-season-horses | [recjyJucOJKkhDKQv](https://airtable.com/appETrjio9Yj9Pv17/tbl6q1qxN2KgdbIaE/recjyJucOJKkhDKQv) | PASS within Season scope; scheduled PASS; upstream/shared/rollover limits recorded |
| wef-season-rings | [rec3CSKz1au3Xs6XZ](https://airtable.com/appxMk9wfbBHEvC5e/tblZa00dMCVYFo46K/rec3CSKz1au3Xs6XZ) | PASS within Season scope; scheduled PASS; upstream/shared/rollover limits recorded |

Live has three confirmed P2 implementation findings. Data has a confirmed P2 full-outgoing archive publication race, separate from accepted current-show recovery. Season has no direct actionable defect in the reviewed transfer/store/checkpoint paths; upstream Data risk, shared-dependency mismatch and unobserved natural different-show rollover remain explicit limitations. No review findings were patched under this request.
