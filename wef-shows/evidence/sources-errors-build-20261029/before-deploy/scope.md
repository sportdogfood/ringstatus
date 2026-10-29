# wef-shows scope

Version: 1.0
Date: 2026-10-29 (America/New_York)
Owner: wef-shows
Coordination: wef-records-coordinator

## Purpose

wef-shows owns the trusted show identity, dates and show-day information, tracks changes and evidence, and supplies schedules, entries, live and Season. It shares ownership, changes, verification and unresolved handoffs with Records coordinator.

Season supplies count evidence; Shows validates and publishes canonical show-days. These are independent responsibilities. Sharing information does not trigger another collector.

## Correction to the earlier description

The earlier description was too narrow. Five minutes describes the current implementation; it does not explain the intended discovery policy. RingStatus Operating Skills had not been loaded before that answer. Its Instruction Loader and Rebalancer were subsequently read and applied during the correction.

The later Shows README describes five-minute discovery. That conflicts with the October 15 discovery decision below and needs reconciliation. The conflict should have been surfaced rather than presenting the running cadence as the intended policy.

## Documented discovery policy

Source: [October 15 owner clarification](../docs/coordination/wef-schedules-only-activation-plan-20261015.md), beginning at line 5.

- New default show: expected Sunday, 4-8 p.m. Eastern, following daylight saving. This is an owner estimate, not a guaranteed source-change time.
- Record the change, then stop that discovery polling cycle until discovery logic calls again. The interval within the Sunday window remains unspecified.
- An optional six-hour background safeguard was proposed, not finalized or enabled by that decision.
- Schedule availability: separately expected 4-8 p.m. Eastern the day before the show starts. This is not a discovery-triggered collection instruction.
- Discovery does not trigger downstream collectors. The original clarification also excludes discovery-triggered focus changes, processing, publication or other business action; its relationship to later approved canonical publication must be reconciled explicitly.

The current five-minute task remains unchanged. This document does not select a new cadence, resolve the conflict by assumption or authorize runtime changes.

## Ownership and handoffs

| Owner | Responsibility |
|---|---|
| wef-shows | Canonical show/customer identity, show dates, source-selected date, show-days, validated publication, discovery health, retained change evidence and its own run logs. |
| wef-season-coordinator | Data acquisition and retained count evidence, including scope, source observation time, completeness and identities; separate Season history responsibilities. |
| wef-schedules, wef-entries, wef-live | Read canonical show context independently and own their respective collection, linking and consumer acceptance. |
| wef-records-coordinator | Record accepted scope, changes, verification, dependencies and handoffs; coordinate unresolved ownership and policy conflicts. |

Shows does not own tenant selections, CWF helpers, downstream execution, timing calculations or whole-show enumeration by assumption. The source-selected date is not automatically TODAY.

## Current implementation and verification

Canonical base: `appCaSmL819KK7UFJ`.

- `shows`: `tblJA5CUon5s1sWib`.
- `show-days`: `tbl14MaCCDw6dAywH`, linked locally to `shows` with a reciprocal `show-days` field.
- Count publication includes counts, coverage, original source time, accepted evidence and publication status. Missing or incomplete information must not become a verified zero. Competition-only coverage does not establish schooling attendance or whole-day emptiness.
- Five-minute zero validity is measured from source observation time. It is separate from discovery cadence and does not authorize a Live suppression gate.

Historical correction: the supplied review text said, "That addition remains unbuilt. No changes made during this review." That accurately described the earlier read-only review, not the subsequent release. Canonical show-days publication was deployed on October 29 at 10:01:45 a.m. Eastern and verified through two normal scheduled Shows runs, starting at 10:04 and 10:09 a.m., consuming two distinct Data revisions. Four day IDs and their native show links remained stable. No manual collector runs or record repairs were used.

Evidence: [release record](evidence/show-days-build-20261029/release.md) and [two normal verification pairs](evidence/show-days-build-20261029/two-normal-pairs.json).

## Open items and boundaries

- Records must coordinate reconciliation of the October 15 discovery window/stop rule with the later five-minute implementation and canonical publication responsibilities. The six-hour safeguard and within-window interval remain unsettled.
- Live count consumption or suppression requires its own approved implementation and verification; Shows publication alone does not activate it.
- At the Shows release checkpoint, the coordinator reported a separate CWF run termination. Shows verification is not a claim of complete downstream acceptance; Records owns tracking that separate item.
- The existing local archive handoff remains an implementation dependency; this document does not migrate it.

This version documents scope and evidence only. It makes no code, Airtable, task, cadence or workflow changes.
