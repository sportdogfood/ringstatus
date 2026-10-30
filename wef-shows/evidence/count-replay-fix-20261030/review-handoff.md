# Shows code-review report for Records

Module: **wef-shows**. Exact target base: **appCaSmL819KK7UFJ**.
Release: **count-replay-fix-20261030**. Locked scope: **v1.4**.
Review date: **2026-10-30, America/New_York**. Reuse verified at **09:06:58 Eastern**.

## Findings

**PASS — no actionable findings** in today's independent review of the approved
Shows-only correction. No severity/correction entries are fabricated when there
are no findings. The unchanged release requires no further implementation.

Independent reviewer: `/root/shows_review`, applying
`C:/Users/gombc/.agents/skills/code-review/SKILL.md`. Shows owner performed today's
hash/evidence refresh; this is reuse of that independent review, not a new
independent review or a duplicate test run.

## Baseline and exact version

Original proven source: `before/day-source.js`, `before/day-replay.py` and the
pre-change SHA256 inventory `baseline.json` in this directory. Scope: duplicate
retained-evidence decoding, bounded replay timeout, validated reads of existing
Data archive references, restoration of approved show-days publication. Review
covered chain/provenance/hash/identity/count validation, freshness, failure stops,
native publication links, archives and downstream boundaries.

Current runtime inventory: `deployed-manifest.json`, 19 files. Manifest SHA256:
`f61ab5d60867a87be6bb2fe21b83cc782e2f59f893003ed056de281fe3f2a3f4`.
`review-reuse-check.json` confirms all 19 runtime hashes match, the reviewed test
file is unchanged, and the scheduled-task definition is unchanged. No runtime
reopening, migration, schema or ownership changes are part of this report.

## Verification gates

| Gate | Result | Evidence and boundary |
|---|---|---|
| Independent code review | PASS | `code-review.md`; no actionable findings; exact unchanged reviewed scope. |
| Offline tests | PASS | Existing `deployed-tests.tap`: 48 passed. Independent reviewer ran 17 relevant tests. Inspected these results today; no tests rerun during this reuse check. |
| Deployment/version | PASS | Existing in-place release, `deployment.json`, `lock.json`, current `review-reuse-check.json`; no deployment performed today for this review request. |
| Normal scheduled acceptance | PASS | Actual count replay/publication runs below; independent retained-count/Airtable readback, stable native links, four verified archives each, zero errors and task exit 0. |
| Latest local runner evidence | PASS | Run `85c36b2e-3a8d-433e-8d5b-deba0e0092e4`, 09:04:03–09:05:25 Eastern: success, four days, no errors; task exit 0. Saved in reuse check. No new Airtable readback asserted for this latest run. |
| Fresh count readiness | BLOCKED | Published counts are Stale. Successful collection/publication or review does not renew source timestamps or prove fresh zero readiness. |
| Airtable review recording | PASS | Records confirmed exact GET readback in Shows base `appCaSmL819KK7UFJ`: `code-reviews` table `tblYDJfIh17xCCtIP`, release record `recjXg1esmui5DGOs`. Six fields match Data's schema; `wef-code-review-v1` notes distinguish code-review PASS and scheduled PASS from source Stale. Shows made no Airtable review/schema writes. |

Actual normal replay acceptance:

- `e899a0c3-b9a0-4ad4-90a4-51028448b408`: **08:44:04–08:45:51 Eastern**.
- `b2fbcfba-e35b-4f66-8b61-fbbe6b177fe1`: **08:49:04–08:50:01 Eastern**.

Their `normal-<run-id>.json` receipts are in this directory. Independent exact
count replay/readback receipts are in `../show-days-build-20261029/` with the same
filenames. The 08:39 Unknown/context-mismatch run is not used as replay acceptance.

## Risks and next authorized action

The historical timeout itself was not reproduced. Confirmed redundant decoding
was removed and bounded headroom increased; two scheduled successes are current
acceptance, not an unlimited future-capacity guarantee. Local archives and shared
code under `wef-schedules` remain required physical dependencies. No evidence or
freshness checks were weakened. Schooling/classless coverage remains unproved.

**Records confirmed review documentation complete and exact GET readback PASS:
`tblYDJfIh17xCCtIP` / `recjXg1esmui5DGOs`.** Shows remains locked;
no code fix, deployment, task/cadence change, manual collector/provider execution,
record repair or retirement work is authorized by this review. Future failures
require review and applicable explicit approval for any correction.
