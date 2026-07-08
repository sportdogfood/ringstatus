# WEC Clean Cadence Repair Log

Date: 2026-07-07 ET / 2026-07-08 focus day

Version: v0.1

Status: repair documented; use as the working evidence log for the July 8 clean cadence handoff.

## Purpose

This document records what happened during the 2026-07-08 focus-day change, what broke, what was fixed, and what must remain repeatable.

This is not a new workflow design. It documents repairs to the clean cadence workflow that already owns:

```text
heartbeat
-> build/update schedule
-> probe/process entries
-> enrich runtime
-> calculate field vars
-> create message/output trigger records
```

The main point: the fixes must not be one-time row wrangling. The next focus-day change must follow the same clean cadence path and produce the same accounting.

## Active Focus

Current active focus during this repair:

| Field | Value |
|---|---|
| show_no | 14910 |
| focus_day | 2026-07-08 |
| focus_day_key | 20260708 |
| cadence target | `wec_stage1_3_clean_proof/?action=wec-clean-cadence-stack` |
| scheduler | `wec_heartbeat_day_every_6_min` |
| environment | Catalyst Development |

## Expected Clean Stage Shape

| Stage | Source | Destination | Expected behavior |
|---|---|---|---|
| Heartbeat | Airtable `focus_show` | Catalyst `hs_heartbeat`, mirror visibility | Resolve active show/day dynamically. |
| Build ring days | HorseShowing `get_ring_days.php` | `hs_get_ring_days` | Current active focus day only. |
| Build/update schedule | HorseShowing `update_schedule.php` | `hs_update_schedule` | Current focus day, non-preflight class rows remain eligible. |
| 3A probe | `hs_update_schedule` class rows | `hs_class_oog_raw` accounting/docs | One checked class gets one raw/accounting row. |
| 3B parse | `hs_class_oog_raw` rows with stored raw HTML | `hs_class_oog` | Parse only stored raw docs; do not parse accounting-only rows. |
| Step 4 runtime | `hs_update_schedule`, `hs_class_oog` | `hs_ring_status`, `hs_class_start_times`, `hs_entry_go_times` | Current-day runtime prep from clean const keys. |
| Mirror | Catalyst `hs_*` | Airtable `hs_*` | Airtable is visibility/review mirror, not source of runtime truth. |

## What Was Observed

The user observed Airtable current rows:

| Table | Observed |
|---|---:|
| hs_get_ring_days | 9 |
| hs_update_schedule non-preflight | 51 |
| hs_class_oog_raw | 7 before accounting fix |
| hs_class_oog | stale / no reliable 2026-07-08 mirror at first |
| hs_ring_status | stale / no reliable 2026-07-08 mirror at first |
| hs_class_start_times | stale / no reliable 2026-07-08 mirror at first |
| hs_entry_go_times | stale / no reliable 2026-07-08 mirror at first |

Catalyst showed the clean cadence had current-day state:

| Table | Catalyst current state |
|---|---:|
| hs_get_ring_days | 9 |
| hs_update_schedule non-preflight | 51 |
| hs_class_oog_raw | 7 raw docs before accounting fix |
| hs_class_oog | 15 matched rows |
| hs_ring_status | 9 |
| hs_class_start_times | 51 |
| hs_entry_go_times | 15 |

The first important clarification:

```text
51 schedule classes
- 7 had allowed-trainer evidence and stored raw class_oog payloads
- 44 were checked and had no allowed-trainer evidence
```

The 44 were not failures and were not "passed." They were checked/no-evidence classes.

## Bugs Found

### 1. `hs_class_oog_raw` did not account for checked/no-evidence classes

Before the fix, 3A only created `hs_class_oog_raw` rows when raw HTML was stored.

That made `hs_class_oog_raw=7`, even though 51 non-preflight classes had been checked.

This made the accounting unclear:

```text
7 raw docs existed
44 checked/no-evidence classes were visible only indirectly through hs_update_schedule probe fields
```

Required behavior is now:

```text
Every checked non-preflight class gets one hs_class_oog_raw row.
If evidence exists:
  raw_stored = true
  raw_html populated
  parsed_status = pending/parsed
If no evidence exists:
  raw_stored = false
  raw_html blank
  parsed_status = not_applicable
  probe_reason explains why
```

This gives complete class-level accounting without forcing broad nonmatching entry materialization.

### 2. Airtable Step 4 mirror target skipped `hs_class_oog`

The clean cadence mirror state originally checked runtime tables:

```text
hs_ring_status
hs_class_start_times
hs_entry_go_times
```

It did not include `hs_class_oog`.

That allowed Step 4 to report runtime complete while Airtable still lacked current `hs_class_oog` mirror rows.

The mirror target order was corrected to include:

```text
hs_class_oog
hs_ring_status
hs_class_start_times
hs_entry_go_times
```

This matters because `hs_class_oog` is the parsed entry source feeding downstream entry/runtime tables.

### 3. Mirror-first cadence branch was needed before rebuilding runtime

The cadence was returning runtime completion while mirror work still remained.

The clean cadence was corrected so that if current Step 4/runtime rows already exist, the cadence first checks Airtable mirror state and mirrors the next incomplete table before continuing.

This prevents repeated "runtime complete" heartbeats while Airtable remains stale.

### 4. Horse parser rejected horse names containing digits

Two parsed entries were wrong:

```text
14910|20260708|4043|709|31361|1044
source_payload = 35 | 1044 | Peridoni 20 | Elisabeth Rotsaert | Alan Korotkin
wrong horse = Elisabeth Rotsaert

14910|20260708|4043|709|33332|138
source_payload = 8 | 138 | Cardozo 4 | Tanner Korotkin | Alan Korotkin
wrong horse = Tanner Korotkin
```

Cause:

The parser tried to infer horse by finding the first text cell without digits.

That failed for horse names containing digits:

```text
Peridoni 20
Cardozo 4
```

Fix:

The parser now uses the class_oog column positions:

```text
entry_order = column 0
entry_no    = column 1
horse       = column 2
rider       = second-to-last column
trainer     = last column
```

This is repeatable because it follows the source table structure instead of guessing from text shape.

### 5. `hs_heartbeat` payload had more useful status than visible Airtable columns

The useful stop-state fields were inside `payload_json`, including:

```text
stop_stage
stop_reason
next_stage
step4
step4_mirror
step4_mirror_state
raw_accounting
```

This explains why visual Airtable inspection felt contradictory. The heartbeat row existed, but the critical state was not always visible as direct Airtable columns.

This should be treated as a dashboard limitation, not proof that cadence did not run.

## Fixes Applied

Code changed in:

```text
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\catalyst-workspaces\horseshowing\functions\wec_stage1_3_clean_proof\index.js
C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\catalyst-workspaces\horseshowing\functions\wec_stage1_3_clean_proof\handler.js
```

### `index.js`

Changed 3A behavior so every checked class writes an `hs_class_oog_raw` accounting row.

Evidence classes:

```text
raw_html populated
raw_stored = true
parse_status = pending
parsed_status = pending
```

No-evidence classes:

```text
raw_html blank
raw_stored = false
parse_status = not_applicable
parsed_status = not_applicable
probe_reason = no_allowed_trainer_evidence
```

### `handler.js`

Added current-day backfill/accounting behavior so already-checked schedule rows also get `hs_class_oog_raw` rows without re-probing HorseShowing.

Added heartbeat visibility for:

```text
raw_accounting
raw_accounting_mirror
step4_mirror
step4_mirror_state
```

Corrected Step 4 mirror target state so `hs_class_oog` is included before downstream runtime mirrors.

Corrected class_oog parser to read the horse field by source column position.

## Scheduler Evidence

The existing clean scheduler was used:

```text
wec_heartbeat_day_every_6_min
```

No new cron was created.

Observed scheduler-owned runs:

| Job ID | Result | Meaning |
|---|---|---|
| 5614000000755379 | HTTP 200 | Mirror-first path ran and mirrored `hs_class_oog` 10 of 15. |
| 5614000000757287 | HTTP 200 | Latest heartbeat returned Step 4 runtime complete after mirror state improved. |
| 5614000000755381 | HTTP 408 | Expanded raw-accounting mirror exposed a timeout risk. Catalyst accounting rows were created, but Airtable mirror proof was not clean in that run. |

Important:

The HTTP 408 run does not invalidate the Catalyst accounting fix. It means Airtable mirroring needs to remain bounded/paged so 51 accounting rows do not exceed the webhook execution window.

## Current Accounting Meaning

For `14910 / 2026-07-08`, the intended accounting is:

| Count | Meaning |
|---:|---|
| 51 | Non-preflight schedule classes checked or eligible for 3A. |
| 7 | Classes with allowed-trainer evidence and stored raw class_oog payloads. |
| 44 | Classes checked with no allowed-trainer evidence. |
| 15 | Matched entry rows materialized into `hs_class_oog`. |

The repeatable rule:

```text
hs_class_oog_raw should account for all 51 classes.
Only raw_stored=true rows are parsed by 3B.
raw_stored=false rows are accounting rows only.
```

## Repeatability Rules

These rules prevent the same failures from recurring.

### 1. Focus source remains Airtable `focus_show`

`focus_show` is the human control source.

`hs_focus_show` is a Catalyst mirror/reference, not a replacement control table.

### 2. Cadence must use the existing clean scheduler path

Manual endpoint runs do not count as workflow proof unless explicitly approved.

The clean cadence path is:

```text
wec_heartbeat_day_every_6_min
-> wec_stage1_3_clean_proof/?action=wec-clean-cadence-stack
```

### 3. Every class checked by 3A must leave an accounting row

This is the new invariant:

```text
hs_update_schedule non-preflight count
should match
hs_class_oog_raw accounting count
```

This does not mean every class has raw HTML.

### 4. 3B only parses stored raw docs

Rows with:

```text
raw_stored=false
parsed_status=not_applicable
```

must not block 3B.

Only stored payload rows should be parsed and materialized.

### 5. Parser must use source structure, not text guessing

Horse names may contain numbers or punctuation.

Do not infer horse/rider/trainer by regex text shape. Use the source column positions from class_oog.

### 6. Airtable mirrors are visibility, not runtime truth

Catalyst remains canonical for clean `hs_*` runtime state.

Airtable mirror gaps should be treated as mirror gaps unless Catalyst also lacks the rows.

### 7. Airtable mirror work must be bounded

The raw-accounting change increases mirror volume.

The mirror path must remain page/batch bounded so the cadence job does not timeout while trying to mirror all rows at once.

## What Is Proven By This Repair

| Item | Status |
|---|---|
| Active focus resolved to `14910 / 2026-07-08` | Proven |
| Scheduler path is clean cadence path | Proven |
| `hs_get_ring_days` current-day build | Proven |
| `hs_update_schedule` current-day build | Proven |
| 3A can distinguish stored raw vs no-evidence accounting | Proven in Catalyst after fix |
| `hs_class_oog_raw` can carry no-evidence accounting rows | Proven in Catalyst after fix |
| `hs_class_oog` parser no longer rejects horses with digits | Code fixed and deployed |
| Step 4 runtime counts exist for current focus day | Proven |
| Airtable current mirror can lag Catalyst | Proven |
| Airtable raw-accounting mirror is fully timeout-safe | Not proven in the 408 run; needs bounded mirror follow-up |

## Open Follow-Up

The one remaining implementation issue from this repair is Airtable mirror pacing for `hs_class_oog_raw`.

Required behavior:

```text
Mirror hs_class_oog_raw in bounded pages.
Do not try to mirror all accounting rows inside one scheduler execution.
Heartbeat should clearly report:
  raw_accounting_total
  raw_accounting_mirrored
  raw_accounting_remaining
```

This is a mirror throughput issue, not a source/probe/parse logic issue.

## Operator Notes

When visually checking the day:

1. Start with Catalyst as canonical.
2. Confirm `hs_update_schedule` non-preflight count.
3. Confirm `hs_class_oog_raw` accounting count.
4. Split raw rows by:

```text
raw_stored=true
raw_stored=false
```

5. Confirm only `raw_stored=true` rows feed 3B.
6. Confirm `hs_class_oog` materialized rows are only matched entries.
7. Confirm Step 4 runtime rows are current focus day.
8. Then inspect Airtable mirrors.

If Airtable is stale but Catalyst is correct, fix mirror pacing. Do not reinterpret the source workflow as failed until Catalyst is also wrong.

