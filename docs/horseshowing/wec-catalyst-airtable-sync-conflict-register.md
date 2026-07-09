# WEC Catalyst/Airtable Sync Conflict Register

Status: draft sync-owner register
Date: 2026-07-08

This document consolidates the current WEC sync ownership rules and known conflict classes between Catalyst and Airtable.

It is a documentation and review artifact. It is not runner proof, does not repair records, and does not approve manual/direct commands as workflow proof.

## Scope

The sync owner is responsible for:

- Keeping Core 1-4 Catalyst tables reliably mirrored one-way into Airtable.
- Defining which downstream fields are allowed to sync two-way between Catalyst and Airtable.
- Finding and reporting conflicts before a sync lane writes partial or misleading state.
- Returning explicit success/failure status back to Airtable for allowed sync actions.
- Preserving lane ownership: sync may report downstream conflict, but must not patch downstream business logic without approval.

## Existing Contract Sources

This register depends on these existing docs:

- `docs/horseshowing/wec-clean-stage1-4-workflow-contract.md`
- `docs/horseshowing/wec-workflow-contract-v0.2-2026-06-15.md`
- `docs/horseshowing/wec-airtable-two-way-edit-contract.md`
- `docs/horseshowing/wec-workflow-log-contract.md`
- `docs/horseshowing/wec-systems-scope-contract-v0.1-2026-06-15.md`
- `docs/horseshowing/wec-focus-workflow-v0.1-2026-06-10.md`
- `docs/codex_runner_integrations_handoff_2026-06-19.md`

## Ownership Model

### Core One-Way Sync

Core source of truth is Catalyst.

Direction:

```text
Catalyst core table -> Airtable mirror table
```

Core sync must not let Airtable mirror rows override Catalyst source rows.

Core one-way tables include:

- `hs_get_ring_days`
- `hs_update_schedule`
- `hs_class_oog_raw`
- `hs_class_oog`
- `hs_ring_status`
- `hs_class_start_times`
- `hs_entry_go_times`

Expected proof for each table:

- Catalyst write completed.
- Catalyst readback by stable key completed.
- Airtable mirror upsert completed.
- Airtable mirror readback by stable key completed.
- Missing/mismatch counts are reported.
- Failure includes table, key, field, direction, and exact stop reason.

### Downstream Two-Way Sync

Downstream sync is only allowed for explicitly approved fields.

Direction:

```text
Catalyst downstream table <-> Airtable allowed fields
```

This is not blanket two-way sync. Airtable may update only fields named in an allowed-field manifest. Catalyst remains responsible for validating, applying, and returning status.

Downstream tables and lanes may include:

- `time_engine`
- `time_engine_logs`
- result queue/classes/results tables
- alerts log/send state
- publish payload/cache state
- approved helper/manual-control tables

Downstream two-way sync must return status to Airtable:

- `PASS`
- `PASS_WITH_WARNINGS`
- `FAIL`
- `SKIPPED`

The status must include row key, table, allowed field list used, changed fields, rejected fields, and error message when applicable.

## Primary Conflict: Field Mismatch

The repeated failure pattern is field mismatch/schema drift.

Known forms:

- Airtable field renamed but runner still writes old name.
- Airtable field id/name map changed but code still uses stale map.
- Catalyst column exists but Airtable mirror field is missing.
- Airtable field exists but has incompatible type.
- Required writable mirror/run field missing for a formula-intended key.
- Writer attempts to write a formula, lookup, rollup, linked display, or computed field.
- Numeric values such as `show_no`, `class_no`, `ring_no`, `entry_no`, or `entry_order` are written as text when the contract expects numbers.
- Airtable checkbox false is omitted by the API and is misread as missing schema.
- Status fields with similar names are mixed across tables.
- Link-back fields are missing, stale, or treated as proof when only display text matched.
- Allowed-field manifest is absent or not checked before PATCH.

Field mismatch must be treated as a hard sync conflict, not as a soft warning, when it affects identity, keys, ownership, required handoff fields, or allowed write fields.

## Other Conflict Classes

### Source-Of-Truth Inversion

Conflict:

```text
Airtable mirror row is treated as current truth while Catalyst has not been reconciled.
```

Required behavior:

- Stop the sync lane.
- Report Catalyst key state and Airtable mirror key state.
- Do not let mirror rows mutate Catalyst core identity.

### Split-Brain Success

Conflict:

```text
Catalyst write succeeds, Airtable mirror/status write fails, but the workflow reports success.
```

Required behavior:

- Core business state may be valid in Catalyst.
- Sync status must still report mirror/status failure.
- Airtable must not show a successful sync if its mirror/status write failed.

### Key-Shape Drift

Conflict:

```text
Different lanes construct different keys for the same row identity.
```

Examples:

- `show_no`
- `focus_day`
- `ring_day_no`
- `class_no`
- `entry_no`
- composite mirror keys such as schedule/class/order/runtime keys

Required behavior:

- One documented key per table.
- No fallback key construction inside a writer.
- No direct writes to formula-intended key fields.
- Writable mirror/run key fields must be separate from formula keys where Airtable needs formulas for audit/display.

### Focus-Day Drift

Conflict:

```text
New focus day or new show changes source scope while stale rows from old scope still look active.
```

Required behavior:

- Active `show_no` and `focus_day` are checked before sync.
- Old-day fallback is not accepted.
- Cleanup must be scoped to the active show/day keys only.
- Stale rows are marked as stale or inactive; source identity is not rewritten.

### Partial Batch Drift

Conflict:

```text
Only part of a Catalyst page/batch is mirrored to Airtable, but the lane reports the batch as complete.
```

Required behavior:

- Track attempted, written, skipped, failed, and readback counts.
- Preserve failed keys and fields.
- Return `PASS_WITH_WARNINGS` or `FAIL` according to required-field impact.

### Helper Identity Drift

Conflict:

```text
Airtable helper/manual-control rows lose their `rec_id`, Catalyst `ROWID`, or approved identity bridge.
```

Required behavior:

- Airtable can remain a human edit surface for approved helper fields.
- Helper edits must not directly mutate Core 1-4 source/runtime identity.
- Sync must preserve Airtable record id and Catalyst row id where both exist.

### Status Drift

Conflict:

```text
Runner/cadence status, workflow_log status, heartbeat status, and Airtable sync status disagree.
```

Required behavior:

- Status row must name the lane, stage, table, key, direction, and blocker.
- Counts alone are not proof.
- Manual/direct endpoint diagnostics are not cron/workflow proof unless explicitly accepted.

### Downstream Mutation Of Core Identity

Conflict:

```text
Live, time-engine, results, alerts, or publish mutates Core 1-4 identity/source rows.
```

Required behavior:

- Downstream lanes may consume Core outputs.
- Downstream lanes may write their own status/enrichment/result/publish tables.
- Downstream lanes must not mutate Core source/runtime identity unless a separate approved contract says so.

## Sync Preflight Requirement

Every sync lane needs a preflight before writes:

1. Load live Catalyst table schema.
2. Load live Airtable table schema.
3. Load the table's sync manifest.
4. Compare required source fields.
5. Compare required destination fields.
6. Compare allowed write fields.
7. Confirm key field and type.
8. Confirm protected fields are not in the write payload.
9. Stop with `FAIL` before writing if required identity, key, required handoff, or allowed-write fields are missing.

## Sync Manifest Requirement

Each synced table needs a manifest entry:

| Field | Meaning |
|---|---|
| `lane` | Core, live-enrich, time-engine, results, alerts, or publish |
| `table` | Logical table name |
| `catalyst_table` | Catalyst physical/API table |
| `airtable_table` | Airtable physical/API table |
| `direction` | `catalyst_to_airtable`, `airtable_to_catalyst`, or `two_way_allowed_fields` |
| `key_field` | Stable sync key |
| `required_source_fields` | Fields required before read/write |
| `required_destination_fields` | Fields required before write/readback |
| `allowed_write_fields` | Fields that may be patched |
| `protected_fields` | Fields that must never be patched |
| `type_rules` | Numeric/text/date/checkbox handling |
| `readback_required` | Whether readback by key is required |
| `status_target` | Airtable/Catalyst status table or field |

## PASS/FAIL Rules

PASS requires:

- Required live schemas match the manifest.
- Source read succeeds.
- Destination write succeeds.
- Readback by key succeeds.
- Required fields are not missing.
- Mismatch count is zero for required identity/handoff fields.
- Status is returned to Airtable when the sync is Airtable-visible.

PASS_WITH_WARNINGS is allowed only when:

- Core identity and handoff fields are correct.
- Optional fields are missing or stale.
- The warning names the affected table, key, and field.

FAIL is required when:

- Required field is missing.
- Protected field would be written.
- Key field is missing or mismatched.
- Required readback fails.
- Airtable mirror/status write fails for an Airtable-visible sync.
- Catalyst and Airtable disagree on source ownership.
- A lane tries to use manual/direct proof as workflow proof without approval.

## Current Open Work

1. Build or locate the live sync manifest for each Core 1-4 table.
2. Build or locate the downstream allowed-field manifest.
3. Add a schema-diff report that names missing, extra, type-mismatched, protected-write, and allowed-write conflicts.
4. Add explicit split-brain reporting for Catalyst success plus Airtable mirror/status failure.
5. Confirm every sync lane writes Airtable-visible status after allowed two-way actions.
6. Reconcile existing docs so `field mismatch` is treated as a first-class failure category, not an incidental Airtable error.

## Sync Owner Rule

When sync fails, do not patch the business lane first.

First answer:

```text
Which table?
Which direction?
Which key?
Which field?
Which schema/manifest rule failed?
Did Catalyst write?
Did Airtable write?
Did readback pass?
Was status returned to Airtable?
```

Only after those answers are known should a code change be proposed.
