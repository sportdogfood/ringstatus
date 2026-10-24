# CWF tenant slices — 24 October 2026

## Scope and baseline

Owner approved inactive roster collection with exclusion from the active team,
and continued privacy for personal follows. Normal scheduled runs only; no
manual collection, record repairs, test selections or test disabling of live rows.

Baseline commit: `28ddf2815d487dbb28144dde34452fdcb38a72d3`.
Existing implementation: `wef-tenants/collector.js`, `entries-consumer.js`,
`airtable.js`, `settings.js`; CWF already consumes the native wef-entries sync.
Baseline source copies, schema/records and task XML are saved alongside this file.
Existing unrelated working-tree changes were retained.

## Changes

- CWF normal runner evaluates the validated trainer collection against both
  horses and riders. Either `disabled` or `source-inactive` hides participation
  from the active team. It does not prevent collection, helper refresh or links.
- Only three local fields on `wef-entries.entries` are added and maintained:
  `active-team`, `team-status`, `team-checked-at`. Source fields remain read-only.
- Native view **Active team** filters `active-team` checked.
- Native **Active roster** views on horses and riders require `disabled`
  unchecked AND `source-inactive` unchecked. Original Grid views are unchanged.
- Heartbeat and run-logs report the active, hidden-inactive and other-hidden counts.
- Gate `WEF_TENANTS_TENANT_SLICES_ENABLED=true` enabled at 15:26:06 UTC.
  Existing task, five-minute interval and launch action are unchanged.

## Privacy boundary

The existing central CWF team sync is filtered to nonempty `trainer-ids`.
Its source-json, collections and source schedules links are excluded. Private-only
entries are not synced; ownership of overlapping personal follows is not exported.
These configuration facts were inspected in Airtable without editing the source view.
The CWF consumer validates this separation before processing records.

`main-user` and `user-2` remain placeholders. Current favorites count is zero;
their eight retained-day collections are complete with zero rows. This is valid
empty-collection proof, not live proof of nonempty personal follows. Separate
authenticated personal delivery is not implemented by these shared Airtable views.

## Verification

- Offline: 98 tests passed across tenant and entries suites. Tests cover both
  inactive flags on horses/riders, repeated runs, reactivation, preserving helpers
  and source status, no private ownership export, and valid empty collections.
- Preflight readback: all 76 horses retained; 67 in Active roster and 9 inactive.
  All 37 riders retained and active. Retired 11 entry records unchanged.
- First normal scheduled run: 11:27:02–11:29:24 America/New_York, Success.
  Run ID `5f7a7b9f-4ac2-4c3c-9d48-ddae7d1d7c1f`. Readback verifies 9 active
  entries; 76 horses retained/67 active; 37 riders retained/active; all nine
  inactive horses refreshed; helpers and retired entries unchanged; heartbeat
  and run-log slice summaries saved. Zero people endpoint fetches by CWF.
- Second normal scheduled run: 11:32:02–11:34:27 America/New_York, Success.
  Run ID `3b8ec095-cc0f-4568-8b9f-275d1738c11e`. Independent Airtable readback passed again,
  with the same 9 active entries, 76 retained/67 active horses and 37 active
  riders. All nine inactive horses refreshed again, helpers/retired entries
  preserved, heartbeat and run-log updated, zero duplicate people fetches.
- Acceptance: PASS for the deployed CWF slice and two ordinary scheduled runs.
  Nonempty private follows and inactive current-entry participation remain
  fixture-proven, as described below; no production records were manufactured.
- Live entry-level inactive exclusion is not exercised because none of the
  nine current entries has an inactive participant; fixture tests cover it.
- Scheduled task XML matches baseline exactly.

## Airtable views

- [Active team](https://airtable.com/appxKPBtdl2Rdq7CS/tbl7ychn5HELB3HTr/viwAqV0OQzzlI9qft)
- [Active horses](https://airtable.com/appxKPBtdl2Rdq7CS/tblxMpG75KL3Nmt1v/viwveStzE4tXrLGIT)
- [Active riders](https://airtable.com/appxKPBtdl2Rdq7CS/tblll5l7eG5wc27uP/viwDXz2D9KM04P8hK)

Read-only verifier: `wef-tenants/verify-tenant-slices.js`. It reads normal receipts,
native view membership, helper preservation, heartbeat and run-log evidence; it
never invokes a collector or writes Airtable records.
