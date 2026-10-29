# Live Entries-consumer simplification
Version: 1.0
Date: 29 October 2026 (America/New_York)

## Approved and delivered

Added exactly five fields to existing Entries view wef-live-reference and Live's existing read-only native sync: schedule-link-status, schedule-link-basis, group-link-status, link-issues, source-json. Kept the twelve existing selected fields, automatic sync and read-only setting. Used the existing Airtable UI; did not press Sync now or trigger a collector. No business record repairs, no new table, no other collector code or schedule changes.

Live's participation resolver now consumes explicit producer linking status and evidence. Removed source schedules/show-collection reads, full dated source/sync replay and schema discovery from collector.js. Kept local exact identity, active status, original source freshness, referenced-key availability/uniqueness, source scope/revision consistency and prior-link preservation. Ordinary class-date association remains distinct from observed-group eligibility. SourceAsOf and last-seen must represent the same instant and meet the existing 30-minute freshness limit (existing one-minute future tolerance retained). Newer applicable attempts or missing evidence prevent current qualification. Group-based associations additionally require explicit linked group and same-revision evidence.

No changes to source endpoints, live observations, class/group schedule links, archival/retirement safeguards, heartbeat/run-logs/errs-log or three-minute cadence. Stale/unresolved links do not stop valid observations. Their prior links are retained as historical and issues are logged.

## Evidence

- tests.txt: 54 tests passed. Includes current unique association without group proof, original-source freshness boundary and publication mismatch, source failures, missing/duplicate local references, group evidence, no upstream replay/schema reads, and repeated stale-source collection preserving observations/links/logs.
- before/: saved source/test baseline. before-records.json: pre-change records and links.
- deployed.json: unchanged collector/resolver hashes checked after tests.
- sync-readback.json: 127 synced records; all five fields present; 114 with new evidence/status, 113 populated schedule-link-basis, 127 source-json values including historical formats. Unsupported historical formats remain unqualified.
- sync-fields.png: visible destination field verification.

## Two normal scheduled runs

| Run | Started Eastern | Completed Eastern | Result |
|---|---|---|---|
| 5836813d-2ea6-4c87-a939-3338af39729c | 13:50:13 | 13:50:36 | Success |
| 2ddf2667-0617-47b0-bab3-4eab37bde314 | 13:53:12 | 13:53:32 | Success |

For both: matching single heartbeat/run-log rows with matching completion time; no receipt errors/link errors/logging errors or errs-log records; verified archive hashes; task result 0. Per-run files run-<id>.json contain exact readback.

Every field and record ID stayed unchanged across 5 rings, 31 groups, 56 classes and 305 live entries (397 records), including all native links.

Both normal runs had valid empty live activity. This proves cadence, independent acquisition/empty handling, archives, logging and preservation; it does NOT constitute natural populated participation/group acceptance. Populated fresh/stale/failure behavior is covered offline. No fresh source was manufactured, old records repaired, or manual business process run.
