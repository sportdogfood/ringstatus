# Live tenant-code removal — 29 October 2026

Approved scope: remove Live tenant-code dependencies and outdated wording; preserve behavior/links, verify two normal scheduled runs. No manual runs, record repairs, upstream code/schema changes or task changes.

## Implementation

- Added local runtime-utils.js for same, unique and acquireLock; local archive.js for archiveFile.
- Switched airtable.js, entry.js, collector.js and verify.js imports away from wef-tenants.
- Preserved exact utility source after line-ending normalization; only lock error wording changed from tenant to Live.
- Updated unmatched-entry message and README wording. Historical evidence is retained.
- Scheduled task already targets this checkout via hidden WScript; three-minute cadence unchanged. No separate deployment route or trigger was used.
- scope.md and HANDOFF.md version 1.2 document acceptance.

## Offline verification

49 existing tests passed; three added tests passed: modules load with tenant imports prohibited; blank/uniqueness/lock semantics; archive reuse and corruption rejection. No collector invocation.
Static comparison initially differed solely because of CRLF/LF line endings; normalized comparison confirms archive and utility equivalence. See tenant-decoupling-equivalence-20261029.json.
Source hashes: tenant-decoupling-deployed-20261029.json.

## Normal scheduled verification (America/New_York)

| Run | Started | Completed | Result |
|---|---|---|---|
| 50e91fc4-a75f-4537-9a67-652f2ac78d73 | 13:11:16 | 13:12:03 | Success |
| 7796f86c-95c7-49e1-af31-84a775daf986 | 13:14:14 | 13:15:17 | Success |

Both receipts reported no live activity. For each, Airtable heartbeat and run-logs had exactly one matching run ID, Success and matching completion timestamp. No errs-log rows or receipt collection/link/logging errors. All receipt archive hashes verified. Task completed with result 0.

Before/after comparison preserved every record ID and every field, including links: 5 rings, 31 groups, 56 classes, 305 entries (397 total). Baseline tenant-decoupling-before-20261029.json; per-run proof tenant-decoupling-run-<run-id>.json.

These runs prove scheduled invocation, source/archive path, logging and preservation under today's empty activity. They do not claim fresh populated activity, multi-group mapping or old-show deletion proof. Those existing gaps remain separate. No CWF dependency remains in Live runtime imports; upstream Entries sync scope is a separate owner's verification.
