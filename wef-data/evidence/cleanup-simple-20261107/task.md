# Simple cleanup replacement

Authority: owner explicitly requested a new simple lane, not a patch or reuse of the old cleanup function. Retire the old path when switching the existing launcher; do not run both. Existing deployment/review/Records-alert authority remains; no manual execution or new schedule.

Contract: calendar-day deletes completed logging; log-check from existing Records three-hour alerts deletes oldest 500 eligible when a logging table has 1,000+; new-show clears checked owned show tables and completed logs; new-show-day does nothing. Protect active/current and undelivered logs. Preserve references, cleanup history and pending logs. Delete only, no record backups or archives.

Only new cleanup-lane.js, cleanup launcher binding and removal of old cleanup-only hooks from entry.js/run.ps1. No acquisition, linker logic, provider, key, cadence, schema or package changes. cleanupV2 is the existing control and latest per-table outcome destination. Existing old cleanup state is read only for the already-completed event during cutover; no old cleanup function/settings/adapter imports. History contains counts/errors, never saved business records. Old implementation remains on disk deprecated and unreachable.

Acceptance: offline event/eligibility/threshold/continuation/repeat checks, independent read-only code review; exact launcher/source readback; next genuine Records alert proof separately. No manual business deletion, no fabricated alert and no replay of the completed November7 calendar event.

## Current preparation status

New lane built; eight offline checks pass. Independent review reproduced two initial candidate defects, now corrected: a newly pending local error must retain its parent, and a logging read failure must not stop unaffected new-show actions or hide the original error. Fresh local eligibility scan and per-logging failure handling cover these cases.

Independent actual-logger interleavings additionally proved: removing delivery acknowledgments can recreate deleted records, so replacement preserves acknowledgments. The existing reader still reports ENOENT when cleanup removes a completed file between enumeration and read. Exact minimal logger tolerance is proposed in logger-proposal.md and owner approval is pending. No logger, launcher, collector or linker integration file has been changed. No replacement deployment, alert execution or manual deletion occurred.

Owner approved the single ENOENT-only reader correction in this chat. Candidate now includes only that change to attempt-log.js. No other logger behavior is authorized.
