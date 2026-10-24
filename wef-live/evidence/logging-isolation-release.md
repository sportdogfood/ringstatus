# Logging isolation - October 24, 2026

Owner approved: logging must not interrupt wef-live collection; preserve data and
links; verify two normal scheduled runs; no manual runs or record repairs.

Baseline failure: c06ff203-e64b-402e-88ae-6588a65f5b84 timed out while auditing
an archived dataset. Every archive previously caused an Airtable run-log upsert.

Narrow change in collector.js: no initial or per-archive Airtable run-log writes.
Archive receipts accumulate in the report; one final run-log write includes them
all. Final run-log and heartbeat writes are independently best effort. Failures
are returned in logging_errors, not collection errors. Source archive failures
still protect against unsafe replacement/deletion; this is not optional logging.

entry.js retains local receipts and errs-log JSONL as best effort, then attempts
Airtable errs-log. Logging failures do not change collection success or prevent
another invocation. When disk is writable, final receipts retain logging failures;
stdout also includes the report. No background replay queue or repair process added.

34 offline tests passed: run-log outage, heartbeat outage, errs-log outage, local
write failure, subsequent invocation, identity/data/link preservation, skips and
archive protections. Only collector.js, entry.js and their two test files changed.
Independent read-only review found no blocking findings. Reviewer inspected code
and tests without running production requests or independently executing tests.

Two post-change normal scheduled runs verified in receipts and Airtable:

- 20cead9e-a6cb-4525-bba8-d3890ac5aad4: 11:32:13-11:33:01 ET, 48 seconds,
  279 entries, 2 unresolved classes. Partial in run-logs/heartbeat; both issues
  in errs-log. Two affected class records and 18 entry records/links unchanged.
- a3770808-eb9a-427c-918a-2d5bec7131c7: 11:37:13-11:38:04 ET, 52 seconds,
  292 entries, 1 unresolved class. Partial in run-logs/heartbeat; issue in errs-log.
  Affected class record/links unchanged; no affected existing entries. Class
  200030330 recovered naturally to complete. Class 200030429 remains unresolved.

Both had no operational or logging errors. Natural identity-error continuation
was exercised. Remote logging outage/local fallback remain offline-tested only;
no outage was manufactured. Task enabled with exit 0 and next 11:42 check.
Readbacks: logging-isolation-run-1.json and logging-isolation-run-2.json.
Task configuration unchanged. No manual runs or record repairs.
