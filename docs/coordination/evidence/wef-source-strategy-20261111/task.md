# Full WEF acquisition review — November 11, 2026

Goal: review Shows, Live, schedules, classes, trips, order-of-go and results as one app; recommend the best useful-data return per invocation and realistic recovery from direct feeds, proxy/runner/tunnel, scraping, official files, CSV and human input.

Owner direction: iphone feeds first where they supply required data; keep protected schedule/class routes for missing fields or fallback; assess smaller specialized feeds for ongoing updates. Existing documented token handling stays the baseline. Do not reopen token semantics or introduce speculative infrastructure.

Scope: read existing code, current saved reports, actual Airtable schema/data examples and existing diagnostic evidence; bounded source diagnostics only when they close a named coverage gap. Save a concrete review and task/endpoint evaluation evidence. No production edits, deployment, credential or cadence changes, collector/manual workflows, data repairs or legacy task resumption. Preserve current keys and lane ownership; no unsupported importer or backup is called operational.

Evidence reused: sgl-stage1-20261111 (229-route inventory), sgl-auth-20261111 (43 requests / 40 findings and actual payload excerpts), existing lane release/readback reports, and current source readers. Existing 26 test-now selections are priorities, not a limit on relevant gap review.

Current: inspecting active consumers and required field mappings, existing request duplication and recovery/import capabilities. Next: same-class iphone/API comparison, populated day/export comparison if supported without new installation, then a seven-lane source/fallback matrix with checks and human responsibilities. All proposed changes remain unimplemented.


## Review delivered

Source plan: docs/coordination/wef-source-strategy-review-20261111.md. Seven-lane source/recovery matrix completed from existing code, saved reports and the same-class comparison. No further source tests. Forty opportunity records now have the requested four individual fields; readback matched. OPP-011 updated and read back. Identity coverage is UNCONFIRMED pending owner review; claims that identity IDs are absent were removed from the current report, individual observation and derived comparison labels. Captured payload values are preserved. No production, credentials, collectors, task restarts or business-record repairs. Next action is owner payload/identity review; implementation remains paused.
