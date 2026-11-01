# Current delivery - November 1, 2026, 09:54 Eastern

DEPLOYED and verified on two populated normal scheduled invocations at 09:51 and 09:54 Eastern. The owner's subsequent errs-log-only approval superseded the original chat-alert deployment hold below. All three lanes finish independently; current collection acceptance passes with six unresolved source trip identities explicitly retained as Partial source completeness. No stage/logging failures, manual runs, provider diagnostics, record repairs or observers. Rollback was not needed. Stage 8 and stage 9 remain held and are absent from the installed entry path.

Evidence: deployed.json, installed-readback.json (21 file hashes), task-readback.json (original hidden action/account/settings retained), normal-run-acceptance.json (six populated lane run IDs, receipt hashes, counts, timings and unchanged-clock proof), populated-airtable-readback-1.json and -2.json (heartbeat/run/error readback), populated-diff-readback-1.json (65 readable diff rows), business-readback.json (four business-table samples). Offline author checks: 18 PASS, offline-tests.json. Author review is not represented as independent review. No forced whole-stage failure or future rollover proof is claimed.

Windows trigger is daily local 05:00, three-minute repetition for 14 hours; America/New_York. Latest task result 0 and next normal trigger 09:57 confirm both populated invocations finished without overrun. Source/save timings refer to source requests and business saves separately; core total excludes final logging, so it is not called full task duration. Records-chat failure delivery remains deferred, not silently implemented. Prior failure/retention stops and archives remain unchanged; this release does not fix or resume the held publisher/retention lanes.

The preparation history below is retained; superseded Prepared/BLOCKED statements are not current deployment status.

# Three-lane collection candidate — November 1, 2026

**Prepared; not deployed.** Exact approved delta and baseline: TASK.md, baseline.json and baseline/. Four deployment files are identified by candidate-hashes.json; their prepared bytes live in candidate/. Unchanged dependencies are copies solely for isolated offline checks, not additional deployment files. 18 offline checks PASS (offline-tests.json); author review and limits: review.md.

Existing target/route: appgzosUcVqA4vZdf, wef-live-collector, existing hidden wscript/run-hidden.vbs -> run.ps1 -> entry.js in this checkout. No parallel collector/task is created. On release, the existing trigger must become daily 05:00 with three-minute repetition through the 19:00 Eastern window, preserving action/account/window hiding and unrelated settings. Candidate also refuses collection/heartbeat outside that Eastern window. No trigger change has happened.

Three lanes collect independently; changed-only business writes; separate source/save times; last successful coverage, last changed and all business-field diffs. Existing tables/log fields reused. Stage 8 and stage 9, including held log cleanup, are absent from the new execution path. Existing records, native links and prior failure/retention stops remain untouched.

**Exact blocker:** the normal scheduled runner has no installed way to send an immediate message to wef-records-coordinator. Candidate's injected callback is tested, but default production entry has no transport and explicitly reports Undelivered. A callback test or errs-log row is not immediate chat delivery. No new messaging service or monitor is assumed. Owner decision is required to allow collection deployment with logged-only failures pending chat delivery, or authorize/provide a supported delivery route.

Before any switch: reconcile candidate hashes and protected current files, complete applicable release checks, wait for the existing task to finish naturally, then replace only this four-file delta and update only its trigger. Do not invoke manually. Verify subsequent normal runs for all three lane receipts, correct current scope, business saves/readbacks, unchanged repeat behavior, timings, diffs and error isolation. Real production alert failure proof cannot be forced.

**Rollback:** saved baseline code + task XML and prepared rollback.ps1. If new normal-run verification fails, stop that affected execution path and report; restore this release's files/trigger to the saved baseline under the owner's approved rollback, preserving failed receipts, links and data. No data rollback/repair, forced collector run or automatic patch. Script refuses mid-run replacement and unrelated file changes. Rollback has only syntax inspection, not live execution proof. Restoring the baseline also restores its known publication/retention shortcomings; it is not a claim those are fixed.

## Owner decision superseding the alert-delivery hold

Owner approved deployment with stage failures recorded in errs-log for now; immediate Records-chat delivery is deferred. Stages 8 and 9 remain held. Verify only normal scheduled runs; no manual runs/repairs; rollback on failed release verification. Prior missing-delivery evidence remains, but is no longer a release hold under this explicit decision. Existing run is allowed to finish naturally before switching files.

