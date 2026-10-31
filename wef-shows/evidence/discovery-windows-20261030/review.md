# Independent review — October 30, 2026

Reviewer: existing read-only shows_review agent. Target: prepared-manifest.json and isolated staged discovery-window delta against baseline.json. No production files or runtime state changed during review.

Initial Shows-only result: BLOCKED for deployment, no additional actionable defect beyond the known compatibility dependency. The approved companion review below supersedes that blocker.

- P1 confirmed: retaining last_checked outside 16:00–20:00 conflicts with existing downstream 30-minute authority expiry (Schedules show-source.js:28, Live model.js:33; other affected readers listed in README). Do not deploy Shows alone or renew source timestamps to bypass this guard. Companion reader contract requires approval.
- Offline PASS: independently executed 27 relevant tests covering Sunday/daily boundaries, both DST transitions, retained timestamps/IDs, endpoint restrictions, heartbeat/logging and count-stop preservation.
- Baseline PASS: deployed collector hash matches the recorded baseline.
- Existing count-replay stop preserved: October 30 at 18:07:49Z. This change does not fix or resume it.
- Deployment and real scheduled daily/skipped/Sunday behavior, genuine rollover and downstream compatibility remain unverified.

Writer's full suite: 55/55 passed. The identical transcript-capture run also completed with 55/55 passing; see tests.tap. These were offline tests only.

## Approved companion checks — independent review

Human explicitly approved companion checks and deployment in the Shows chat. The independent reviewer inspected authority-age.js, the seven companion reader changes and their original baseline copies. Result: PASS within the approved authority-only scope, no new actionable defect.

- JS/Python calculations preserve Eastern DST, original timestamps, and each reader's existing future/equality boundary behavior.
- Independently executed three new cross-language/actual-reader authority tests: PASS.
- Prepared full suite: 380 tests, 341 pass, 39 failures. All 39 failure names match the unchanged baseline Entries/Timing suite (208 tests, 169 pass, 39 failures). This is not a fully passing suite. No failing implementation or fixture was patched to force this result.
- Data's offline scheduled-contract suite: 13/13 pass.
- Active Timing entry-data.js/data-runner.js, purge authority and supporting Shows-reference paths are included. Retired Schedules/Engine paths remain untouched.
- Deploy only the manifest: helper first, seven companion readers, discovery helper, Shows collector last. Preserve concurrent day-source.js/day-archive.js edits and the count-replay stop; do not copy the staged tree over production.
- Deployment hashes, normal scheduled daily/skipped/Sunday observations and genuine rollover remain separate acceptance gates.
