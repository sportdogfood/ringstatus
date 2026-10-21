# WEF duplicate schedule-key correction — 20 October 2026

Owner authorization: APPROVED TO EDIT, fix duplicate schedule keys, preserve archive-before-delete, test/deploy and verify two normal scheduled runs; keep existing workflows untouched, no manual runs or record repairs.

## Proven baseline and exact delta

Editable isolated source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules. Deployed package: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules. Existing run/archive logging release was verified at 09:20/09:25; no need to repeat that implementation. Fresh read-only review identified a P2 issue at current-store.js: uniqueness verification threw before same-key duplicates could reach its existing exact archive-and-cleanup path. Existing non-idempotent schedule POST retry can create those duplicates after an uncertain response.

Corrected only private current-store.js and tests/current-store.test.js. Before cleanup, select and verify the existing intended survivor (or a newly created match) against all required fields, complete source JSON and dataset ID. Derive extras from the post-write readback so newly created duplicates are included. Existing scope predicate, exact gzip archive verification, archive logging and delete pacing remain in place. After archived extras are removed, read back exactly one verified survivor per desired key before publishing completion. Existing request helpers/transport, collector discovery, source key, manual/drop guards and all other workflows remain unchanged. No live records were repaired manually.

## Local evidence and review

Five regressions were added; three first failed for the existing duplicate blocker, while the two existing protective behaviors already passed. All 14 focused current-store checks then passed. Full private suite: 80 PASS, 0 FAIL. New coverage includes identical stage-key duplicates and repeated collection, duplicates returned by repeated POST within one invocation, exact extra-row archive before deletion, owner-note preservation in archive, survivor/other-show preservation, invalid-survivor deletion blocking, archive failure protection, and cleanup failure followed by normal-invocation convergence. Fixtures only: no business endpoint/task/collector execution.

Read-only reviewer found no material issues. This is independent static review; execution of the 80-test suite was performed in this chat. No additional dependencies, schema changes or redesigned transport.

## Deployment — 09:41 Eastern

First deployment opportunity was deferred because the normal 09:40 trigger was too close. No files changed during that deferral. The unmodified 09:40 run completed at 09:40:51, exit 0. Deployed current-store.js and its regression test between runs at approximately 09:41:05, after Ready state and no collector lock were confirmed. Both source/deploy hashes and protected source hashes verified. Separate source/deployed inventories are expected to differ for a previously protected standalone test; no baseline test was overwritten.

Task definitions and runtime settings were not changed. No secrets read/printed, environment/data/checkpoint overwrite, manual trigger, direct SGL request, record repair or other workflow edits occurred. Two normal scheduled runs at 09:45 and 09:50 remain pending; do not treat passing local tests as that live acceptance.

Evidence directory: deployed runtime/duplicate-key-release-20261020/. Contains exact baseline source/deployed manifests, code/test copies, protected hashes, task XML, red/focused/full test outputs, and deployment.json. An initial preparation check incorrectly assumed the two inventory manifests were identical; it stopped before product edits, and was corrected to preserve the intentionally separate manifests. No product test or runner failure was masked by that preparation correction.

Current-data baseline: 435 current schedule rows and zero duplicate keys via connector. Duplicate recovery must remain fixture-proven unless naturally observed; no duplicate live records may be manufactured. Preserve every prior failure finding as history and stop the approved verification path on a new actual failure.


## Duplicate schedule-key correction verified - 20 October 2026, 09:52 Eastern

Owner-approved P2 correction deployed to private current-store.js and its tests only. Verified survivor first, then exact archive-before-delete of scoped extras, then final unique survivor/field readback. Post-write duplicates from repeated creates now reach the existing safe reconciliation path. Existing transport, schedule/manual/drop behavior and other workflows preserved. Local suite 80 PASS, targeted 14 PASS; release reviewer no material findings. Two normal triggers 09:45:01/09:50:01 exited 0, ended 09:45:49/09:50:52. Each reports all four October 22-25 dates complete, 435 rows, no errors/unmatched times and only the empty-October-22 warning. All 435 record IDs/values unchanged; zero duplicates; eight actual attempt archive files/hash entries verified. No live duplicate case was fabricated: duplicate/failure recovery is fixture-proven, normal operation independently observed. No manual runs or record repairs. Protected source hashes and collector/heartbeat/epoch/pipeline task definitions unchanged. P2 review finding resolved. Next Monday code review remains October 26 at 09:00 Eastern. Evidence: runtime/duplicate-key-release-20261020/deployment.json; root docs/coordination/wef-schedules-duplicate-key-fix-20261020.md.

