# Independent review: actual cleanup failure cause

Reviewer /root/schedule_stage_review. Review COMPLETE. Diagnostic behavior FAIL: the saved message cannot identify the actual cause. No implementation, records, stop, tasks or schedules changed; no live business requests or manual cleanup.

## Finding — P2 CONFIRMED: distinct failures lose their identity

wef-data/cleanup-airtable.js retireCleanup, current lines 137–139, throws only `Cleanup record changed before deletion` for every failed pre-delete comparison. The condition combines:
1. returned batch count differs from expected;
2. an expected native record ID is missing, even if counts match;
3. compared fields differ after excluding configured computed fields and already-deleted native links;
4. the returned record is no longer eligible.

The message carries no branch, expected/actual counts, native IDs, differing field names or eligibility reason. The actual saved historical receipt, last-run and stop preserve only that generic text. Consequence: subsequent investigation cannot distinguish missing records from changed fields or eligibility. This is a diagnostic defect retained from the failed baseline, not newly introduced by cleanupV2. It is not evidence that any particular field actually changed.

Smallest correction PROPOSAL ONLY: at the existing throw, include table, batch position, exact failed reason, expected/actual counts, missing native IDs, and differing field names or eligibility reason as applicable. Pass that message through existing action.error/run.error/cleanupV2 error and local receipt paths. No new table, record snapshots, collection gate, retry, deletion-policy change or timer. No correction applied or authorized by this review.

## Known from the saved run

Run1794034173999 began 2026-11-07T06:49:33.999Z and failed at06:51:38.460Z. run-logs action reports target_count1534 from1536 records, deleted630, then Error with the generic text. heartbeat was skipped, zero deleted; err-logs recorded target66, zero deleted and Running when the old overall path aborted. Failed stop names the same run/message. Those are the saved outcomes, not fresh Airtable verification.

Source control flow establishes that this specific message is thrown before DELETE for the failing batch; prior batches can already have completed, consistent with630 saved deletions. It does not establish which individual records remained or why the comparison failed. No offending record IDs, branch selection or field differences appear in the inspected exact run receipt/stop/last-run metadata. No broad unrelated history scan or live query was used.

## Unknown — do not infer

Exact failing record, field, branch, source of a missing/different row and whether any concurrent writer caused it are UNKNOWN. Saying simply that logs changed overstates the evidence. The later pending-native-family preservation defect is independently established but is NOT shown to have caused this historical generic comparison failure. A new diagnostic change cannot reconstruct missing historical details.

## Independent offline checks

Executed actual deployed adapter with fake HTTP and zero production traffic. Four distinct fixture conditions (count mismatch; expected member missing at same count; ordinary field mismatch; newly ineligible status) all produce exactly the same message, with no DELETE for that failing batch. The ineligible status fixture also changes compared fields; because of short-circuit order it does not claim to isolate the last Boolean branch. The source explicitly contains that eligibility branch. Matching control fixture completes mocked DELETE/absence verification. A sixth assertion confirms the entire failed-baseline retireCleanup function is text-identical to the deployed function.

All six targeted checks passed. These passing counterexamples demonstrate diagnostic ambiguity, not successful historical cleanup. Full16-test suite was not repeated. Exact command/runtime/time/output in commands.json; fixture source conditions.js; hashes and saved-evidence identity in manifest.json. Review report/logs read back and current hashes checked before receipt verification.

## Authority and acceptance

User requested Code Review and no guessing; scope is read-only causal diagnosis and targeted offline evidence. No repair, stop clearing, replay, deployment or manual business run is authorized by these findings. Current production contains separately approved cleanupV2/pending-preservation changes; this review neither rolls them back nor asserts they resolve the historical cause. Natural end-to-end completion remains outside this review's proof.
