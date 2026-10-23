# Continue after errors - 23 October 2026

Owner-directed change: scheduled failures no longer create or obey a permanent halt.
Each failed invocation remains marked failed; the next five-minute invocation can run.
Individual class failures preserve prior data, and failed runs cannot authorize retirement.

The wef-live Airtable table `errs-log` (tblMzOaTvWus2pM4I) records error-key, run-id,
occurred-at, show-id, date and message. Local runtime/errs-log.jsonl and run receipts
retain evidence if Airtable is unavailable. Historical halt receipts remain as evidence.

21 offline tests passed, including failure followed by another invocation and
logging outage retention. Existing task enabled with next normal run at 14:07:11 ET.
Five-minute trigger and hidden launch unchanged. No manual collector runs or record repairs.
Normal scheduled-run verification pending; offline tests do not establish live proof.

Files changed: entry.js, schema.js, test/entry.test.js. No changes to schedules,
entries, CWF or shared source/proxy logic.
