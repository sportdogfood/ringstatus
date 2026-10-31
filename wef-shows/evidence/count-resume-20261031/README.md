# Count publication recovery — October 31, 2026

Owner authorized repairing the count-replay timeout and restoring show-days entry/trip publication, preserving workflows, two normal scheduled proofs, no manual runs or record repairs.

## Inspected cause and current repair boundary

The retained stop records a90-second count-replay timeout on October30 at18:07:49.560Z. That historical full-chain replay is stopped. The exact original transient wall-time cause is not retained/reproduced, and no claim is made to diagnose it further. Source inspection shows the old path traverses predecessors and all observations. A newer saved-day path is ALREADY installed in Shows/Data: day-source.js selects day-heads, day-replay.py reads saved_day_summaries, and show-days.js preserves non-eligible older days. Current saved receipts have6and5reference parts, with no predecessor replay. Preserve this existing implementation; do not invent another replay or raise the timeout again.

Current runtime and read-only Data dependencies are frozen by manifest.json; four Shows files are snapshotted. No runtime code was changed by this recovery. Recovery will archive and release ONLY the exact old count stop after independent review and at an idle boundary, using resume-counts.py. That is explicitly authorized by the owner repair request; it does not invoke any business process or repair Airtable rows. The unchanged five-minute task alone will publish counts.

## Offline checks

17 targeted isolated reader/replay/publisher tests passed (tests.tap). Tests ran from wef-shows/staged/count-resume-20261031 with no production day-heads in the isolated tree. The Python saved-fixture-test.py uses frozen test source parts and a TemporaryDirectory, matches original counts/source times, verifies repeat results and rejects corrupt receipt/archive evidence. No production replay, source/provider call or collector was invoked. Independent review COMPLETE/PASS; durable0.2.2report/command log/receipt/verifier retained in this directory.

## Acceptance

COMPLETE: exact old stop archived/released at07:39:50Eastern. Two normal scheduled runs published October31 andNovember1:741d912f-bec2-42d7-8819-855a4d3b1c98 at07:44 andfb210d52-e086-48ee-808a-6840a9fb4258 at07:49. Both Success, zero errors/telemetry failures; independent Airtable counts/heartbeat/log/source-clock/native identity readback and archive checksums passed. See verification.json, normal-*.json and readback-1/2.json. Older days' existing values/IDs/links/priorities remain preserved under the current Data policy. Original observation ages and Unknown/Stale/verified-zero rules stay intact; publication success is not a promise of fresh counts. before-airtable.json and stop-before.json retain prior evidence.
