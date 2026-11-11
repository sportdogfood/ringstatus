# November 8 â€” retain error logging; suppress unchanged repeats

The owner clarified that stopping the error flood did not authorize disabling errs-log. This correction restores new/changed error logging. One repeated condition (message + show/date, within its lane) produces one error row; all affected-record evidence stays in existing local run receipts. A later clear run allows recurrence to be logged. Uncertain error POST outcomes are recorded locally and not blindly retried.

At 1,000 or more logging records, the same cleanup selects all eligible excess above 500 and processes it through existing guarded deletion batches. It does not stop after deleting 500. Active/current records remain protected. Seven p.m. Eastern and new-show delete-only cleanup remain. No manual runs, record repairs, new schedules, Records alerts or decoded-trip requests.

51 author checks passed before the threshold clarification; the 11 affected cleanup checks then passed. Independent source review passed, with 13 checks for the final threshold/batch/failure delta. Prior failures and their corrections are preserved. Collection, source calls, business links and task settings are unchanged.

Installed at 2026-11-08T15:08:39.3855630Z. Exact27JS readback matches frozen reviewed candidate; task unchanged. Normal-run/native acceptance remains pending. The prior writer-disable release is superseded.

## Normal acceptance — November 8, 2026

10:09 normal linker 412667bc-ea86-4c07-9593-182bfc011d1c retained948 issues in receipt and created3 native error rows (horse/rider/trainer missing IDs). No collection/logging failures. Normal cleanup203233cd-a309-432a-8cba-04ce3726f107 succeeded:2393before,1893deleted,500remain, completed15:14:43Z. Native readback also heartbeat291/runlogs287. Active/current rows preserved by unchanged selection guards.

10:15 normal linker90f98104-73e7-4939-a479-7153ac649755 again observed948 unchanged issues, no logging errors and no new native error records. Latest3error records remain exactly those created15:09:47Z; table count500. Cleanup83ee1af3-9bea-42e5-8255-306aa67d08c9 Skipped with no excess, completed15:15:45Z; taskReady. Saved normal-repeat receipts and native-repeat-proof.json establish repeat behavior. No manual invocation, repair or forced error.

Error logging is enabled, duplicate flood suppressed and current backlog target verified. Missing entity source IDs themselves remain unresolved; no guessed links or resumed decoded requests. Seven p.m./new-show rules are preserved by source/offline checks, not claimed as newly occurring natural events. Uncertain future error-write outcomes are not blindly retried. The backlog cleanup occupied the10:09–10:14 scheduled execution Eastern; this was transient, not proof every future cleanup finishes within3minutes.
