# Entries release

Version: 1.0. Date: 29 October 2026, America/New_York.
Status: Deployed and accepted through two normal scheduled runs.

Independent review and 36 offline checks passed. The five approved fields were added and read back. Thirteen runtime files were deployed at 13:27:23 Eastern using the existing idle task and lock. CWF is outside this release and acceptance. Schedules, CWF, Live and task configuration were not changed.

| Normal run | Eastern time | Receipt | Outcome |
| --- | --- | --- | --- |
| First | 13:29:02-13:30:15 | f53bcd48-c4ad-421b-b911-101f7fa3bb25 | Task exit 0; business ok; Partial; no errors |
| Second | 13:34:02-13:34:36 | b92abdb6-5503-430e-a88e-b420937bd066 | Task exit 0; business ok; Partial; no errors |

Each processed 160 accepted class scopes and 117 observed participations, with 161 archive events, zero direct source fetches and zero tenant reads. Heartbeat and run-log readback agree with each receipt.

Airtable contains 127 records: 113 supported current schedule mappings, one unresolved mapping and three newer existing observations preserved without regression. Group attribution remains uncertain. All original 13 record IDs and helpers were preserved. Repeat readback found no duplicate keys or changes to IDs, links, source-json or original source timestamps. All deployed hashes match; task XML is unchanged.

Partial reports link uncertainty, not processing failure. Accepted Data revision was adaptive-2026-10-29T14:11:18.894144+00:00. This does not establish fresh upstream observations. No consumer TTL or automatic absence retirement was introduced. No manual runs or record repairs occurred.

Evidence: evidence/data-input-switch-20261029/ contains coordinator-review.md, deployment.json, schema-applied.json, tests.txt, normal-run-1.json, normal-run-2.json, task-run-1.json, task-run-2.json, verification-1.json, verification-2.json and before/after business snapshots.

No Entries release action remains pending. Live reference sync exposure and consumer freshness policy are separate downstream work; Live and CWF consuming compatibility is not claimed.


## Conflict refinement accepted - 29 October 2026

Ambiguous schedule associations now show exact Conflict; missing associations remain unresolved. Supported group evidence may still disambiguate. No guessed schedule, clearing prior links, source timestamp refresh or absence retirement was added.

Records independently reviewed the two-file change; 30 offline checks passed. Two distinct normal scheduled runs passed with task exit0, business oktrue, Partial for link uncertainty and no errors:
- ad9fb75d-bd5f-41fe-8f0a-23bc09fd9053: 14:09:02-14:09:51 Eastern.
- f7745e75-9005-413d-91ea-3ce0df49e512: 14:14:05-14:14:49 Eastern.

Airtable confirms MASTERFUL record recB75ieQrVXxiVNY is Conflict; 113 supported schedule mappings remain linked. Repeat readback:127records,zero duplicate keys,zero changed entry rows. Existing IDs, helpers, native links and source times preserved. Heartbeat/run-log agree. Schedules code hashes and task XML unchanged. No manual runs or repairs.

Evidence: evidence/conflict-status-20261029/ (deployment.json, tests.txt, normal-run-1/2.json, task-run-1/2.json, after-1/2.json, verification-1/2.json).


## Shared-schedules-only release accepted - 29 October 2026

Four reviewed files deployed through the existing idle/lock route; 34 offline checks passed. Two distinct normal scheduled runs passed with task exit0, business oktrue, Partial for relationship uncertainty and no processing errors:
- 5ca0b0c7-fca9-44f3-9f74-143a472a7da1: 14:39:02-14:39:44 Eastern, accepted Data revision15.
- 0b494dd2-560b-499b-9360-ef36f3006369: 14:44:02-14:44:46 Eastern, accepted Data revision16 (18:22:08.409536 UTC).

504 active shared schedule rows define the universe. Each run processed160 matching accepted class scopes and117 participations. 257 shared class scopes lacking accepted observations were reported without pretending they were empty or stopping valid work. Readback:127totalrecords,113schedulelinks,1Conflict,103sourcegroupmemberships,11unresolvedgroups,13historicalrecords. Repeat IDs, schedule links, statuses, identity fields and original source timestamps unchanged; zero duplicate keys. Heartbeat/run-log match both receipts. Deployed hashes match; task XML and Schedules code hashes unchanged.

No external Schedules table/show-collection reads, source/sync parity replay or global Data-failure gate remain in the collector. Accepted Data integrity checks still apply. Actual errors remain reportable. Source group membership with class-date basis does not prove an operational group occurrence. No manual runs, record repairs, task/schema changes or absence retirement.

Proof: evidence/shared-schedules-20261029/ deployment.json, tests.txt, normal-run-1/2.json, task-run-1/2.json, after-1/2.json and verification-1/2.json. Release complete.
