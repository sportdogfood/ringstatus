# Schedules-owned entries/trips release - 29 October 2026

Status: ACCEPTED. Additive entries, trips and entry-collection tables were created/read back. Reviewed code deployed at22:07:48Eastern under the existing collector lock. No schema or runtime changes to another base/collector; no legacy Worker route, task/cadence change, manual collector/source run or record repair.

179offline checks passed,14focused cases independently reviewed; two predeployment review findings corrected before activation. Exact source transport/validator attribution, baseline bytes and schema are retained here.

| Normal run | Eastern start/completion | Run ID | Result |
| --- | --- | --- | --- |
| First |22:10:02-22:13:54|19f039c7-dd55-4c55-8e0e-338c21a1f684|Success; wrapper/task exit0; no errors|
| Second |22:15:02-22:18:19|5349d8fc-b6fa-4ee8-a17b-66acc407c1f5|Success; wrapper/task exit0; no errors|

Each exhausted all89directory requests (877entries, including final page1 consistency read) and320unique decoded class reads across four show dates, collecting475trips. Full source-scope and identity validation passed, with x-rs-decoded confirmed on409reads. No trainer filter, per-tenant selection or artificial total entry/trip cap.

Readback:877entries,475trips,321complete collection scopes.10trips carry Conflict for ambiguous schedule occurrences; they remain collected without guessing or blocking other records. Source horse/rider/entry identities, local show/ring/entry/schedule links and accepted run attribution are verified. Second archive's prior-record snapshot proves entry/trip keys and IDs persisted. Original512schedule,56ring,3show and14helper IDs retained; schedule native show/ring links and static helper fields unchanged. Task XML unchanged and runtime hashes match. Native reciprocal trip fields are the intended additive schema links.

Every sweep's raw source payloads/receipts, failures and prior owned business records are compressed/read back/hash verified before publication under runtime/history/entry-collection/<run-id>.json.gz; both hashes are in run-logs. Heartbeat/run-log/attempt receipt agree for both runs. Directory observed-at represents the completed-page time for the coherent directory sweep; per-page original read receipts remain archived. Trip observed-at uses its class response completion. No consumer freshness lease is asserted.

No records repaired/deleted. Complete class disappearance may mark only that scoped trip history dropped in later normal runs; failed scopes cannot do so. Older-show entry/trip rows remain retained with show/date identity and archives; no new automatic deletion policy was introduced. Existing schedule old-show archive policy is unchanged. No runtime dependence on a separately owned entry module or Data collector.

Evidence: schema.json, review.json, offline-tests-final.txt, deployment.json, normal-run-1/2.json, task-run-1/2.json, business-before.json/business-latest.json, verification.json, acceptance.json and exact task XML.
