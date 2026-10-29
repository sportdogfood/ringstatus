# Entries runtime switch - 29 October 2026, version 1.2

Deployed 17:27:23 UTC. CWF is outside deployment and acceptance. Schedules/CWF/Live untouched. Existing task and cadence unchanged.

Independent review: coordinator-review.md; 36 offline checks passed. Five fields added/read back: schema-applied.json. Exact 13-file deployment: deployment.json.

First normal run f53bcd48-c4ad-421b-b911-101f7fa3bb25, 17:29:02-17:30:15 UTC: task exit 0, business ok true, Partial for link uncertainty, no errors. 160 accepted class scopes, 117 observed participations, 161 archive events. Zero source acquisition and tenant reads. Accepted Data revision adaptive-2026-10-29T14:11:18.894144+00:00. Original source time preserved; currentness not asserted.

Airtable readback agrees with heartbeat/run-log. 127 total entries, 113 supported schedule mappings, one unresolved schedule mapping, three newer existing observations preserved. No group attribution asserted. All 13 existing record IDs preserved, no helper changes. Ten old rows entirely unchanged; automatic absence retirement disabled.

Second normal run b92abdb6-5503-430e-a88e-b420937bd066, 17:34:02-17:34:36 UTC: task exit 0, business ok true, Partial with no errors. Same 160 scopes and 117 observed rows; heartbeat/run-log agree. Repeat readback: 127 total records, zero duplicate keys, zero changes to IDs, native/scalar/plural links, original observation timestamps or source provenance. Task XML unchanged; 13 deployed hashes match. Two distinct normal scheduled runs verified. See verification-1.json and verification-2.json.

PASS: reviewed deployment, five fields, two ordinary runs, publication readback, repeat preservation, logging and archives. LIMITATION: group attribution remains unresolved; one schedule association is unresolved; three newer observations were preserved rather than regressed. No claim of fresh upstream Data or downstream consumer compatibility. No manual collector/source run or record repair performed.

Live separately reports its sync lacks the four link-status/reason fields and source-json. Publication/sync changes and consumer freshness policy require their own downstream authorization; Live checks remain intact. CWF compatibility is not claimed or gated.
