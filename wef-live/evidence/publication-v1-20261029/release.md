# Live publication v1 — 29 October 2026

Owner approval verified in wef-records-coordinator human message: add seven fields, implement/deploy Live publication, update ownership and verify two normal scheduled runs. Independent populated/repeat proof is required; no manual runs or task changes.

Seven multiline live-observation-json fields created/read back (schema-readback.json). Live-only modules plus optional read deadline in existing adapter deployed at 20:28:06 Eastern through existing task path. Exact files/hashes: deployed.json. Other collectors, native links, syncs and tasks untouched. Acquisition fields excluded from PATCH by allowlist; no destination creation/deletion.

80 offline tests pass both staged and deployed. Independent code review passed after correcting source multi-group attribution, exact trip/UUID identity, unknown outcome audit, dispatch freshness/TODAY checks and request budgets. Review evidence: review.md. Publication source/intent/before-after is archived before PATCH; outcome/readback follows. Unknown writes retain pending intent for a later normal readback. Existing source/retirement protections remain.

First new normal run: 9ed1ab69-9d8c-40df-b84f-1c9d6d7f9f1f, 20:29:12–20:29:32 Eastern. Success, both publication lanes Skipped with zero patches/errors because live activity empty. Matching heartbeat/run-log completion, no error records. Seven old-show catalogs unchanged; archive file hashes/counts verified. Seven destination tables read back, no observations populated. Evidence: verified-9ed1ab69-9d8c-40df-b84f-1c9d6d7f9f1f.json.

Second new normal run: b1ffb86b-7e0c-41e3-b72c-70306fe48b65, 20:32:12–20:32:33 Eastern. Success, both lanes Skipped with zero patches/errors. Heartbeat/run-logs agree, no error records, deployed hashes match. Evidence: verified-b1ffb86b-7e0c-41e3-b72c-70306fe48b65.json.

Read-only baseline comparison across all seven tables: all existing record IDs, keys and native links retained; no additions/removals. Schedules 8 rings/26 schedules; Data 8 rings/26 groups/0 classes/26 schedules/0 trips. Only non-overlay differences were Data last-success on 26 groups and 26 schedules, consistent with its independent collector; Live made zero publication PATCHes. Seven catalogs and archived file hashes/counts retained exactly.

Acceptance: PARTIAL. Deployment, empty-run safety, independent invocation, preserved baseline and logs are proven. Next gate: two naturally populated normal runs with exact observation matching, one-field PATCH/readback, protected destination fields/native links and repeat/no-change behavior verified from durable audit. Do not manually invoke, seed records or treat empty runs as populated proof. Natural populated publication, destination protected-value/link evidence during actual PATCH, and populated repeat/no-change behavior remain unverified. Multi-group natural proof remains separate; ambiguous source groups are withheld offline. No old v1.6 run is counted toward this release.
