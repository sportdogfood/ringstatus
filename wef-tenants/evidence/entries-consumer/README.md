# CWF entries consumer — 23 October 2026

Owner authorization: use synced wef-entries; stop duplicate collection only after successful normal scheduled verification. Preserve helpers, links and other CWF functions. No manual runs or record repairs.

## Baseline and scope

- Existing CWF collector: `wef-tenants/collector.js`, `airtable.js`, `settings.js`; originals copied to `baseline/` before changes.
- Scheduled task: `wef-tenant-cwf-collector`; five-minute trigger and actions preserved in `task-before.xml`.
- `baseline.json`: CWF schema, helper records, old entries and links captured at 17:00:02 UTC.
- Unrelated pre-existing repository changes, schedules/entries producers, task definitions, credentials and legacy code are outside this edit.

## Native sync

- Source: wef-entries `appSxF4pYMBaxpTZd`, entries `tblqkWmrsYMs2hyQ6`, view `CWF team entries` / `viwGmsuyn4cofGwnX`.
- Filter: trainer-ids is not empty. Private-only follows do not enter this team export. Collections, source-json and source schedules links are hidden from the shared view; private ownership is not exported.
- Destination: wef-tenant-cwf `appxKPBtdl2Rdq7CS`, `wef-entries.entries` / `tbl7ychn5HELB3HTr`.
- Native settings observed: automatically sync at regular intervals; edit source records Off.
- Fifteen source scalar fields are read-only. Six local links connect the existing horses, riders, trainers and synced shows/rings/schedules. Reciprocal fields are `wef-entries.entries` on the existing tables.
- No manually written business records. Initial import and subsequent source changes are Airtable native sync.

## Gated transition

`WEF_TENANTS_ENTRY_SOURCE_MODE` defaults to `legacy`.

- `shadow`: existing collection still runs; validate central heartbeat, each trainer/date collection, exact team sync values and local links; compare active legacy entries with the sync. Normal scheduled evidence must pass before switching.
- `synced`: read the validated native sync and central completion metadata; no people endpoint requests from CWF. Continue standing helpers, entry-derived helpers, rider/horse links, local collection audit/archive, heartbeat, run-logs and tenant-index updates. Keep old entry records/links untouched. No fallback collection on bad/stale sync.
- Empty validated collections succeed; inactive horse/rider helpers remain collected and their flags are preserved.
- Central business writes and synced source-field writes are rejected by the adapter. Only six local link fields can be patched by CWF.
- `verify-entry-consumer.js` reads normal-run receipts and Airtable only; never executes the collector. It verifies source equality, local/reciprocal links and baseline helper preservation, and saves a per-run evidence file.

## Current verification

- 71 offline tests passed (`tests.txt`), including zero fetches in synced mode, repeat identity/link preservation, valid empty collections, private-only exclusion, stale/partial input rejection and read-only source enforcement.
- Shadow mode enabled at 13:09:42 Eastern for the next normal 13:12 trigger. No manual run.
- Before shadow: original CWF runs at 13:02 and 13:07 encountered people HTTP 502 for 50121. This source path was not patched. Independent wef-entries completed its 13:09 run successfully with 9 active entries.
- Shadow runs at 13:12 and 13:17 passed. Both matched all 9 active entries, all links/reciprocals and baseline helpers. Repeat run required zero link updates. Evidence: `shadow-a7cfd069-f7de-42db-9476-e24159629801.json` and `shadow-18b3c31b-40f0-40b7-ab24-2c0fa4ede52f.json`.
- Cutover to `synced` enabled at 13:19:54 Eastern (`cutover.json`). Task definition remains identical (`task-after.xml`).
- First normal synced run: 13:22:11–13:24:23 Eastern, run `0befec08-5b16-454d-9ad5-b1a521543651`. Success, 9 rows, 0 people fetches, 0 link changes, 9 verified linked entries; old entries byte-for-byte unchanged from final shadow snapshot; helpers and trainer configuration preserved. Read-only proof: `synced-0befec08-5b16-454d-9ad5-b1a521543651.json`.

## Final repeat proof

- Second normal synced run: 13:27:05–13:29:19 Eastern, run `f17c8cd3-2dff-4bad-8b5e-c9a1adecb980`. Success, 9 entries, 0 people fetches, 0 link changes, all 9 local and reciprocal relationships verified. Evidence: `synced-f17c8cd3-2dff-4bad-8b5e-c9a1adecb980.json`.
- Both post-switch snapshots retain identical record IDs for all nine synced entries; 76 horses; 37 riders; 3 trainers; 12 audit collections; 6 ring helpers; 6 walks; tenant configuration; and 11 original entry records. Old entry records/fields are unchanged from the final shadow snapshot. `repeat-proof.json` records this comparison.
- Existing scheduled task and five-minute cadence unchanged. No manual collector runs, source fetch diagnostics, record repairs or deletes were performed. Normal runner archive-before-cleanup behavior remains intact; old entry cleanup is excluded in synced mode.
- Screenshot: `synced-entry-links.png`. Live table: https://airtable.com/appxKPBtdl2Rdq7CS/tbl7ychn5HELB3HTr
- Complete. No workers or extra automations were created. Unrelated user repository changes remain untouched.
