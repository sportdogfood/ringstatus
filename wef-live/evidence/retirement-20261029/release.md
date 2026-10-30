# Live older-data retirement - 29 October 2026

Authorized: archive and verify older Live records then retire them on normal runs even when today has no activity; protect current/future rows and retain archive locations, counts and logs. No manual runs or repairs.

Deployment: 23:06:46Z / 19:06:46 Eastern, existing wef-live-collector task and checkout. Files: collector.js, airtable.js, retirement.js plus focused tests. Prior collector/adapter saved in before/. Tested staged code before replacement; 62 tests passed. Deployment hashes in deployed.json.

Baseline: 5 old rings, 31 old groups, 56 old classes, 305 old trips and 153 old operational collections (show 200000075). Full fields and links in before-records.json for exact archive comparison. Current reference IDs also recorded.

## Normal scheduled acceptance - PASS

- 852feeec-ef3a-4176-bd4a-2014b4dfa15d: 19:08:13 to 19:09:47 Eastern. Success with empty current activity; archived and retired 550 old show records: 305 entries, 56 classes, 31 groups, 5 rings and 153 operational collections.
- 3fad9818-a4f3-4c62-b12e-cbc3a499d265: 19:11:14 to 19:11:40 Eastern. Success with empty activity; zero additional retirement, seven retained archive catalogs, no duplicate cleanup.

Both: matching Success heartbeat/run-log, zero errs-log rows and zero logging errors. Verified seven gzip archives and hashes against catalog, and every original archived record ID/field/native-link value against before-records.json. All 512 synced schedule and 512 synced Entries reference IDs remain. Active Live observations are now empty because the current show has no live activity; old show 200000075 is represented by its archive catalog, not active observations. Source references and other collectors were not edited.

Evidence: verified-<run-id>.json contains exact checks. Catalog in wef-live.collections, source-kind archive-catalog, status Retired, archived show 200000075. Catalog file paths are local paths, not hosted downloads. Logs retained. No manual collector/source calls or record repairs.

Limits: natural same-date other-show deletion was not exercised; it remains offline-tested. This release proves older-date retirement on a normal empty run. Historical native-link values survive in archives; deleted active records no longer supply active reciprocal links. No populated activity, multi-group, or downstream Entries acceptance claim.
