# Participation link contract - 24 October 2026

Scope: wef-live only. Existing five-minute task and runner entry point unchanged. No manual collector runs or record repairs. Other collector code, shared source decoding and CWF untouched by this change.

## Behavior

- Keep the existing native live-entries.entry link to the local synced participation record. Do not add a redundant trip-to-schedule native link.
- Read schedule-key, schedule-keys, last-seen and observation-status through the existing restricted wef-entries.entries sync. Destination remains read-only, automatic sync, explicit field selection. Entries owner manages producer/source view separately.
- Accept exact show/date/entry/class/horse/rider identity only. Require active participation and a successful source observation no older than 30 minutes.
- Parse sorted unique schedule-keys JSON. Scalar fallback applies only when the plural field is absent. Singleton scalar must agree; multi-key scalar must be blank. No inferred participation-specific group eligibility.
- Require a complete dated schedule collection no older than 30 minutes and identical active source/local stage-key sets for that date. Listed keys must resolve uniquely to active, identity-matching source and local rows.
- Invalid, stale, unavailable or ambiguous references retain prior links and label them historical. Without an existing link, a rejected candidate is unresolved. Unselected participation without a prior link is unmatched.
- Class errors preserve previous observations and links, mark applicable participation links historical/unresolved, and do not stop other classes. Participation failures appear in errs-log and Partial run summaries.
- Audit fields: participation-link-status, participation-link-reason, participation-checked-at, participation-verified-at. Failed checks do not erase the last verified timestamp. Consumers must require current status; timestamp alone is not current proof.

## Validation

40 offline tests passed. Independent read-only review identified and then cleared the full dated source/local key-parity correction; reviewer independently passed all six participation tests.

12:57 ET normal run ede4bd03-7b4a-4979-b5fc-89fb745555f0 completed 12:58:10 ET, ok=true, 5 rings / 26 groups / 59 classes / 275 entries. Required upstream observation fields were not populated yet: two participation links remained historical, collection continued, no logging errors. This transitional run is not proof of current participation links or the final parity correction.

First post-configuration normal run: d66f6c2e-6dd4-4ea8-b93d-ffb5699e840e, 13:07:13-13:08:03 ET. Both TODAY participation links current; all six prior native entry links preserved; no participation or logging errors. 5 rings / 26 groups / 59 classes / 287 entries. Pre-existing class 200030429 identity mismatch remains logged, so overall result is Partial; collection continued. Evidence: participation-run-1.json. Transition link-retention and errs-log evidence: participation-transition.json.

Second post-configuration normal run: 5257eecc-cbfe-4cb6-93bc-e37e3856b472, 13:12:13-13:13:05 ET. Both TODAY participation links remained current with refreshed checked/verified timestamps; all six prior native entry links preserved; no participation or logging errors. Same 5 rings / 26 groups / 59 classes / 287 entries. Class 200030429 remains independently unresolved and logged; overall Partial, collector ok=true. Evidence: participation-run-2.json.

Two ordinary scheduled runs verified the live participation contract. Natural participation-specific multi-group proof remains unavailable upstream; fixture coverage is not live multi-group proof. No manual execution or record repairs were used. Source-reader hashes and review/test result are in participation-deployment.json; saved destination read-only automatic-sync settings are in participation-sync-settings.png.
