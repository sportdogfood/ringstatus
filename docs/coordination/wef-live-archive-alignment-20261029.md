# Live archive alignment with the proven Schedules pattern

29 October 2026. Owner requested evaluation of Schedules archival and instructions to Live to use the same pattern. Evaluation and owner handoff only: no code, Airtable records, task definitions or archive files changed; no manual collectors or source calls.

## Verified baseline

Schedules current-store.js finalize() runs after report.ok; store.js establishes success for all discovered show dates before finalization. Old owned schedule and collection rows are compressed locally, decompressed and compared with the exact snapshot, hashed, and then removed with deletion acknowledgments checked against requested IDs. show-index.js reconstructs a retained per-show archive catalog from verified files/logs; aggregate counts deduplicate Airtable record IDs across snapshots. Canonical discovery remains separate from this archive index.

Local read-only evidence for show200000075:

- wef-schedules/runtime/history/retired/e9e115d2-a31e-41e4-9a49-288f9b9fc431.json.gz:463 schedule records.
- wef-schedules/runtime/history/retired/068d8cc7-88c9-42b1-82ff-c58cf11105e0.json.gz:4 daily collection records.

Both files decompressed and parsed successfully during this evaluation. The Schedules owner independently confirmed the baseline. Airtable catalogs are file indexes, not hosted download links.

Live already has the essential protection: archive.js gzip/byte-readback/hash verification; airtable.js retire() validates the exact archived records and permitted scope before deletion; collector.js retires children before parents only after qualifying successful collection. rollover.js protects current/future dates and handles earlier dates plus same-date other-show records. Empty/skipped runs retain historical records. Existing source snapshots and old archive folders must remain intact.

## Smallest Live delta sent to the owner

1. Reuse existing archive/rollover/retire helpers. Stage and verify exact snapshots of eligible owned live-entries, live-classes, live-groups, live-rings and operational collections before any deletion. Snapshot all field values and native record IDs/links; index the archived OLD show/date, not the new current scope used by keep() today.
2. Keep a permanent archive catalog in existing writable collections. No canonical Shows writes, reference-sync writes or new table are proposed. Use existing fields below; validate the actual schema before implementation. Never retire catalog rows, including through the lower-level adapter.
3. Read back the catalog and verify every selected table archive before deletion starts. Then retire entries/classes/groups/rings, collections last. Match deletion acknowledgment ID sets exactly, rather than only comparing counts/deleted booleans. Preserve incomplete/uncertain cleanup status and remaining rows; no blind deletion retry loop.
4. Preserve current/TODAY scope, future rows, synced references, helper/private links, source collection, cadence and independent collectors. Missing or failed archive/index evidence retains affected old rows and logs the issue; it must not erase already valid current observations or justify guessed links.
5. Keep the existing requirement for a clean qualifying populated canonical TODAY collection. Live is TODAY-only; do not copy Schedules' all-date acquisition. False live-status, empty widgets, skipped runs and missing/failed/partial source evidence do not prove safe retirement. Changing this empty-run rule is a separate owner decision with exact source identity/completeness evidence, not implied by the analogy.

## Proposed existing-field catalog contract

| Existing collections field | Catalog use |
|---|---|
| collection-id | Stable archive identity including old show/date/table and snapshot hash; distinct from operational collection keys |
| show-id, date | Actual archived record scope, split mixed snapshots into explicit scopes |
| source-kind | archive-catalog, permanently excluded from operational cleanup |
| source-id | Archived owned table |
| started, completed | Archival/verification timestamps, not renewed source observation times |
| status | Verified/pending cleanup, completed or partial/uncertain cleanup, using an agreed explicit vocabulary |
| row-count | Exact per-file record count |
| fingerprint | Verified decompressed SHA256 |
| archive-file | Existing local verified file path |
| message, errors | Compact versioned manifest/run reference, table/scope, verification and cleanup outcome; payload stays in archive |

Per-show totals must deduplicate archived Airtable IDs across snapshots; summing files would overcount partial-cleanup/repeated snapshots. Catalog records and run-log archive evidence survive active-record retirement. Existing folders/paths remain valid; no rename, hosted storage or portability migration is required.

## Files, acceptance and boundary

Expected bounded files: Live collector.js, rollover.js, airtable.js and focused tests; reuse archive.js unchanged where sufficient. Preserve all other module behavior. Any schema addition requires a concrete separately reviewed delta; none appears necessary from the existing schema.

Offline checks: new default/same-date old show; earlier/current/future dates; empty/skipped/partial/unresolved input; corrupt or missing archive; catalog readback failure; exact deletion IDs; partial cleanup; repeated run/catalog retention; raw old-scope/native-link preservation. Then two ordinary scheduled runs with actual record/link/catalog/log readback. Actual new-show archival/deletion remains UNVERIFIED until a natural qualifying rollover occurs; same-show empty runs cannot prove it. No seeded records, manual collector/source calls, forced syncs or repairs to manufacture proof.

Live acknowledged the minimum plan and recommends preserving the empty-run prohibition. Implementation/deployment and expanded collections retirement are not performed by this evaluation. The catalog field contract and exact implementation scope go to Live for review before changes. Schedules is untouched.

Live confirmed preparation complete and the exact proposed catalog status vocabulary: Verified (archive/catalog readback passed, cleanup incomplete), Retired (every requested ID acknowledged), Partial (only some deletions confirmed), Uncertain (outcome not verified), Failed (archive/catalog verification failed, no deletion authorized). These are proposed values in existing fields, not a new schema or live status change.

Exact implementation approval scope to present: Live-only code/tests/deployment for the retained collections archive catalog and archive-protected retirement of its four owned observation tables plus operational collections; preserve the populated canonical TODAY guard, current/future rows, syncs/helpers/logs/keys/links and cadence; verify two normal scheduled runs without manual source/collector runs, forced syncs or record repairs. Actual natural rollover/deletion proof remains separately observable and must not be claimed from empty normal runs. Expanding the existing retirement adapter allowlist to collections requires that concrete approval; this evaluation has not changed it.
