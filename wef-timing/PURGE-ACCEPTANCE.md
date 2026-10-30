# Ring pace purge — 29 October 2026

Implemented and deployed to the existing minute-based Timing task only.

PASS:35 offline checks, including prior/unknown/upcoming show distinctions, current/future preservation, two-pass authority, New York/UTC boundary, stale/partial/future-day rejection, pending deletion reconciliation, exact response-ID/readback validation, and no recreation of retired days. Independent source review closed findings.

PASS:normal20:17 and20:18 Eastern runs returned taskexit0/businessok/noerrors;80 snapshots each readback/hash verified. Purge authority became confirmed after the second normal invocation. 1375 original current/future snapshot IDs/keys/hashes remain intact.

Deletion is restricted to owned Engine pace snapshots for explicitly confirmed prior active shows and past working days. No source/collector/legacy/Messages/Results/Publish records are deleted or edited. Snapshot baseline state is updated only in Engine.

PARTIAL:natural rollover deletion not yet demonstrated because no eligible old-scope rows exist. No synthetic business records or manual pipeline runs were used to manufacture proof. Future observed rollover must verify actual delete/readback.

Current/future same-day history is retained; the4000record cap remains. Purge is not an approved same-day compaction policy.

Evidence:evidence/purge-acceptance.json,purge-before.json,purge-deployment.json,purge-offline-tests.txt,normal-bde1033a-61f5-4e44-b843-ef45bd73ec54.json,normal-54d51d96-c1d1-49d3-b4b5-9c6578a850fe.json.
