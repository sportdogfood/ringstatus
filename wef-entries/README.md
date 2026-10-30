## Linking-only restoration - v1.7 - 29 October 2026

Owner explicitly approved restoration of linking-only scope, preserving records, links and compact-input/readback/log fixes. Deployed 19:37:05 Eastern using the unchanged wef-entries-cwf-collector route and existing lock. Normal scheduled acceptance PASS:19:39 and19:44Eastern triggers completed task0/businessPartial without errors; both preservation readbacks passed. All513entries/446collections, checked links and source times preserved. See evidence/linking-only-restoration-20261029/acceptance.json.

Only active Entries-local shared schedules select matching accepted Data class/participation evidence. No new Data-only participation outside those scopes. No shared schedules means no Data replay or business updates. Live reads/qualification/enrichment and collector comparison outputs are removed from active execution; the unused live-input helper is retained in release before/ history. Existing stored lane/Live JSON is preserved as historical evidence, not scrubbed, regenerated or used to select records. Current linking still validates identities, deduplicates, reports unresolved/Conflict and preserves supported facts.

Preserved: compact once-per-day inventories below the existing 8MB bridge limit; bounded exact-key Airtable readbacks with lost-response protection; compressed verified archive indexes for oversized logs; all matcher/freshness/identity/group safeguards, existing keys, native/helper links and source times. No absence retirement, schema/task/cadence changes, manual runs or record repairs. Existing downstream Live consumes the unchanged core source-evidence/schedule contract; no consumer code or sync changes.

Evidence: evidence/linking-only-restoration-20261029/ (before/, staged/, current-tests.txt, saved-replay-check.json, review.md, deployment.json, before.json, release.md). 41 current-contract Node checks, compact projection Python check and saved406class replay passed; independent review cleared the selective correction. Historical CWF/feed tests remain historical, with baseline failures documented; they do not restore tenant collection duties.

This supersedes later comparison/enrichment implementation claims and earlier pending comparison proposals. Prior accepted shared-schedule linking behavior and historical evidence below remain protected. Data/Season scope and separately approved Live retirement work are independent.


# Current scope: link the schedules shared into Entries

Only active local shared schedules determine what Entries processes. Accepted Data supplies matching participation/group evidence; unrelated Data failures do not block supported links. No source-base schedule parity or dated-health replay. Missing evidence is reported and processing continues; ambiguous or duplicate schedule keys are Conflict. Actual read/write errors remain errors.

Existing keys, native links, helpers, provenance and source timestamps are preserved. No absence retirement, source acquisition or tenant selection. Group membership plus class-date mapping does not establish an operational group occurrence.

Latest release evidence: evidence/shared-schedules-20261029/. Accepted through normal 14:39 and 14:44 Eastern runs, 29 October 2026. See release.md.

# Current Entries deployment - 29 October 2026

Data-input switch deployed at 17:27:23 UTC after independent review and 36 offline checks. Two normal scheduled runs verified at 13:29 and 13:34 Eastern: both business-ok Partial, no processing errors. Evidence: evidence/data-input-switch-20261029/.

Entries links accepted retained Data to canonical Shows and read-only Schedules: rings, groups, classes, horses, riders and actual trainers. No direct source acquisition or tenant selection. CWF is outside this module and its acceptance. Optional Season references are nonblocking.

Existing five-minute task, launcher, keys, native links and helpers remain unchanged. Historical task name wef-entries-cwf-collector does not mean a tenant dependency. Module heartbeat key: wef-entries.

Source observation times and revision provenance are preserved. Replay never refreshes source time. No consumer TTL is invented. Automatic absence retirement is disabled. No retained class evidence fails unavailable; explicit complete zero-trip classes succeed. Ordinary class/date links continue while uncertain group links are flagged.

Added fields: source-trainer-ids, schedule-link-status, schedule-link-basis, group-link-status, link-issues. Future results/messages/waze links do not block supported linking.

## Operation and evidence

The existing task calls run-hidden.vbs -> run.ps1 -> entry.js every five minutes. entry.js holds collector.lock and saves runtime/last-run.json. Only scheduled execution is operational proof.

See [release.md](release.md) for accepted run IDs and [scope.md](scope.md) for responsibilities. Exact hashes, schema readback, task XML and before/after Airtable snapshots are in evidence/data-input-switch-20261029/.

Historical README content is preserved in evidence/data-input-switch-20261029/README-before-finalization.md; its tenant-driven instructions are retired.

Downstream Live sync field exposure and consumer freshness policy remain separately owned work. Entries release does not claim Live or CWF consumer compatibility.

Latest accepted refinement: ambiguous schedule links show Conflict; missing links stay unresolved. Verified through normal 14:09 and 14:14 Eastern runs on 29 October. See release.md.
