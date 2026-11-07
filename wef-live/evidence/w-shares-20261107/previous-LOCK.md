# WEF Live — locked protected baseline

**Version 2026.11.07.4 · Locked 2026-11-07T10:06:03-05:00 · America/New_York.**

Empty-only linker deployed after the three unchanged collection lanes. Five Live tables link to existing local synced wef-shows.show, wef-shows.show-days and wef-data.schedules. Internal forward fields reused; native reciprocal appends are automatic. Populated links are not audited or rewritten. Old WEF Schedules links retained but not used for new links. Decoded requests stay paused; only approved empty native links on retained trips are filled.

Further code/schema/routing/cadence/cleanup/deployment/resume changes need owner approval. Normal scheduled operations and authorized read-only checks continue. This is a documented lock, not an ACL/task/process change. No manual collector/provider runs or record repairs.

Protected26JS/sharedreader/task and document hashes: [locked baseline](evidence/linker-20261107/locked-baseline.json). [Current README](README.md) and [deployed manifest](evidence/linker-20261107/deployed.json).

## Verification

17offlinechecks and author review passed; not independent review. Normal09:51/09:57runs verified2,897emptyfills on968existing records,2,300populatedforwardfields retained, then zero repeat writes.423trip business rows frozen; decoded calls0. Native parent logs/heartbeats and7individual skip logs read back perrun. [Exact IDs/evidence](evidence/linker-20261107/acceptance.json). `completed:null` handled and tested; no payload correction or natural-null waiting condition.

## Known trouble and limits

- Seven Data links stay blank: only Dropped schedule occurrences match groups200028530/200028533,class200031158 and its two trips/twoentries. Logged individually; no forced joins.
- Immediate Records chat alert transport is not configured; local/Airtable logs are not chat delivery.
- Two conditional cleanup-reporting P2s remain: unexecuted plans can stay Running; interrupted stop can leave stale latest/Airtable summary.
- Widget{} can yield no ring rows without a warning. Original three collection heartbeats omit native show/date; linker heartbeat includes them.
- Paused decoded business data/results/identities are retained, not refreshed. New UI-only entries can lack entity IDs; linker does not invent them.
- Three-minute checks do not promise whole-run completion. IgnoreNew/PT10M unchanged. Initial linker writes took longer; repeat link pass12seconds with0changes.
- Shows clock may lag; existing fallback remains. Native reciprocal appends are expected. Empty read then PATCH is not an atomic CAS guarantee against simultaneous edits.

Decoded trips remains the workflow design discussion. Cross-base publication/formerstage8 andold24hretention remain held. Approved delete-only cleanup stays active, including new-show deletion of owned show records; linking does not disable cleanup. Existing evidence/catalogs retained; no new cleanup archives.

Rollback: [reviewed code-only revert](evidence/linker-20261107/revert.ps1), authorized on failed rollout; not needed. Wait for normal task completion, protect intervening edits, retain schema/data/links, no repairs/manual reruns. [Prior lock](evidence/linker-20261107/previous-LOCK.md).
