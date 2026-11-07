from pathlib import Path
import json,hashlib,datetime
root=Path('wef-live');r=root/'evidence/linker-20261107'
read=lambda p:json.loads(p.read_text(encoding='utf-8-sig'))
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
now=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-5))).isoformat(timespec='seconds')
version='2026.11.07.4';manifest=read(r/'deployed.json');accepted=read(r/'acceptance.json')
assert all(sha(root/x['file'])==x['sha256'] for x in manifest['hashes'])
assert accepted['result'].startswith('PASS:')
prefix=f'''# Current release — empty-only linker

**Version {version} · Locked {now} · America/New_York.** Owner-approved linker deployed and verified; unrelated work stays locked.

- **Stages 4–6 stay unchanged.** Groups, rings and live class details collect/save as before. Status uses Shows' published Eastern time and priority-0 date. Source URLs, headers, three-minute/hourly rules, task and cleanup remain unchanged.
- **Stage 7: `linker.js` runs after collection finishes.** It fills empty native fields only: `show` → synced **wef-shows.show**, `show-day` → synced **wef-shows.show-days**, and `data-schedules` → synced **wef-data.schedules** on all five Live tables. Fifteen missing fields were added; no records were manually written. Existing internal forward links are reused. Airtable maintains reciprocal links automatically. Populated forward fields are not audited, corrected or rebuilt. Old WEF Schedules links stay retained; the linker never reads/writes that target.
- **Exact saved keys only.** Use show/day/ring/group/class/trip IDs and Data's explicit ring/group/class keys, with source ring-ID mapped to ring-number. No names, invented IDs or forced joins. Existing Dropped-schedule exclusion stays. Missing/ambiguous matches remain empty and are individually logged; other links/collection continue. No new readiness gates, tasks or schedules.
- **Decoded-trip requests remain paused.** Empty native links on existing trips can be filled by the approved linker, but decoded identities, results, membership states and other business values remain frozen. No new decoded runner or automatic resume.
- **`completed:null` is handled as an empty list.** Original source evidence, ring record and other links remain. No null-format error, payload correction or waiting condition. Natural null occurrence is not claimed.

**Normal verification:** November 7 **09:51 and 09:57 Eastern** runs passed without stage/logging failures. Both collected35groups/71appearances,7rings,66classes and408live-detail rows; decoded requests0. First linker filled **2,897 empty fields on968 existing records**:968show,968day and961Data-schedule fields. All968priorrecords and2,300populatedforwardfields were retained. All423pausedtrip business records remained unchanged. The second linker wrote **zero fields/records**. Native heartbeat/run-log and one-per-skip errs-log readback confirmed both runs.17offlinechecks and author code review passed (not independent review).

**Seven known skips:** only Dropped Data schedule occurrences match retained groups **200028530 /200028533**, class **200031158**, and its two trips/twoentries. Seven `data-schedules` fields remain empty; show/day and existing native links remain. Each skip is logged, not guessed or repaired. These are not collection stops.

**Revert:** no rollout failure occurred. Reviewed [revert.ps1](evidence/linker-20261107/revert.ps1) restores only the three prior changed JS files and removes the new linker after the normal task finishes; intervening owner edits are protected. Retain added schema/data/valid links, never repair records or manually rerun. No new cleanup archives, observers, dependencies or runtime upgrades.

Evidence: [acceptance](evidence/linker-20261107/acceptance.json), [review](evidence/linker-20261107/review.md), [17offlinechecks](evidence/linker-20261107/offline.json), [deployed26JS](evidence/linker-20261107/deployed.json), [native field targets](evidence/linker-20261107/schema-readback.json), [locked baseline](evidence/linker-20261107/locked-baseline.json). Existing known-trouble/hold notes below remain; older releases are historical where superseded here.

'''
for name in ['README.md','scope.md','HANDOFF.md']:
 p=root/name;s=p.read_text(encoding='utf-8')
 if not s.startswith('# Current release — empty-only linker'):p.write_text(prefix+s,encoding='utf-8')
p=root/'README.md';s=p.read_text(encoding='utf-8')
if '| Linker unmatched schedules |' not in s:s=s.replace('| Three-minute interval |','| Linker unmatched schedules | Seven retained records have only Dropped Data matches; Data links stay empty and skips are logged. Shows/day links and collection continue. | Known skips; no guessing |\n| Three-minute interval |',1)
s=s.replace('Collection-lane heartbeat writes do not populate native show-id/date cells.','The original three collection-lane heartbeat writes do not populate native show-id/date cells; the linker heartbeat does.',1)
p.write_text(s,encoding='utf-8')
old=(root/'LOCK.md').read_text(encoding='utf-8')
if not (r/'previous-LOCK.md').exists():(r/'previous-LOCK.md').write_text(old,encoding='utf-8')
lock=f'''# WEF Live — locked protected baseline

**Version {version} · Locked {now} · America/New_York.**

Empty-only linker deployed after the three unchanged collection lanes. Five Live tables link to existing local synced wef-shows.show, wef-shows.show-days and wef-data.schedules. Internal forward fields reused; native reciprocal appends are automatic. Populated links are not audited or rewritten. Old WEF Schedules links retained but not used for new links. Decoded requests stay paused; only approved empty native links on retained trips are filled.

Further code/schema/routing/cadence/cleanup/deployment/resume changes need owner approval. Normal scheduled operations and authorized read-only checks continue. This is a documented lock, not an ACL/task/process change. No manual collector/provider runs or record repairs.

Protected26JS/sharedreader/task and document hashes: [locked baseline](evidence/linker-20261107/locked-baseline.json). [Current README](README.md) and [deployed manifest](evidence/linker-20261107/deployed.json).

## Verification

17offlinechecks and author review passed; not independent review. Normal09:51/09:57runs verified2,897emptyfills on968existing records,2,300populatedforwardfields retained, then zero repeat writes.423trip business rows frozen; decoded calls0. Native parent logs/heartbeats and7individual skip logs read back perrun. [Exact IDs/evidence](evidence/linker-20261107/acceptance.json). `completed:null` handled and tested; no payload correction or natural-null waiting condition.

## Known trouble and limits

- Seven Data links stay blank: only Dropped schedule occurrences match groups200028530/200028533,class200031158 and its two trips/twoentries. Logged individually; no forced joins.
- Immediate Records chat alert transport is not configured; local/Airtable logs are not chat delivery.
- Two conditional cleanup-reporting P2s remain: unexecuted plans can stay Running; interrupted stop can leave stale latest/Airtable summary.
- Widget{{}} can yield no ring rows without a warning. Original three collection heartbeats omit native show/date; linker heartbeat includes them.
- Paused decoded business data/results/identities are retained, not refreshed. New UI-only entries can lack entity IDs; linker does not invent them.
- Three-minute checks do not promise whole-run completion. IgnoreNew/PT10M unchanged. Initial linker writes took longer; repeat link pass12seconds with0changes.
- Shows clock may lag; existing fallback remains. Native reciprocal appends are expected. Empty read then PATCH is not an atomic CAS guarantee against simultaneous edits.

Decoded trips remains the workflow design discussion. Cross-base publication/formerstage8 andold24hretention remain held. Approved delete-only cleanup stays active, including new-show deletion of owned show records; linking does not disable cleanup. Existing evidence/catalogs retained; no new cleanup archives.

Rollback: [reviewed code-only revert](evidence/linker-20261107/revert.ps1), authorized on failed rollout; not needed. Wait for normal task completion, protect intervening edits, retain schema/data/links, no repairs/manual reruns. [Prior lock](evidence/linker-20261107/previous-LOCK.md).
'''
(root/'LOCK.md').write_text(lock,encoding='utf-8')
base={'version':version,'locked_at':now,'runtime_manifest':'deployed.json','runtime_hashes':manifest['hashes'],'shared_reader':{'path':'../../../lib/wef-source-read.js','sha256':sha(root.parent/'lib/wef-source-read.js')},'task_snapshot':{'path':'task-before.xml','sha256':sha(r/'task-before.xml')},'documents':[{'file':n,'sha256':sha(root/n)} for n in ['README.md','LOCK.md','scope.md','HANDOFF.md']],'approval':'Empty-only linker and revert on failure;unchangedstages4-6/decodedpause','manual_runs':0,'record_repairs':0,'known_skipped_data_links':7,'acceptance':'acceptance.json'}
(r/'locked-baseline.json').write_text(json.dumps(base,indent=2),encoding='utf-8')
print(json.dumps({'version':version,'locked_at':now,'runtime_files':len(manifest['hashes']),'retained_records':accepted['prior_records_retained'],'empty_fields_filled':accepted['empty_fields_filled_readback']}))
