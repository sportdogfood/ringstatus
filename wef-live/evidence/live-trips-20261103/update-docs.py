from pathlib import Path
from datetime import datetime
r=Path(__file__).resolve().parent;root=r.parents[1]
now=datetime.now().astimezone().isoformat(timespec='seconds')
section=f'''# Current release — day-0 live-trips

**Version 2026.11.03.3 · Updated {now}.** Owner-approved addition deployed within wef-live. General lock remains; this approval changes only the documented trips/result/message/comment structure and its existing new-show cleanup integration.

## Collection

The existing classes lane projects **live-trips** from the decoded `/classes/{{class-id}}?show_id=...&customer_id=15&cgid=-1` response already fetched through `wef-schedules-decoded.gombcg.workers.dev`. No new endpoints, tasks, helpers, count/status/age gates or reverse Entries reads. UI live details supplement matching trips; unavailable/empty UI details do not suppress decoded source trips. Individual errors are logged and valid trips continue. Empty collections succeed. Existing Shows day-priority **0**, live-status/date cadence, three independent lanes and logging remain.

Each trip records show/day/class/ring and source trip ID/UUID, show-level entry-key, horse/rider/trainer/owner IDs and names, order/actual order, scratch/gone and source evidence. Missing source identities stay missing. Composite `trip-key` follows `customer|show|date|ring|class|entry|UUID` with numeric trip-ID fallback. `entry-key` is `show|entry`, allowing many class trips/riders across one horse's show entry. Existing **live-entries** records, keys, links and collection writes remain; retirement is not part of this release.

## Tables and links

| Table | Airtable table ID | Purpose |
| --- | --- | --- |
| live-trips | tblhUd5DucnpkEall | Individual day-0 source trips; native class/ring/group/available schedule and exact legacy-record links |
| live-trip-results | tblPIZYbHp8850uoA | Each trip's source result facts, scores/times/faults/placing and verification state |
| live-trip-messages | tblUbF9tygLGChbM4 | Messages directly related to an individual trip |
| live-trip-comments | tblg29QWHi5GB8p4X | Comments directly related to an individual trip |

Child tables link directly to their trip; parent reciprocal fields are **results, messages, comments**. Message/comment authoring and delivery belong to downstream owners; this collector does not invent or send them. Entity/show-entry source IDs/keys allow downstream linking without a new Live dependency on helper/Entries bases. Source show/date are direct fields; existing class/schedule links provide related context.

## Cleanup and protection

The four new tables are checked in existing **cleanup** controls and included in new-show deletion, child tables before trips. **New show clears all checked owned show records.** New calendar day clears eligible completed heartbeat/run-logs/errs-log; every 3 hours checks those log tables and deletes oldest500eligible at1000+. No separate new-show-day cleanup. Active/current/diff logs, permanent action history and reference/catalog tables remain protected. Delete only; no cleanup archives, new schedule or observer. Records supplies alerts. Existing source-evidence storage is unchanged.

## Verification

**85 offline checks PASS; author review PASS, not independent review.** Four table schemas,8trip links/reciprocals and4cleanup control rows were GET-read back. Installed25JS hashes match candidate;21existing JS and the entire scheduled task are unchanged. [Review](evidence/live-trips-20261103/review.md) · [Schema readback](evidence/live-trips-20261103/schema-readback.json) · [Deployment](evidence/live-trips-20261103/deployed.json).

Normal scheduled proof is recorded separately as available. Source currently reports live=false and Shows day0isNovember5; idle checks do **not** prove populated trip/result/link coverage. No manual runs, forced source/date, historical backfill or record repairs. New-show cleanup with populated new tables also awaits a natural event. Stage8publication and old24hretention remain held; existing two conditional cleanup-reporting P2s remain separate.

## Earlier dated documentation — historical where superseded above

'''
for name in ('README.md','scope.md','HANDOFF.md'):
 p=root/name;p.write_text(section+p.read_text(encoding='utf-8'),encoding='utf-8')
p=root/'AGENTS.md';p.write_text('''## Current owner-approved trips addition — November3,2026

Explicit human APPROVED TO EDIT covers the agreed day0 live-trips build, own results/messages/comments relationships, preserving existing live-entries and integrating the same new-show cleanup. Current approved delta/runtime evidence: evidence/live-trips-20261103/{TASK.md,deployed.json,review.md,schema-readback.json}. No new collection gates, endpoint/task/cadence, provider/manual test runs, record repairs or extra bases. Live projects decoded class trips independently of UI availability. Entity/show-entry source IDs remain direct facts; no new reverse Entries/Data/helper dependency. Four new tables are live-trips/live-trip-results/live-trip-messages/live-trip-comments. General lock/unrelated holds remain. Natural populated acceptance remains distinct from idle normal checks. Do not revive old cleanup/Entries/CWF assumptions from historical sections.

'''+p.read_text(encoding='utf-8'),encoding='utf-8')
(root/'LOCK.md').write_text('''# wef-live — protected baseline

Updated November3,2026, version2026.11.03.3. The owner approved the day0trips build as a scoped exception; that candidate is deployed. Further unrelated code/schema/cadence changes still require explicit owner approval. Normal collection, revised delete-only cleanup, read-only checks and established reviews continue.

Current25JS manifest and unchanged task: [deployment](evidence/live-trips-20261103/deployed.json). Scope and four new tables: [README](README.md), [task](evidence/live-trips-20261103/TASK.md).85offline checks and author review passed; schema/configuration GET-readback passed. Normal scheduled and natural populated acceptance are recorded separately. Do not infer populated proof from live=false checks. No retirement of live-entries, no reverse Entries/Data/helper reads, no new gates/schedule/observer.

Original lock snapshots and historical evidence remain preserved. This documentation is not a file or Airtable permission change.
''',encoding='utf-8')
print('README/scope/handoff and current baseline updated')
