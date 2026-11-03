from pathlib import Path
from datetime import datetime
r=Path(__file__).resolve().parent;root=r.parents[1]
now=datetime.now().astimezone().isoformat(timespec='seconds')
section=f'''# Current release — live-trips only

**Version 2026.11.03.4 · Updated {now}.** Owner correction removes unnecessary separate trip-result/message/comment tables. The simplified collector is deployed. General lock remains; scheduled operation continues.

- **live-trips** (`tblhUd5DucnpkEall`) remains the sole new trip table. Day-0 decoded class-source collection, keys, identities and existing native class/ring/group/schedule/legacy links are preserved.
- Results now stay **directly on each trip**: placing, position, results-verified, class-status and results-json. Original source-json retains all source facts. Changed-only saves continue.
- Separate **live-trip-results, live-trip-messages and live-trip-comments were empty and deleted**, with their three obsolete cleanup controls removed. Collector has no reads/writes/dependency on those tables. Airtable converted their former reciprocal columns to empty plain text; no broken table links remain.
- Downstream results/messages/comments can reference the source trip-key. No new local child tables or messaging/comment workflow. Existing live-entries remains untouched and continues its existing collection.
- **Cleanup:** live-trips stays in new-show deletion. New calendar day clears eligible completed heartbeat/run-logs/errs-log; every3hours checks1000/500; no separate show-day action. Active/current/diff logs, action history and references stay protected. Delete only, no new schedule/observer. Records supplies alerts.

**85 offline checks passed; author review completed, not independent review.**25installed runtime hashes match candidate;21other JS and Windows task unchanged. Direct result fields and empty table/config removal were GET-read back. No manual collector/provider runs, business-record repairs or backfill. Source currentlyfalse, canonical day0November5; populated verification remains natural, not forced.

[Review](evidence/live-trips-simple-20261103/review.md) · [Deployment](evidence/live-trips-simple-20261103/deployed.json) · [Schema readback](evidence/live-trips-simple-20261103/schema-readback.json). Normal scheduled proof recorded separately. Stage8/old24hourholds and known conditional cleanup-reporting P2s remain unchanged.

## Earlier dated documentation — historical where superseded above

'''
for name in ('README.md','scope.md','HANDOFF.md'):
 p=root/name;p.write_text(section+p.read_text(encoding='utf-8'),encoding='utf-8')
p=root/'AGENTS.md';p.write_text('''## Owner correction — November3,2026: live-trips only

Current build removes the three unnecessary empty live-trip-results/messages/comments tables. Results belong directly on live-trips; downstream messages/comments reference trip-key. No child-table reads/writes or cleanup targets remain; only live-trips is new-show-cleanup-owned. New result fields and removed schema/configuration verified in evidence/live-trips-simple-20261103/schema-readback.json. Current25JS baseline is deployed.json in that directory. Day0source/cadence,sourceidentity,legacy links,active/current logs and prior cleanup rules preserved. No manual business runs/repairs/new schedules/gates. General lock and unrelated holds remain. Older four-table descriptions below are superseded.

'''+p.read_text(encoding='utf-8'),encoding='utf-8')
(root/'LOCK.md').write_text('''# wef-live — current protected baseline

November3,2026, version2026.11.03.4. Owner corrected the trips build to remove unnecessary child tables. Current collector uses live-trips only, with direct result fields. [Runtime hashes](evidence/live-trips-simple-20261103/deployed.json), [schema](evidence/live-trips-simple-20261103/schema-readback.json), [scope](README.md).

Further changes require owner approval. Normal collection/cleanup/read-only checks/reviews continue.85offline checks passed; normal scheduled and natural populated proof remain separate. Source currentlyfalse. No manual runs/repairs/observers, task/cadence changes or permission changes. Existing holds/findings/history retained.
''',encoding='utf-8')
print('README/scope/handoff updated for single-table trips')
