from pathlib import Path
from datetime import datetime
root=Path(__file__).resolve().parents[2]
now=datetime.now().astimezone().isoformat(timespec='seconds')
section=f'''# Current approved cleanup revision

**Version2026.11.03.1 · Updated{now}.** Owner-approved cleanup exception to the November2lock is deployed. Installed24JS hashes read back; only cleanup.js, airtable.js/retireCleanup and lanes.js's local cleanup-due evaluation changed. Existing Windows task, source polling cadence, schemas and other collectors are unchanged. Baseline and review: [release](evidence/cleanup-revision-20261103/TASK.md), [readback](evidence/cleanup-revision-20261103/deployed.json), [review](evidence/cleanup-revision-20261103/review.md).75offline checks PASS; author review, not independent.

- **New Eastern calendar day:** clear all completed eligible records from heartbeat/run-logs/errs-log at any count.
- **Every3hours:** inspect those three tables; at1000+ total records delete oldest500eligible completed records. Full calendar/show clears restart the3hour interval.
- **New show:** clear all records in checked owned live-entries/live-class-appearances/live-classes/live-groups/live-rings, regardless of show-id, and eligible completed records in the three logging tables.
- **New planning show-day:** no separate cleanup action.
- Protect incomplete/Running logs, current invocation and its diff children. Preserve checked-table controls, permanent cleanup-logs/action history, catalogs and references. **Delete only; no new cleanup archives or deleted-field backups.** Existing archives remain untouched.
- Same registered task evaluates local cleanup due state on quiet three-minute wakes. It does no provider/Airtable work or cleanup receipt write until due. No separate task/schedule/observer. Records supplies alerts; existing errs-log and local failure/stop evidence remain.

The general lock stays in force. Stage8publication and old24hretention remain disabled. Known two conditional cleanup-reporting P2s are unchanged. Normal scheduled proof and naturally occurring deletion branches are recorded separately; no manual runs, forced events or record repairs.

## Earlier dated documentation — historical where superseded above

'''
for name in ('README.md','scope.md','HANDOFF.md'):
 p=root/name;p.write_text(section+p.read_text(encoding='utf-8'),encoding='utf-8')
p=root/'LOCK.md';p.write_text('''# wef-live — current locked baseline

Updated November3,2026. General code/workflow freeze remains. The owner explicitly approved the revised cleanup, tests, deployment and normal scheduled verification in this chat. This is the only current exception.

Current runtime: [November3 deployment hashes](evidence/cleanup-revision-20261103/deployed.json); original November2lock evidence remains historical. Existing task/action/account/cadence unchanged. No file/Airtable permission changes. Further code/schema/cadence/deployment changes require owner approval. Read-only checks and existing reviews remain permitted; normal collection and revised delete-only cleanup continue.

Current cleanup rules and exact scope: [README](README.md), [task](evidence/cleanup-revision-20261103/TASK.md).75offline checks PASS. Normal scheduled outcome and natural calendar-clear/new-show-clear/500-delete proof remain distinct. Two preexisting conditional cleanup-reporting P2 findings remain documented, not claimed fixed.
''',encoding='utf-8')
p=root/'AGENTS.md';p.write_text('''## Current owner-approved exception — November3,2026

Owner explicitly approved revised cleanup/test/deploy/normal verification, overriding only the older locked cleanup rules. Runtime hashes and task readback: evidence/cleanup-revision-20261103/deployed.json. New calendar day clears eligible completed heartbeat/run-logs/errs-log; every3hours checks1000/500; new show clears all checked owned show rows and eligible completed logs, even matching new-show-ID rows. No separate planning-day action. Protect active/current/diff logs. Delete only. cleanup-logs/catalogs/references retained. Same existing task evaluates local due state on quiet wakes; no extra schedule or observer. Records supplies alerts. General lock/holds and manual-run/repair/failure boundaries remain. No other collector changes.

'''+p.read_text(encoding='utf-8',errors='replace'),encoding='utf-8')
print('Current docs/lock updated; historical evidence preserved')
