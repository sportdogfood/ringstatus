from pathlib import Path
from datetime import datetime
root=Path(__file__).resolve().parents[2]
now=datetime.now().astimezone().isoformat(timespec='seconds')
section=f'''# Current Live release — delete only

Version **2026.11.02.12** · Updated **{now}**.

The owner explicitly approved corrections and redeployment. Installed source hashes and existing task definition were read back successfully. This supersedes prior archive-era cleanup and pending-deployment statements below; old dated evidence remains history.

- **New show:** delete nonmatching-show records in checked owned tables. No new cleanup archives or deleted-field backups.
- **New Eastern calendar day only:** check heartbeat/run-logs/errs-log. At1,000+ records delete the oldest500 completed eligible rows; protect active/current invocation rows. Same show/day performs no cleanup table inventory or deletion.
- **Logs:** cleanup-logs records table/action, start, completion, result, show IDs, deleted count and error. Old archive fields remain unused/zero for new actions. Local cleanup receipts contain action/count metadata only. Existing historical archives/catalogs are left untouched.
- **Cadence:** actual live-status checks and registered runs are hourly around the clock when live=false, priority-0 date differs from today, or outside05:00–19:00Eastern. True/date mismatch adds an errs-log warning. True/today inside that window collects every3minutes. The existing hidden task wakes every3minutes24h as a local dispatcher; not-due wakes do not call providers or Airtable and do not create collection logs.
- **Protected behavior:** independent lanes, IDs/links, changed-only saves, exact pre-delete target/value checks, canonical-show recheck, DELETE response/absence readback, active records and permanent catalogs. Stage8 publication and old24h retention remain disabled. Other collectors untouched.

**Verification:**81 offline checks PASS; author review and review-packet verification PASS, not independent review. All24 installed JS hashes match the reviewed candidate;20 existing JS files unchanged. Windows normalized task duration PT24H toP1D and omitted default StopAtDurationEnd=false; semantic readback matches. No manual run, record repair or observer. Scheduled proof is recorded separately in evidence/delete-only-20261102/acceptance.json when available. No claim that future new-show/new-day deletion or populated activity has already occurred. Two known conditional cleanup-reporting P2 findings remain documented in the review.

Evidence: [release task](evidence/delete-only-20261102/TASK.md), [review](evidence/delete-only-20261102/review.md), [installed readback](evidence/delete-only-20261102/deployed.json), [tests](evidence/delete-only-20261102/offline-final.json).

## Prior dated documentation — historical only

'''
for name in ('README.md','scope.md','HANDOFF.md'):
 p=root/name
 p.write_text(section+p.read_text(encoding='utf-8-sig'),encoding='utf-8')
p=root/'AGENTS.md'
p.write_text(p.read_text()+f'''
## Owner correction and deployment — {now}

Latest direct human approval supersedes archive-before-delete and pending-deployment notes for cleanup: DELETE ONLY. New show deletes nonmatching-show rows; new Eastern day alone applies1000/500 to heartbeat/run-logs/errs-log; unchanged show/day does not inventory logs. No new cleanup archive files or deleted-record-field backups. Preserve existing historical files/catalogs. Cadence correction and delete-only cleanup are deployed; exact source/task readback in evidence/delete-only-20261102/deployed.json, normal proof separately recorded. Stage8/old24h retention remain disabled; no manual runs, repairs or observers. Existing two reporting P2s remain separate. Do not revive superseded archive requirements from older instructions.
''')
print('README, scope, handoff and module instruction correction saved')
