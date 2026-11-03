from pathlib import Path
import json,re,datetime
r=Path(__file__).resolve().parent
a=json.loads((r/'normal-0906.json').read_text());b=json.loads((r/'normal-0909.json').read_text());task=json.loads((r/'task-0909.json').read_text(encoding='utf-8-sig'))
assert a['result']==b['result']=='PASS'
assert task['result']==0 and '09:09:' in task['last']
assert a['cleanup']['run_id']==b['cleanup']['run_id']
assert a['state']==b['state']
assert {x['id'] for x in a['records']['cleanup-logs']}=={x['id'] for x in b['records']['cleanup-logs']}
assert any('09:09:' in x and 'exit=0' in x for x in b['log'])
quiet=[json.loads(x) for x in b['log'] if x.startswith('{') and 'cleanup' in x]
assert quiet[-1]['cleanup']['result']=='Skipped'
accepted={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'normal_runs':'PASS: November3 09:06 and09:09 Eastern, unchanged existing task exit0','cleanup_run_id':a['cleanup']['run_id'],'first':'Three-hour due check completed; heartbeat231/run-logs498/errs-log1; below1000, three Airtable Skipped action records, zero deletions/archives','repeat':'Quiet scheduled run Skipped cleanup, unchanged checkpoint and same action IDs; no repeated inventory/action','source':'Live remained hourly/inactive; no manual source or collector invocation','offline':'75 PASS','installed':'24 candidate hashes match,21 unchanged JS, task unchanged','retained_limits':['Natural full calendar-day clear under revised rules not yet occurred','Natural full new-show clear under revised rules not yet occurred','Natural >=1000/500 actual deletion not yet occurred','Two preexisting conditional cleanup-reporting P2 findings unchanged'],'manual_runs':False,'repairs':False,'new_schedules':False}
(r/'acceptance.json').write_text(json.dumps(accepted,indent=2),encoding='utf-8')
(r/'release.md').write_text('''# Revised cleanup release — November3,2026

Approved delta deployed in the existing Live module/task.75offline checks and author review PASS; installed24JS hashes match,21JS unchanged. Task/action/account/cadence unchanged. Three changed runtime files:cleanup.js,airtable.js,lanes.js.

Normal09:06 and09:09Eastern invocations completed exit0. First run fd7c951f-df1a-476a-acea-3cf14c43ec2f performed due3hour inspection:231heartbeat/498run-logs/1errs-log, all below1000; three Skipped Airtable action readbacks and zero deletes/archives/errors. Repeat quiet run correctly skipped cleanup and preserved checkpoint/action IDs. No stop, forced event, manual run, record repair or new schedule.

New calendar-day fullclear, new-show fullclear and actual500delete remain natural operational limits, with offline coverage; unchanged-day checks do not prove those branches. Existing two conditional cleanup-reporting P2s remain separate. Records supplies alerts, without a new alert implementation here. Permanent cleanup-logs/catalogs/references retained; general lock still applies.
''',encoding='utf-8')
note='**Normal scheduled verification:** November3 **9:06 and9:09a.m.Eastern** completed exit0. Due three-hour check logged three below-threshold Skips (heartbeat231/run-logs498/errs-log1), zero deletions/archives/errors; repeat wake skipped cleanup without changing its checkpoint or action IDs. [Acceptance](evidence/cleanup-revision-20261103/acceptance.json). Natural new-calendar/new-show full clears and actual500deletion remain unverified operational branches, covered offline.\n\n'
for name in ('README.md','scope.md','HANDOFF.md'):
 p=r.parents[1]/name;s=p.read_text(encoding='utf-8');s=s.replace('## Earlier dated documentation — historical where superseded above\n\n',note+'## Earlier dated documentation — historical where superseded above\n\n',1);p.write_text(s,encoding='utf-8')
print(json.dumps(accepted))
