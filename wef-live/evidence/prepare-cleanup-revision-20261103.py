from pathlib import Path
import shutil
root=Path(__file__).resolve().parent.parent
out=root/'evidence'/'cleanup-revision-20261103'
out.mkdir(exist_ok=False)
for folder in ('baseline','candidate'):
 (out/folder).mkdir()
 for p in root.glob('*.js'):shutil.copy2(p,out/folder/p.name)
for name in ('lanes.test.js','cadence.test.js','adapter.test.js'):
 shutil.copy2(root/'evidence'/'delete-only-20261102'/name,out/name)
# Retain the unchanged historical task fixture used by the cadence suite.
for name in ('task-before.xml','task-prepared.xml'):
 shutil.copy2(root/'evidence'/'delete-only-20261102'/name,out/name)
p=out/'candidate'/'cleanup.js';s=p.read_text(encoding='utf-8')
s=s.replace("(trigger==='new-show'?Number(row.fields['show-id'])!==show:trigger==='quick-cleanup'&&LOGS.includes(table));", "(trigger==='new-show'||(['calendar-day','quick-cleanup'].includes(trigger)&&LOGS.includes(table)));")
s=s.replace("const protect=(t,r)=>LOGS.includes(t)&&protectedIds.has(r.fields['run-id']);", "const protect=(t,r)=>LOGS.includes(t)&&protectedRun(r,protectedIds);")
s=s.replace("  if(!run.new_show&&!run.new_day){", "  const lastQuick=Date.parse(previous.quick_checked_at||previous.completed);\n  run.quick_due=!Number.isFinite(lastQuick)||Date.parse(run.started)-lastQuick>=3*60*60*1000;\n  if(!run.new_show&&!run.new_day&&!run.quick_due){")
s=s.replace("run.reason='Same show and Eastern day'", "run.reason='No show/day change; three-hour log check not due'")
s=s.replace("  for(const t of LOGS.filter(t=>run.new_day&&selected.includes(t))){const rows=await api.list(t);", "  for(const t of LOGS.filter(t=>!run.new_show&&(run.new_day||run.quick_due)&&selected.includes(t))){const rows=await api.list(t);")
s=s.replace("   const targets=quickRows(t,rows.filter(r=>!protect(t,r)),rows.length);\n   quick.push(plan(t,'quick-cleanup',targets,rows.length));", "   const available=rows.filter(r=>completed(t,r)&&!protect(t,r));\n   const targets=run.new_day?available:quickRows(t,available,rows.length);\n   quick.push(plan(t,run.new_day?'calendar-day':'quick-cleanup',targets,rows.length));")
s=s.replace("write(stateFile,{show:run.show,day:run.day,run_id:run.run_id,completed:clock()});", "write(stateFile,{show:run.show,day:run.day,quick_checked_at:run.started,run_id:run.run_id,completed:clock()});")
s=s.replace("function quickRows(", "function protectedRun(row,runs){\n const id=row.fields?.['run-id'];\n return typeof id==='string'&&[...runs].some(run=>id===run||id.startsWith(run+'|'));\n}\nfunction quickRows(")
s=s.replace('quickRows,canonical};','quickRows,canonical,protectedRun};')
p.write_text(s,encoding='utf-8')
p=out/'candidate'/'airtable.js';s=p.read_text(encoding='utf-8')
s=s.replace("!['new-show','quick-cleanup'].includes(policy.trigger)", "!['new-show','calendar-day','quick-cleanup'].includes(policy.trigger)")
s=s.replace("policy.protectedRuns?.has(r.fields['run-id'])", "cleanup.protectedRun(r,policy.protectedRuns||new Set())")
p.write_text(s,encoding='utf-8')
(out/'TASK.md').write_text('''# Revised cleanup — November 3, 2026

Direct owner APPROVED TO EDIT authorizes this change, testing, deployment and normal scheduled verification, overriding only the matching locked cleanup behavior.

Baseline: locked November2 release,24 current JS and existing hidden wef-live-collector. Delta: cleanup.js and its Airtable deletion-policy boundary only. No task/cadence/schema/dependency change or manual run/repair. Existing hourly/three-minute Live checks continue; cleanup is evaluated by that same runner. No independent schedule. Records supplies alerts; existing errs-log/local failure receipts remain.

New Eastern calendar day clears all completed eligible rows from heartbeat/run-logs/errs-log, regardless of count. Otherwise every3 elapsed hours check those tables and, at1000+ total, delete oldest500 eligible completed rows. New show clears all rows in the five checked owned Live tables, regardless of show-id, plus eligible completed rows in the three logging tables. Active/current logs and diff children stay protected. New planning show-day has no separate trigger. New-show/day full clears subsume the threshold action and restart its3hour clock. Selection controls retain their existing checked-table scope. cleanup-logs and catalogs remain protected action history, not the three collection logging tables.

Delete only, metadata-only receipts; existing exact-value/ID/canonical pre-delete and absence readback remain. Stage8/old24h retention and all unrelated collectors unchanged. Existing two reporting P2 findings stay separate. No source/event replay, forced failure or stop clearing. Natural show/day/threshold branches are not claimed proved by unchanged-show runs.
''',encoding='utf-8')
print(out)
