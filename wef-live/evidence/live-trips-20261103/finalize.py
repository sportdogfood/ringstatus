from pathlib import Path
import json,datetime
r=Path(__file__).resolve().parent
a=json.loads((r/'normal-1500.json').read_text());b=json.loads((r/'normal-1503.json').read_text())
ta=json.loads((r/'task-1500.json').read_text(encoding='utf-8-sig'));tb=json.loads((r/'task-1503.json').read_text(encoding='utf-8-sig'))
assert a['result']==b['result']=='PASS' and ta['result']==tb['result']==0
assert '15:00:' in ta['last'] and '15:03:' in tb['last']
assert any('15:03:' in line and 'exit=0' in line for line in b['task_log'])
live=a['live_check'];assert not live['issues'] and not live['logging_errors'] and not live['stage_errors']
assert len(a['run_records']['heartbeat'])==len(a['run_records']['run-logs'])==1 and len(a['run_records']['errs-log'])==0
assert a['run_records']['heartbeat'][0]['fields']['result']=='Skipped' and a['run_records']['run-logs'][0]['fields']['result']=='Skipped'
assert a['cleanup']['actions']==b['cleanup']['actions']==[]
receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'deployment':'PASS:25installed JS hashes match final review;21existing JS/task unchanged','schema':'PASS:4newtables,8trip-native links,reciprocal results/messages/comments and4checked cleanup controls GET-read back','offline':'85PASS; author review,not independent','scheduled':'PASS:November3 15:00actual status check and15:03quiet dispatcher completed exit0','actual_check_run_id':live['run_id'],'heartbeat_record':a['run_records']['heartbeat'][0]['id'],'run_log_record':a['run_records']['run-logs'][0]['id'],'source_outcome':{'live_status':live['live_status'],'date':live['scope']['date'],'today':live['today'],'collect':live['collect'],'next_check':live['next_check_at']},'cleanup':'CorrectlySkipped,notdue/no show-day change;0actions,deletions,archives,errors','population':'UNVERIFIED:source live=false; current canonical day0Nov5 differs from calendarNov3. Newtables empty; no forced run/backfill. Existing scheduler rules unchanged.','remaining':['Natural populated trip/result/class/legacy native-link acceptance','Natural populated4newtable new-show deletion proof','Messages/comments authoring/delivery is downstream scope,not this collector'],'manual_runs':False,'record_repairs':False,'new_schedule_or_observer':False}
(r/'acceptance.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
(r/'release.md').write_text('''# Day-0 live-trips release — November3,2026

Four new tables and trip-specific links are deployed in wef-live. Decoded class trips are independent of UI row membership/detail availability; verified class source fallback retains native trip.class availability. Current live-entries data/writes stay intact.85offline checks PASS; author review PASS,not independent.25runtime hashes verified;21existing JS and Windows task unchanged.

Normal15:00actual status check and15:03quiet dispatch completed exit0. Status check0a8c4fcd-235a-485f-814a-a41f144acd87 returnedfalse, no issues/stage/logging errors; heartbeat/run-log Skipped/completed GET-read back. Cleanup correctly skipped with0actions;4newcheckedcontrols confirmed. No newgate/endpoint/schedule/observer,manualrun orrepair.

Source currentlyfalse; Shows priority0November5, calendarNovember3. Newtrip/result/message/comment tables are empty. Idle normal runs do not prove populated trip collection or native-link coverage. Those and future populated new-show deletion remain natural acceptance limits. No fake business data,manualbackfill orforcedsource/day used. Message/comment authoring/delivery remains downstream. Existing two conditional cleanup-reporting P2s and Stage8/old24hourholds remain.
''',encoding='utf-8')
note='**Normal scheduled verification:** November3 **3:00 and3:03p.m.Eastern** completed exit0. Actual status check returnedfalse, no errors; heartbeat/run-log Skipped/completed read back. Following quiet wake and cleanup correctly skipped; four cleanup controls confirmed. New tables are empty pending natural day0activity; populated trip/result/native-link and new-show deletion coverage remain unverified. [Exact acceptance](evidence/live-trips-20261103/acceptance.json).\n\n'
root=r.parents[1]
for name in ('README.md','scope.md','HANDOFF.md'):
 p=root/name;s=p.read_text(encoding='utf-8');marker='## Earlier dated documentation — historical where superseded above\n\n';s=s.replace(marker,note+marker,1);p.write_text(s,encoding='utf-8')
p=root/'LOCK.md';p.write_text(p.read_text(encoding='utf-8')+'\nCurrent release normal scheduled checks15:00/15:03Eastern verified. See [acceptance](evidence/live-trips-20261103/acceptance.json) for exact outcome and remaining natural populated limits.\n',encoding='utf-8')
print(json.dumps(receipt))
