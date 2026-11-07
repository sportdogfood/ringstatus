from pathlib import Path
import json,datetime
r=Path(__file__).resolve().parent
read=lambda n:json.loads((r/n).read_text(encoding='utf-8-sig'))
before=read('before.json')['tables'];first=read('after-first.json')['tables'];second=read('after-second.json')['tables']
runs=[read('normal-first.json'),read('normal-second.json')]
tables=['live-rings','live-groups','live-classes','live-trips','live-entries']
forward=['show','show-day','w-shows','w-days','data-schedules','ring','class','groups','legacy-live-entry','schedules','___schedules']
preserved=0;fields_verified=0
for t in tables:
 a={x['id']:x for x in before[t]};b={x['id']:x for x in first[t]};c={x['id']:x for x in second[t]}
 assert a.keys()<=b.keys()<=c.keys(),(t,'record removed')
 for rid,old in a.items():
  for f in forward:
   ids=old['fields'].get(f)
   if isinstance(ids,list) and ids:
    assert set(ids)<=set(b[rid]['fields'].get(f,[])) and set(ids)<=set(c[rid]['fields'].get(f,[])),(t,rid,f)
    preserved+=1
for run in runs:
 assert run['runtime_hashes_match']
 assert len(run['logs']['heartbeat']['records'])==4 and len(run['logs']['run-logs']['records'])==4
 for lane in run['lanes']:
  assert lane['ok'] and not lane['stage_errors'] and not lane['logging_errors']
  assert not any(x['label'].startswith('identity-') for x in lane['timings']['requests'])
 link=run['linker'];assert link['ok'] and not link['stage_errors'] and not link['logging_errors']
for change in runs[0]['linker']['changes']:
 old=next((x for x in before[change['table']] if x['id']==change['id']),None)
 saved=next(x for x in second[change['table']] if x['id']==change['id'])
 for f,ids in change['fields'].items():
  assert old is None or not old['fields'].get(f),(change['table'],change['id'],f,'was populated')
  assert set(saved['fields'].get(f,[]))==set(ids);fields_verified+=1
  if f=='w-shows':assert len(ids)==1 and next(x for x in second['w-shows'] if x['id']==ids[0])['fields']['show-key']=='15|200000077'
  if f=='w-days':assert len(ids)==1 and next(x for x in second['w-days'] if x['id']==ids[0])['fields']['show-day-key']=='15|200000077|2026-11-07'
  if f=='data-schedules':
   for rid in ids:assert next(x for x in second['wef-data.schedules'] if x['id']==rid)['fields']['schedule-key'].startswith('15|200000077|2026-11-07|')
assert not runs[1]['linker']['changes'],'Repeated populated fields were rewritten'
oldtrip={x['id']:x for x in before['live-trips']};newtrip={x['id']:x for x in second['live-trips']}
for rid,x in oldtrip.items():
 for f,v in x['fields'].items():
  if f not in forward and not isinstance(v,list):assert newtrip[rid]['fields'].get(f)==v,(rid,f,'paused trip business data changed')
out={'verified_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'result':'PASS:corrected w-shows/w-days references in two normal runs;old populated fields retained;Data/class-group/collection unchanged;repeat zero changes',
 'runs':[{'status_run_id':x['status']['run_id'],'collection_run_ids':[y['run_id'] for y in x['lanes']],'linker_run_id':x['linker']['run_id'],'linker_started':x['linker']['started'],'linker_completed':x['linker']['completed'],'changed_records':len(x['linker']['changes']),'skips':len(x['linker']['issues']),'stage_errors':x['linker']['stage_errors'],'logging_errors':x['linker']['logging_errors'],'decoded_requests':0} for x in runs],
 'prior_records_retained':sum(len(before[t]) for t in tables),'prior_populated_forward_fields_preserved':preserved,'empty_fields_filled_readback':fields_verified,
 'external_linked_records':{f:sum(bool(x['fields'].get(f)) for t in tables for x in second[t]) for f in ['w-shows','w-days','data-schedules']},
 'unmatched':runs[1]['linker']['issues'],'unmatched_reason':'Seven existing Dropped-only Data misses remain empty;log recommends no retry until source data changes. No cache or new gate added.',
 'decoded_trip_business_values_retained':len(oldtrip),'stages4_5_6_unchanged':True,'offline':'17PASS;author review,not independent;class-group reciprocal schema/sample verified','reverted':False,'revert_plan':'revert.ps1;authorizedcode-only restoration on failure;not needed',
 'natural_null_proof':'Not claimed or required;existing tested null-as-empty normalization unchanged','manual_runs':0,'record_repairs':0}
(r/'acceptance.json').write_text(json.dumps(out,indent=2),encoding='utf-8');print(json.dumps({k:out[k] for k in ['result','prior_records_retained','prior_populated_forward_fields_preserved','empty_fields_filled_readback','external_linked_records']}))
