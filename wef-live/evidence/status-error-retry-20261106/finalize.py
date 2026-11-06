from pathlib import Path
import json,datetime,hashlib
r=Path(__file__).resolve().parent
a=json.loads((r/'normal-0906.json').read_text());b=json.loads((r/'normal-0909.json').read_text());task=json.loads((r/'task-after-verification.json').read_text(encoding='utf-8-sig'))
assert a['installed_match'] and b['installed_match'] and task['result']==0 and '09:09:' in task['last']
for report in (a,b):
 assert report['status']['live_status'] is True and report['status']['minutes']==3
 assert all(x['ok'] and x['completed'] and not x['stage_errors'] and not x['logging_errors'] for x in report['lanes'])
 assert len(report['normal_run_logs'])==len(report['normal_heartbeats'])==3
 assert all(x['fields']['result'] in ['Success','Partial'] and x['fields']['completed'] for x in report['normal_run_logs']+report['normal_heartbeats'])
group=next(x for x in b['lanes'] if x['lane']=='groups');ring=next(x for x in b['lanes'] if x['lane']=='rings');classes=next(x for x in b['lanes'] if x['lane']=='classes')
assert group['archives'] and ring['archives'] and classes['archives']
assert all(x['source_shape']['detail_id'] and x['source_shape']['decoded_class'] for x in classes['archives'])
links=a['trip_link_readback'];assert links['trips']==links['unique_trip_keys']==links['correct_class_links']==links['result_json_present']==402
deployed=json.loads((r/'deployed.json').read_text());root=r.parents[1]
assert all(hashlib.sha256((root/x['file']).read_bytes()).hexdigest()==x['sha256'] for x in deployed['hashes'])
receipt={'verified_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'code':'PASS: activity.js/source.js exact owner User-Agent on four paths; live-cadence.js failed status3minutes,confirmedfalse60; only these3files changed across both approvals','offline':'12PASS including7header/proxy/URL/retry and5cadence/checkpoint/recovery checks; author review,not independent','normal_source_verification':'PASS:09:03status/groups/widget recovery and09:06/09:09class detail source snapshots; no manual source or collector calls','normal_runs':{'09:06':[{k:x[k] for k in ['lane','run_id','started','completed','ok','counts']} for x in a['lanes']],'09:09':[{k:x[k] for k in ['lane','run_id','started','completed','ok','counts']} for x in b['lanes']]},'live_counts':{'rings':ring['counts']['live-rings'],'groups':group['counts']['live-groups'],'class_appearances':group['counts']['live-class-appearances'],'classes':classes['counts']['live-classes'],'live_trips':classes['counts']['live-trips']},'trip_readback':links,'source_coverage':{'class_payloads':len(classes['archives']),'ui_rows':sum(x['source_shape']['detail_rows'] for x in classes['archives']),'decoded_trips':sum(x['source_shape']['decoded_trips'] for x in classes['archives'])},'remaining_record_warnings':{'classes':classes['issues'],'rings':ring['issues']},'limits':['Successful normal source parsing proves accepted2xx usable responses; numeric status200 was not separately instrumented','Results retain source values/verification state; source placeholders are not final results','No natural failed status was forced after repair;3minute failure/false-hourly behavior covered offline'],'preserved':['URLs/queryparameters','other request headers','Cloudflare proxy/default headers','shared reader/other collectors','Windows task/cadence definition','schemas/cleanup/logging behavior/checkpoints except normal runner writes'],'manual_runs':False,'record_repairs':False,'new_observers':False}
(r/'acceptance.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
(r/'release.md').write_text('''# Live header and failed-status retry release — November6,2026

Exact owner User-Agent applied to getLiveClassStatus,ListAjax,LiveScoreWidget,getLiveClassData only. Failed status rechecks3minutes; confirmedfalse remains hourly. Existing unavailable hourly checkpoint interpreted by normal runner using original checked_at+3minutes; no manual checkpoint change. Three modified module files across approvals:activity.js,source.js,live-cadence.js. All URLs/query/otherheaders,proxy/shared reader,othercollectors,task/schema/cleanup unchanged.

12offline checks and author review PASS,not independent review. Header deployment08:51 and retry correction09:02 installed/read back. Normal09:03status returnedtrue,groups/widget populated. Normal09:06and09:09 completed without stage/logging failure; actualtaskexit0.39normal class payload pairs saved and hashes validated.26groups/44appearances/6rings/39classes/402live-trips. All402trip keys unique,402correct native class links and402nonemptyresultJSON fields GET-verified. Initial missing-reference warnings cleared in repeatgroupslane.

Seven source UI-trip identities missing from decoded class data remain logged for classes200030984/200030985; two widgetcompletedformat warnings remain for rings3/7. UI source409rows vs decoded402trip rows; no forced matches or repairs. These warnings did not stop valid collection and were not changed in this header/retry scope. Source result placeholders are not final results. Normal parsing proves usable accepted2xx responses; exact numeric HTTP200not separately instrumented. No manual source/collector runs,newobservers or otherchanges.

Exact per-lane IDs,times,sourcehashpaths,Airtable logs,readback and limits: acceptance.json,normal-0903.json,normal-0906.json,normal-0909.json. Current25JS manifest is deployed.json; earlier header-only packet remains in ../user-agent-20261106 as dated evidence.
''',encoding='utf-8')
print(json.dumps({'verified':'PASS four normal direct feeds','counts':receipt['live_counts'],'trip_readback':links,'remaining_trip_identity_warnings':len(classes['issues']),'remaining_widget_warnings':len(ring['issues'])}))
