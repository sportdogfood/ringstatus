from pathlib import Path
import json,datetime,hashlib
r=Path(__file__).resolve().parent;m=r.parent.parent;runtime=m/'runtime';dep=json.loads((r/'deployed.json').read_text(encoding='utf-8-sig'))
out={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'deployed_at':dep['deployed_at'],'manual_runs':False,'receipts':{}}
for f in ['live-check-last-run.json','lane-groups-last-run.json','lane-rings-last-run.json','lane-classes-last-run.json','cleanup-last-run.json']:
 p=runtime/f;v=json.loads(p.read_text(encoding='utf-8-sig'));out['receipts'][f]={k:v.get(k) for k in ['run_id','started','completed','ok','issues','stage_errors','logging_errors','scope','live_status','collect','reason','minutes','next_check_at','counts']}
 if v.get('started','')>dep['deployed_at'] and f=='live-check-last-run.json':
  (r/('normal-status-'+v['run_id']+'.json')).write_text(json.dumps(v,indent=2))
print(json.dumps(out,indent=2));(r/'normal-observation.json').write_text(json.dumps(out,indent=2))
