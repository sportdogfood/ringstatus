from pathlib import Path
import json,datetime
r=Path(__file__).resolve().parent;m=r.parent.parent;runtime=m/'runtime';dep=json.loads((r/'deployed.json').read_text(encoding='utf-8-sig'));out={'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
for n in ['cleanup-state.json','cleanup-last-run.json','cleanup-stop.json']:
 p=runtime/n
 if not p.exists():out[n]=None;continue
 v=json.loads(p.read_text(encoding='utf-8-sig'));out[n]=v
 if n=='cleanup-last-run.json' and v.get('started','')>dep['deployed_at']:(r/('normal-'+v['run_id']+'.json')).write_text(json.dumps(v,indent=2),encoding='utf-8')
print(json.dumps(out,indent=2));(r/'latest-observation.json').write_text(json.dumps(out,indent=2),encoding='utf-8')
