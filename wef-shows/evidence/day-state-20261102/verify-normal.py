import pathlib,json,hashlib,subprocess,datetime
R=pathlib.Path(__file__).resolve().parents[3]
E=pathlib.Path(__file__).resolve().parent
report=json.loads((R/'wef-shows/runtime/last-run.json').read_text())
deploy=json.loads((E/'deployment.json').read_text())
assert report['started']>deploy['at'],'No completed post-deployment normal run yet'
assert report['ok'] and not report['errors'] and not report['telemetry_errors'],report
assert report['show_id']==200000077 and report['selected_date']=='2026-11-05',report
code="""const fs=require('node:fs');const report=JSON.parse(fs.readFileSync('wef-shows/runtime/last-run.json'));const api=require('./wef-shows/airtable').airtable();Promise.all([api.list('show-days'),api.list('heartbeat'),api.list('run-logs',undefined,'{run-id}='+JSON.stringify(report.run_id))]).then(([days,heartbeat,logs])=>console.log(JSON.stringify({days,heartbeat,logs}))).catch(e=>{console.error(e.message);process.exitCode=1;});"""
p=subprocess.run(['node','--env-file=wef-shows/runtime/collector.env','-e',code],cwd=R,text=True,capture_output=True)
assert p.returncode==0,p.stderr
data=json.loads(p.stdout)
before=json.loads((E/'before-show-days.json').read_text())
assert {r['id'] for r in data['days']}=={r['id'] for r in before}
allowed={'fldbK4VEp3aNOdJlB','fldXVyc7NrEl6xWzx','fldMtRYYUQwljw7fx'}
before_schema={f['name']:f['id'] for f in json.loads((E/'schema.json').read_text())['fields']}
after_schema={f['name']:f['id'] for f in json.loads((E/'schema-after.json').read_text())['fields']}
def stable(fields,schema):return {schema.get(k,k):v for k,v in fields.items() if schema.get(k,k) not in allowed}
for row in data['days']:
 f=row['fields'];old=next(r['fields'] for r in before if r['id']==row['id'])
 assert stable(f,after_schema)==stable(old,before_schema),row['id']
 active=f['is_default (from show)']==[True]
 assert f['last_checked_status']==('Active' if active else 'Not active'),row
 assert f['day-priority']==((datetime.date.fromisoformat(f['date'])-datetime.date(2026,11,5)).days if active else 999),row
 assert report['started']<=f['last_checked']<=report['completed'] or report['day_priority']['status']=='Skipped',row
 assert datetime.datetime.fromisoformat(f['last_checked'].replace('Z','+00:00')).tzinfo is not None
assert len(data['logs'])==1 and data['logs'][0]['fields']['result']=='Success',data['logs']
assert data['heartbeat'][0]['fields']['result']=='Success'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for item in json.loads((E/'manifest.json').read_text()):assert sha(R/item['path'])==item['sha256'],item['path']
for name,h in json.loads((E/'protected-hashes.json').read_text()).items():assert sha(R/name)==h,name
(E/('normal-'+report['run_id']+'.json')).write_text(json.dumps({'report':report,'readback':data,'verified_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':'PASS: status, priority, ISO timestamp, heartbeat/run-log, protected data and hashes'},indent=2))
print(json.dumps({'run':report['run_id'],'completed':report['completed'],'phase':report['day_priority'],'verified_rows':len(data['days'])}))
