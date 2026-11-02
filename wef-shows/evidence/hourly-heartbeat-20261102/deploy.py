"""Prepared deployment only. Requires a separate owner deployment approval."""
import pathlib,json,hashlib,os,datetime
E=pathlib.Path(__file__).resolve().parent
R=E.parents[2]
S=R/'wef-shows/staged/hourly-heartbeat-20261102'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
approval=E/'deployment-approval.json'
assert approval.exists(),'Missing explicit deployment approval; no changes made'
assert json.loads(approval.read_text())['approved'] is True
review=E/'telemetry-review'
assert json.loads((review/'review-verifier.json').read_text())['status']=='PASS'
receipt=json.loads((review/'review-receipt.json').read_text())
assert receipt['candidate_status']=='PASS' and receipt['review_status']=='COMPLETE'
m=json.loads((E/'manifest.json').read_text())
lock=R/'wef-shows/runtime/collector.lock'
try:
 with lock.open('x') as h:h.write(str(os.getpid()))
except FileExistsError:
 print(json.dumps({'deployed':False,'reason':'Existing collector active; no lock clearing'}));raise SystemExit(0)
try:
 for i in m:
  p=R/i['path'];assert (sha(p) if p.exists() else None)==i['before'],i['path']
  assert sha(S/i['path'])==i['sha256'],i['path']
 for p,h in json.loads((E/'protected-hashes.json').read_text()).items():assert sha(R/p)==h,p
 # Load new policy support before the collector that calls it.
 for i in sorted(m,key=lambda x:x['path'].endswith('/collector.js')):
  p=R/i['path'];p.parent.mkdir(parents=True,exist_ok=True);temp=p.with_name(p.name+'.hourly-deploy.tmp')
  temp.write_bytes((S/i['path']).read_bytes());os.replace(temp,p);assert sha(p)==i['sha256']
 receipt={'deployed':True,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':m,'manual_runs':0,'task_changes':0,'record_repairs':0,'stops_cleared':0}
 (E/'deployment.json').write_text(json.dumps(receipt,indent=2));print(json.dumps({'deployed':True,'at':receipt['at']}))
finally:lock.unlink()
