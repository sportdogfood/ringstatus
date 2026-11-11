import pathlib,json,hashlib,os,datetime
E=pathlib.Path(__file__).resolve().parent
R=E.parents[2]
S=R/'wef-shows/staged/browser-primary-20261107'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
receipt=json.loads((E/'reviewer/review-receipt.json').read_text())
assert receipt['review_status']=='COMPLETE' and receipt['candidate_status']=='PASS'
assert json.loads((E/'reviewer/review-verification.json').read_text())['status']=='PASS'
m=json.loads((E/'manifest.json').read_text())
lock=R/'wef-shows/runtime/collector.lock'
try:
 with lock.open('x') as h:h.write(str(os.getpid()))
except FileExistsError:
 print(json.dumps({'deployed':False,'reason':'Existing collector active; no lock clearing'}));raise SystemExit(0)
try:
 assert not (R/'wef-shows/runtime/day-priority.json.stopped.json').exists(),'Priority phase stopped; no clearing'
 for x in m:
  p=R/x['path'];assert (sha(p) if p.exists() else None)==x['before'],x['path']
  assert sha(S/x['path'])==x['sha256'],x['path']
 for name,value in json.loads((E/'protected-hashes.json').read_text()).items():assert sha(R/name)==value,name
 for x in m:
  p=R/x['path'];p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+'.browser-primary.tmp');tmp.write_bytes((S/x['path']).read_bytes());os.replace(tmp,p);assert sha(p)==x['sha256']
 result={'deployed':True,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':m,'manual_runs':0,'record_repairs':0,'task_changes':0,'stop_files_cleared':0,'normal_runs':[],'scheduled_source_discovery':'Pending existing November8 22:00Eastern slot'}
 (E/'deployment.json').write_text(json.dumps(result,indent=2));print(json.dumps({'deployed':True,'at':result['at']}))
finally:lock.unlink()
