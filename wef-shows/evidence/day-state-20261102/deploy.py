import pathlib,json,hashlib,os,datetime
E=pathlib.Path(__file__).resolve().parent
R=E.parents[2]
S=R/'wef-shows/staged/day-state-20261102'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert json.loads((E/'review-verifier.json').read_text())['status']=='PASS'
receipt=json.loads((E/'review-receipt.json').read_text())
assert receipt['candidate_status']=='PASS' and receipt['review_status']=='COMPLETE'
m=json.loads((E/'manifest.json').read_text())
lock=R/'wef-shows/runtime/collector.lock'
try:
 with lock.open('x') as h:h.write(str(os.getpid()))
except FileExistsError:
 print(json.dumps({'deployed':False,'reason':'Existing collector active; no lock clearing'}));raise SystemExit(0)
try:
 assert not (R/'wef-shows/runtime/day-priority.json.stopped.json').exists(),'Existing priority stop; no clearing'
 for i in m:
  assert sha(R/i['path'])==i['before'],i['path']
  assert sha(S/i['path'])==i['sha256'],i['path']
 for p,h in json.loads((E/'protected-hashes.json').read_text()).items():assert sha(R/p)==h,p
 for i in m:
  p=R/i['path'];tmp=p.with_name(p.name+'.day-state-deploy.tmp');tmp.write_bytes((S/i['path']).read_bytes());os.replace(tmp,p)
  assert sha(p)==i['sha256']
 result={'deployed':True,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':m,'manual_runs':0,'records_repaired':0,'task_changes':0,'stop_files_cleared':0}
 (E/'deployment.json').write_text(json.dumps(result,indent=2));print(json.dumps({'deployed':True,'at':result['at']}))
finally:lock.unlink()
