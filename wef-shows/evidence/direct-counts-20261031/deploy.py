import pathlib,json,hashlib,os,datetime
E=pathlib.Path(__file__).resolve().parent;R=E.parents[2]
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert json.loads((E/'review-verifier.json').read_text())['status']=='PASS'
m=json.loads((E/'manifest.json').read_text());lock=R/'wef-shows/runtime/collector.lock'
try:
 with lock.open('x') as h:h.write(str(os.getpid()))
except FileExistsError:
 print(json.dumps({'deployed':False,'reason':'Existing collector active; no lock clearing'}));raise SystemExit(0)
try:
 stop=R/'wef-shows/runtime/show-days-stopped.json';assert not stop.exists(),'Existing count stop requires owner review; no clearing'
 for i in m:
  p=R/i['path'];assert (sha(p) if p.exists() else None)==i['before'],i['path'];assert sha(E/'prepared-release'/i['path'])==i['sha256']
 for i in sorted(m,key=lambda i:i['path'].endswith('/collector.js')):
  p=R/i['path'];p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+'.count-deploy.tmp');tmp.write_bytes((E/'prepared-release'/i['path']).read_bytes());os.replace(tmp,p);assert sha(p)==i['sha256']
 data=json.loads((E/'data-before-hashes.json').read_text());assert all(sha(R/p)==h for p,h in data.items() if (R/p).is_file()),'Data changed since baseline; inspect concurrent owner changes'
 receipt={'deployed':True,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':m,'Data_hashes_unchanged':True,'manual_runs':0,'tasks_changed':False,'stop_files_cleared':0}
 (E/'deployment.json').write_text(json.dumps(receipt,indent=2));print(json.dumps({'deployed':True,'at':receipt['at']}))
finally:lock.unlink()
