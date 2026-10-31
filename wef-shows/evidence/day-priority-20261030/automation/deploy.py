import pathlib,json,hashlib,os,datetime
E=pathlib.Path(__file__).resolve().parent
R=E.parents[3]
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
m=json.loads((E/'manifest.json').read_text());lock=R/'wef-shows/runtime/collector.lock'
try:
 with lock.open('x') as h:h.write(str(os.getpid()))
except FileExistsError:
 print(json.dumps({'deployed':False,'reason':'Existing collector active; no lock clearing or collector invocation'}));raise SystemExit(0)
try:
 for i in m:
  p=R/i['path'];assert (sha(p) if p.exists() else None)==i['before'],i['path'];assert sha(E/'prepared-release'/i['path'])==i['sha256']
 stop=R/'wef-shows/runtime/show-days-stopped.json';before=sha(stop)
 order=sorted(m,key=lambda i:0 if i['path'].endswith('day-priority.js') else 2 if i['path'].endswith('collector.js') else 1)
 for i in order:
  p=R/i['path'];p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_name(p.name+'.priority-deploy.tmp');tmp.write_bytes((E/'prepared-release'/i['path']).read_bytes());os.replace(tmp,p);assert sha(p)==i['sha256']
 assert sha(stop)==before
 receipt={'deployed':True,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'files':m,'count_stop_sha256':before,'manual_collectors':0,'tasks_changed':False}
 (E/'deployment.json').write_text(json.dumps(receipt,indent=2));print(json.dumps({'deployed':True,'at':receipt['at']}))
finally:
 lock.unlink()
