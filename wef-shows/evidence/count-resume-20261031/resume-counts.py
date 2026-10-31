import pathlib,json,hashlib,os,datetime,shutil
E=pathlib.Path(__file__).resolve().parent;R=E.parents[2]
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert json.loads((E/'review-verifier.json').read_text())['status']=='PASS'
lock=R/'wef-shows/runtime/collector.lock'
try:
 with lock.open('x') as h:h.write(str(os.getpid()))
except FileExistsError:
 print(json.dumps({'resumed':False,'reason':'Existing normal collector active; no lock clearing'}));raise SystemExit(0)
try:
 for i in json.loads((E/'manifest.json').read_text()):assert sha(R/i['path'])==i['sha256'],i['path']
 stop=R/'wef-shows/runtime/show-days-stopped.json';assert sha(stop)==sha(E/'stop-before.json')
 # Explicit owner-approved release of this exact prior count stop only.
 stop.unlink()
 receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'resumed':True,'archived_stop_sha256':sha(E/'stop-before.json'),'runtime_edits':0,'reason':'Already-installed saved-day reader avoids full historical replay; targeted/independent checks passed','manual_runs':0,'records_repaired':0,'tasks_changed':False}
 (E/'recovery.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt))
finally:lock.unlink()
