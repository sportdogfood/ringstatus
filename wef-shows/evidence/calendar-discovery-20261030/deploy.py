"""Deploy only reviewed files at an idle boundary. Never invokes a collector."""
import ctypes, datetime, hashlib, json, os, pathlib, sys

EVIDENCE=pathlib.Path(__file__).resolve().parent
ROOT=EVIDENCE.parents[2]
MANIFEST=json.loads((EVIDENCE/'deployment-manifest.json').read_text())
LOCKS=['wef-shows/runtime/collector.lock','wef-schedules/runtime/collector.lock',
       'wef-entries/runtime/collector.lock','wef-live/runtime/collector.lock',
       'wef-data/runtime/collector.lock','wef-timing/runtime/data/runner.lock']

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

def retained_dead_lock(name):
    # Read-only inspection: preserve an existing dead-process lock, never reclaim it.
    # Only Schedules' known prior failure is allowed here; all other locks defer.
    if name!='wef-schedules/runtime/collector.lock':return False
    try:pid=int((ROOT/name).read_text())
    except (ValueError,OSError):return False
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.restype=ctypes.c_void_p
    handle=kernel.OpenProcess(0x1000,False,pid)
    if handle:
        kernel.CloseHandle.argtypes=[ctypes.c_void_p];kernel.CloseHandle(handle);return False
    return ctypes.get_last_error()==87

def main():
    progress_path=EVIDENCE/'deployment-progress.json'
    progress=json.loads(progress_path.read_text()) if progress_path.exists() else {'files':{},'groups':{}}
    groups=[
      ('helper',['wef-shows/authority-age.js','wef-shows/discovery-policy.js'],None),
      ('schedules',['wef-schedules/show-source.js'],'wef-schedules/runtime/collector.lock'),
      ('entries',['wef-entries/model.js'],'wef-entries/runtime/collector.lock'),
      ('live',['wef-live/retirement.js','wef-live/model.js'],'wef-live/runtime/collector.lock'),
      ('timing',['wef-timing/data-runner.js','wef-timing/purge.js','wef-timing/class-adapter.js'],'wef-timing/runtime/data/runner.lock'),
      ('data',['wef-schedules/tests/wef_data_context.py','wef-schedules/tests/wef_data_airtable.py'],'wef-data/runtime/collector.lock'),
      ('shows',['wef-shows/collector.js'],'wef-shows/runtime/collector.lock'),
      ('tests',[m['path'] for m in MANIFEST if '/tests/' in m['path'] and not m['path'].endswith(('wef_data_context.py','wef_data_airtable.py')) or m['path'].endswith('.test.js')],None)
    ]
    items={m['path']:m for m in MANIFEST};busy=[];installed=[]
    def save():progress_path.write_text(json.dumps(progress,indent=2))
    for group,names,lockname in groups:
        if group=='shows' and not all(g in progress['groups'] for g in ['helper','schedules','entries','live','timing','data']):
            busy.append('Shows waits for companions');continue
        if group in progress['groups']:
            for name in names:
                if sha(ROOT/name)!=items[name]['sha256']:raise RuntimeError('Deployed target changed: '+name)
            continue
        retained=None;acquired=None;byte_context=None
        try:
            if lockname:
                lock=ROOT/lockname
                if lock.exists():
                    if retained_dead_lock(lockname):retained=(lock,sha(lock))
                    else:busy.append(group);continue
                else:
                    try:
                        with lock.open('x') as h:h.write(str(os.getpid()))
                        acquired=lock
                    except FileExistsError:busy.append(group);continue
            if group=='data':
                sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-schedules/tests')]
                from retirement import worker_lock,WorkerBusy
                byte_context=worker_lock(ROOT/'wef-data/runtime')
                try:byte_context.__enter__()
                except WorkerBusy:byte_context=None;busy.append('Data Python worker');continue
            for name in names:
                item=items[name];target=ROOT/name;source=EVIDENCE/'prepared-release'/name
                if (sha(target) if target.exists() else None)!=item['before']:raise RuntimeError('Target changed since review: '+name)
                if sha(source)!=item['sha256']:raise RuntimeError('Prepared artifact changed: '+name)
            protected={name:sha(ROOT/name) for name in ['wef-shows/day-source.js','wef-shows/day-archive.js','wef-shows/runtime/show-days-stopped.json'] if (ROOT/name).exists()}
            for name in names:
                target=ROOT/name;target.parent.mkdir(parents=True,exist_ok=True)
                temp=target.with_name(target.name+'.discovery-deploy.tmp')
                temp.write_bytes((EVIDENCE/'prepared-release'/name).read_bytes());os.replace(temp,target)
                if sha(target)!=items[name]['sha256']:raise RuntimeError('Deployment readback mismatch: '+name)
                progress['files'][name]={'sha256':items[name]['sha256'],'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
                save()
            for name,digest in protected.items():
                if sha(ROOT/name)!=digest:raise RuntimeError('Protected file changed: '+name)
            if retained and sha(retained[0])!=retained[1]:raise RuntimeError('Retained dead lock changed')
            progress['groups'][group]={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'protected':protected,'retained_dead_lock':lockname if retained else None};save();installed.append(group)
        finally:
            if byte_context:byte_context.__exit__(None,None,None)
            if acquired:acquired.unlink()
    complete=all(g in progress['groups'] for g,_,_ in groups)
    if complete:
        receipt={'deployed':True,'at':progress['groups']['shows']['at'],'files':MANIFEST,'groups':progress['groups'],'manual_runs':0,'tasks_changed':False}
        (EVIDENCE/'deployment.json').write_text(json.dumps(receipt,indent=2))
    print(json.dumps({'deployed':complete,'installed':installed,'busy':busy}))

if __name__=='__main__':main()
