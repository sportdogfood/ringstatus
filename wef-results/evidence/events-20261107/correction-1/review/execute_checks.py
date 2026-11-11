import subprocess,pathlib,json,datetime,hashlib
root=pathlib.Path.cwd(); out=root/'evidence/events-20261107/correction-1/review'
files=['event-core.js','events.js','linker.js','cleaner.js','build-events.js','test/events.test.js']
(out/'candidate-before.json').write_text(json.dumps([{'path':str(root/f),'sha256':hashlib.sha256((root/f).read_bytes()).hexdigest()} for f in files],indent=2),encoding='utf8')
commands=[]
for ident,cmd in [('events-suite',['node','--test','test/events.test.js']),('cleaner-counterexamples',['node','evidence/events-20261107/correction-1/review/counterexamples.cjs'])]:
 start=datetime.datetime.now(datetime.timezone.utc).isoformat(); p=subprocess.run(cmd,capture_output=True,text=True);end=datetime.datetime.now(datetime.timezone.utc).isoformat()
 row=dict(id=ident,command=subprocess.list2cmdline(cmd),cwd=str(root),runtime=subprocess.check_output(['node','--version'],text=True).strip(),started_at=start,ended_at=end,exit_code=p.returncode,stdout=p.stdout,stderr=p.stderr)
 commands.append(row);print(json.dumps(row))
(out/'command-log.json').write_text(json.dumps({'reviewer':'/root/workflow_review','commands':commands},indent=2),encoding='utf8')
