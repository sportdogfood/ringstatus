import subprocess,json,datetime,pathlib
root=pathlib.Path.cwd(); out=root/'evidence/workflow-20261101/review'
commands=[]
for ident,cmd in [('offline-suite',['node','--test','test/workflow.test.js']),('counterexamples',['node','evidence/workflow-20261101/review/counterexamples.cjs'])]:
 start=datetime.datetime.now(datetime.timezone.utc).isoformat(); p=subprocess.run(cmd,capture_output=True,text=True)
 end=datetime.datetime.now(datetime.timezone.utc).isoformat()
 entry=dict(id=ident,command=subprocess.list2cmdline(cmd),cwd=str(root),runtime=subprocess.check_output(['node','--version'],text=True).strip(),started_at=start,ended_at=end,exit_code=p.returncode,stdout=p.stdout,stderr=p.stderr)
 commands.append(entry); print(json.dumps(entry))
(out/'command-log.json').write_text(json.dumps(dict(reviewer='/root/workflow_review',commands=commands),indent=2),encoding='utf8')
