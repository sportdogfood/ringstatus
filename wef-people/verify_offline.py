"""Reviewer-owned targeted commands; never invokes a production entry point."""
import datetime,json,pathlib,subprocess,sys
ROOT=pathlib.Path(__file__).resolve().parent
commands=[]
for ident,args in [('join-tests',[sys.executable,'-B','-m','unittest','discover','-s',str(ROOT),'-p','test_join.py','-v']),
                   ('logging-tests',['node','--test',str(ROOT/'test_logging.js')]),
                   ('entry-syntax',['node','--check',str(ROOT/'entry.js')]),
                   ('logging-syntax',['node','--check',str(ROOT/'logging.js')])]:
    start=datetime.datetime.now(datetime.timezone.utc).isoformat()
    result=subprocess.run(args,cwd=ROOT.parent,capture_output=True,text=True,encoding='utf-8',errors='replace')
    commands.append({'id':ident,'command':subprocess.list2cmdline(args),'cwd':str(ROOT.parent),
        'runtime':sys.version if args[0]==sys.executable else subprocess.check_output(['node','--version'],text=True).strip(),
        'started_at':start,'ended_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'exit_code':result.returncode,'stdout':result.stdout,'stderr':result.stderr})
out=ROOT/'evidence';out.mkdir(exist_ok=True)
(out/'commands.json').write_text(json.dumps({'reviewer':'01a19727-9a70-7283-a865-01dd38aacb9d (module author self-review)','commands':commands},indent=2),encoding='utf-8')
print(json.dumps({'checks':[{'id':r['id'],'exit_code':r['exit_code']} for r in commands]}))
sys.exit(int(any(r['exit_code'] for r in commands)))
