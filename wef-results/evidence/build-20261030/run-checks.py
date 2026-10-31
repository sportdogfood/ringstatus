"""Build checks only: no business entrypoint invocation or network requests."""
import subprocess,json,sys,shutil,hashlib
from pathlib import Path
from datetime import datetime,timezone
root=Path(__file__).resolve().parents[2]
out=Path(__file__).resolve().parent
node=shutil.which('node');logs=[]
if '--remaining' in sys.argv and (out/'execution-log.json').exists():
 logs=json.loads((out/'execution-log.json').read_text(encoding='utf-8'))['commands']
def now():return datetime.now(timezone.utc).isoformat()
def run(name,argv,cwd=root):
 if '--remaining' in sys.argv and any(e['id']==name and e['exit_code']==0 for e in logs):return
 start=now();r=subprocess.run(argv,cwd=cwd,capture_output=True,text=True)
 logs.append(dict(id=name,command=subprocess.list2cmdline(argv),cwd=str(cwd),runtime=argv[0],started_at=start,ended_at=now(),exit_code=r.returncode,stdout=r.stdout,stderr=r.stderr))
 (out/'execution-log.json').write_text(json.dumps(dict(reviewer='01a169d7-0bcd-7ca1-8b39-4e124da8b363',commands=logs),indent=2),encoding='utf-8')
 if r.returncode:print(r.stdout,r.stderr);raise SystemExit(r.returncode)
 return r
run('unit-integration-offline',[node,'--test',*[str(p) for p in sorted((root/'test').glob('*.test.js'))]])
run('retained-source-shape',[node,str(out/'retained-shape-check.js')])
run('javascript-syntax',[node,'-e',"const fs=require('node:fs'),cp=require('node:child_process');for(const f of fs.readdirSync('.').filter(f=>f.endsWith('.js'))){const r=cp.spawnSync(process.execPath,['--check',f],{encoding:'utf8'});if(r.status)throw Error(r.stderr);}console.log('All module JavaScript parsed');"])
shell=shutil.which('pwsh') or str(Path.home()/'.cache/codex-runtimes/codex-primary-runtime/dependencies/native/powershell/pwsh.exe')
run('powershell-syntax',[shell,'-NoProfile','-NonInteractive','-Command',"$e=$null; $t=$null; [System.Management.Automation.Language.Parser]::ParseFile((Join-Path (Get-Location) 'run.ps1'),[ref]$t,[ref]$e)|Out-Null; if($e){$e|Out-String|Write-Error;exit 1}; 'Launcher parsed; not executed'"])
manifest=[dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(root.glob('*')) if p.is_file()]
(out/'runtime-manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
print(json.dumps(dict(checks=len(logs),all_exit_zero=True,log=str(out/'execution-log.json'))))
