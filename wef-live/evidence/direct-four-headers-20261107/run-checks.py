from pathlib import Path
import subprocess,datetime,json
r=Path(__file__).resolve().parent;cwd=Path.cwd()
a=datetime.datetime.now(datetime.timezone.utc).isoformat();cmd=['node','--test',str(r/'headers.test.cjs'),str(r/'pause.test.cjs')]
p=subprocess.run(cmd,capture_output=True,text=True);b=datetime.datetime.now(datetime.timezone.utc).isoformat()
log={'reviewer':'wef-live author','commands':[{'id':'direct-headers-offline','command':subprocess.list2cmdline(cmd),'cwd':str(cwd),'runtime':subprocess.check_output(['node','--version'],text=True).strip(),'started_at':a,'ended_at':b,'exit_code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}]}
(r/'offline.json').write_text(json.dumps(log,indent=2));print(p.stdout);print(p.stderr);raise SystemExit(p.returncode)
