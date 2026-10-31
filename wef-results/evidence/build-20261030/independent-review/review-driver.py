from pathlib import Path
import json,hashlib,subprocess,datetime
root=Path(r'C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-results')
out=root/'evidence/build-20261030/independent-review';out.mkdir(parents=True,exist_ok=True)
files=list(root.glob('*.js'))+list(root.glob('*.json'))+list(root.glob('*.md'))+list(root.glob('*.ps1'))+list(root.glob('*.vbs'))+list((root/'test').glob('*.js'))+[root.parent/'wef-shows/authority-age.js',root.parent/'wef-data/data_runner.py',root/'evidence/build-20261030/requirements.md',root/'evidence/build-20261030/retained-shape-fixtures.json']
def manifest():return [{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in files]
(out/'before.json').write_text(json.dumps(manifest(),indent=2))
script=r'''const assert=require('node:assert/strict');const root=process.argv[2];const {evaluate,project}=require(root+'/model');const {readSnapshot}=require(root+'/reader');const {fixture,options}=require(root+'/test/fixtures');
const output=[];
for(const kind of ['truncated','duplicate-uuid','whitespace-uuid']){const r=fixture(),j=JSON.parse(r.fields['source-json']);j.total_entry_trips=2;if(kind==='truncated')j.trips.pop();if(kind==='duplicate-uuid')j.trips[1].entryxclasses_uuid=j.trips[0].entryxclasses_uuid;if(kind==='whitespace-uuid')j.trips[0].entryxclasses_uuid='   ';r.fields['source-json']=JSON.stringify(j);const e=evaluate(r,options);const p=project(e,undefined,'review',options.now);const consumer=readSnapshot(p,{...options,maxCheckAgeMs:600000,heartbeat:{result:'Success','run-id':'review',completed:options.now,'checked-at':options.now}});assert.equal(e.status,'ready');assert.equal(consumer.status,'available');output.push({kind,declared:j.total_entry_trips,actual:j.trips.length,evaluation:e.status,consumer:consumer.status});}
console.log(JSON.stringify({confirmed_defect_reproductions:output},null,2));'''
(out/'supplementary.js').write_text(script)
commands=[]
def run(id,cmd,cwd=root):
 start=datetime.datetime.now(datetime.timezone.utc).isoformat();p=subprocess.run(cmd,cwd=cwd,capture_output=True,text=True);end=datetime.datetime.now(datetime.timezone.utc).isoformat();commands.append(dict(id=id,command=subprocess.list2cmdline(cmd),cwd=str(cwd),runtime='Windows subprocess; '+subprocess.check_output(['node','--version'],text=True).strip(),started_at=start,ended_at=end,exit_code=p.returncode,stdout=p.stdout,stderr=p.stderr));print(id,p.returncode,p.stdout[-2200:],p.stderr)
run('suite',['node','--test',*map(str,(root/'test').glob('*.test.js'))])
run('retained-shape',['node',str(root/'evidence/build-20261030/retained-shape-check.js')])
run('supplementary',['node',str(out/'supplementary.js'),str(root)])
(out/'execution-log.json').write_text(json.dumps({'reviewer':'/root/results_review','commands':commands},indent=2))
(out/'after.json').write_text(json.dumps(manifest(),indent=2));print('UNCHANGED',json.loads((out/'before.json').read_text())==manifest())
