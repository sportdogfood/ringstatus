"use strict";
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),{spawnSync}=require('node:child_process'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../..'),stage=path.join(__dirname,'staged');
const baseline=JSON.parse(fs.readFileSync(path.join(__dirname,'baseline-hashes.json'))),candidate=JSON.parse(fs.readFileSync(path.join(__dirname,'candidate-hashes.json')));
const hash=file=>crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const state=spawnSync('powershell',['-NoProfile','-Command',"$t=Get-ScheduledTask -TaskName wef-entries-cwf-collector;$i=Get-ScheduledTaskInfo -TaskName wef-entries-cwf-collector;[pscustomobject]@{state=[string]$t.State;seconds=($i.NextRunTime-(Get-Date)).TotalSeconds}|ConvertTo-Json -Compress"],{encoding:'utf8',windowsHide:true});
assert.equal(state.status,0,state.stderr);const timing=JSON.parse(state.stdout);if(timing.state!=='Ready'||timing.seconds<75){console.log(JSON.stringify({deferred:true,reason:'Waiting for natural idle deployment window',...timing}));process.exit(0);}
const preTask=fs.readFileSync(path.join(__dirname,'task-before.xml'),'utf8');
const release=require(path.join(root,'runtime-utils')).acquireLock(path.join(root,'runtime/collector.lock'));
try{
 for(const n of [...Object.keys(candidate),'live-input.js'])assert.equal(hash(path.join(root,n)),baseline[n],'Protected source changed '+n);
 for(const [n,h]of Object.entries(candidate))assert.equal(hash(path.join(stage,n)),h,'Candidate changed '+n);
 // Establish a stable GET-only snapshot while the existing collector lock is held.
 const snapshot=spawnSync(process.execPath,[path.join(__dirname,'verify-readonly.cjs'),'before'],{encoding:'utf8',windowsHide:true,timeout:55000});
 assert.equal(snapshot.status,0,snapshot.stderr);console.log(snapshot.stdout);
 for(const n of Object.keys(candidate)){const dest=path.join(root,n),tmp=dest+'.linking-restoration.tmp';fs.copyFileSync(path.join(stage,n),tmp);fs.renameSync(tmp,dest);}
 // Preserve removed helper/test source in before/; no record or runtime-data deletion.
 fs.unlinkSync(path.join(root,'live-input.js'));
 for(const n of ['current-writer.test.js','lanes.test.js','test_retained_linking.py'])fs.copyFileSync(path.join(stage,'tests',n),path.join(root,'tests',n));
 for(const n of ['live-input.test.js','test_retained_lanes.py'])fs.unlinkSync(path.join(root,'tests',n));
 for(const [n,h]of Object.entries(candidate))assert.equal(hash(path.join(root,n)),h);
 assert(!fs.existsSync(path.join(root,'live-input.js')));
 const task=spawnSync('powershell',['-NoProfile','-Command','Export-ScheduledTask -TaskName wef-entries-cwf-collector'],{encoding:'utf8',windowsHide:true});assert.equal(task.status,0);assert.equal(task.stdout,preTask,'Task definition changed');
 const receipt={at:new Date().toISOString(),route:'Unchanged wef-entries-cwf-collector -> run-hidden.vbs -> run.ps1 -> entry.js',files:candidate,retiredCode:['live-input.js'],taskXmlSha256:crypto.createHash('sha256').update(task.stdout).digest('hex'),manualRuns:0,taskChanges:0,recordRepairs:0,recordsDeleted:0};
 fs.writeFileSync(path.join(__dirname,'deployment.json'),JSON.stringify(receipt,null,2));console.log(JSON.stringify(receipt));
}finally{release();}
