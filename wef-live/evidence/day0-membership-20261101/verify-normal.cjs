"use strict";
// Read-only acceptance: local receipts/archives and Airtable GETs only.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),zlib=require('zlib');
const root=path.resolve(__dirname,'../..'),api=require(path.join(root,'airtable')).airtable(),settings=require(path.join(root,'settings'));
const hash=b=>crypto.createHash('sha256').update(b).digest('hex'),read=f=>JSON.parse(fs.readFileSync(f,'utf8').replace(/^\uFEFF/,''));
const equal=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
async function main(){
 const output=process.argv[2];if(!output||!/^normal-[12]$/.test(output))throw Error('Choose normal-1 or normal-2');
 if(fs.existsSync(path.join(__dirname,output+'.json')))throw Error('Evidence already exists; preserve it');
 const deployed=read(path.join(__dirname,'deployed.json')),reports={};
 for(const name of ['groups','rings','classes']){
  const r=read(path.join(root,'runtime',`lane-${name}-last-run.json`));
  if(Date.parse(r.started)<=Date.parse(deployed.deployed_at))throw Error('Postdeployment '+name+' run not yet available');
  if(!r.ok||r.stage_errors.length||r.logging_errors.length)throw Error('Failed approved normal path '+name+': '+JSON.stringify({stage:r.stage_errors,logging:r.logging_errors}));
  if(r.day_priority!==0||r.scope.date!=='2026-11-01'||r.scope.show!==200000076)throw Error('Wrong canonical day0 '+name);
  reports[name]=r;
 }
 if(new Set(Object.values(reports).map(r=>r.started.slice(0,16))).size!==1)throw Error('Lane receipts do not identify one completed invocation');
 if(output==='normal-2'){
  const first=read(path.join(__dirname,'normal-1.json'));
  if(Object.keys(reports).some(k=>first.reports[k].run_id===reports[k].run_id))throw Error('Second normal invocation not yet available');
 }
 const group=reports.groups,source=group.archives.find(a=>a.kind==='source-groups');
 const bytes=zlib.gunzipSync(fs.readFileSync(source.file));if(hash(bytes)!==source.sha256)throw Error('Saved source hash mismatch');
 const payload=JSON.parse(bytes),inventory=require(path.join(root,'membership')).appearances(payload,group.scope);
 if(!inventory.complete)throw Error('Source inventory incomplete; cannot certify membership');
 const tables={};for(const name of ['live-rings','live-groups','live-classes','live-entries','live-class-appearances'])tables[name]=await api.list(name);
 const scoped=rows=>rows.filter(x=>Number(x.fields['show-id'])===group.scope.show&&x.fields.date===group.scope.date);
 const groups=scoped(tables['live-groups']),appearances=scoped(tables['live-class-appearances']);
 const records=new Map(appearances.map(x=>[x.fields['appearance-key'],x]));
 if(records.size!==appearances.length)throw Error('Duplicate appearance keys');
 const bad=[];
 for(const wanted of inventory.classes){
  const row=records.get(wanted.key),f=row?.fields||{};
  const g=groups.find(x=>x.fields['group-key']===`${group.scope.show}|${group.scope.date}|${wanted['group-id']}`);
  const c=tables['live-classes'].find(x=>x.fields['class-key']===`${group.scope.show}|${group.scope.date}|${wanted['class-id']}`);
  if(!row||f['record-status']!=='Active'||f['class-group-id']!==wanted['group-id']||f['class-id']!==wanted['class-id']||!f.group?.includes(g?.id)||!f.class?.includes(c?.id))bad.push(wanted.key);
 }
 for(const wanted of inventory.groups){const r=groups.find(x=>x.fields['group-key']===wanted.key);if(r?.fields['record-status']!=='Active')bad.push(wanted.key);}
 const before=read(path.join(__dirname,'native-before.json')),keyFields={'live-rings':'ring-key','live-groups':'group-key','live-classes':'class-key','live-entries':'live-entry-key'},durable={'live-rings':['collection'],'live-groups':['ring','classes','schedules','collection','live-classes'],'live-classes':['groups','ring','schedules','collection','live-entries','live-groups'],'live-entries':['class','entry','collection']};
 const preservation=[];let linksChecked=0,idsChecked=0;
 for(const [name,oldRows]of Object.entries(before.tables))for(const old of oldRows){
  idsChecked++;const now=tables[name].find(x=>x.id===old.id);if(!now){preservation.push('Missing '+old.id);continue;}
  for(const field of [keyFields[name],'show-id','date'])if(!equal(old.fields[field],now.fields[field]))preservation.push(name+' '+old.id+' '+field);
  for(const field of durable[name])for(const id of old.fields[field]||[]){linksChecked++;if(!now.fields[field]?.includes(id))preservation.push(name+' '+old.id+' '+field+' lost '+id);}
 }
 const ids=Object.values(reports).map(r=>r.run_id),filter='OR('+ids.map(id=>'{run-id}='+JSON.stringify(id)).join(',')+')',logs={};
 for(const table of ['heartbeat','run-logs','errs-log']){
  let offset;logs[table]=[];do{const q=new URLSearchParams({filterByFormula:filter,pageSize:'100',...(offset?{offset}: {})});const data=await api.call(settings.BASE_ID,table,'GET',undefined,'?'+q);logs[table].push(...data.records);offset=data.offset;}while(offset);
 }
 for(const id of ids){if(logs.heartbeat.filter(x=>x.fields['run-id']===id&&x.fields.completed).length!==1)bad.push('Missing completed heartbeat '+id);if(logs['run-logs'].filter(x=>x.fields['run-id']===id&&x.fields.completed).length!==1)bad.push('Missing completed run log '+id);}
 const expectedErrors=Object.values(reports).reduce((n,r)=>n+r.issues.length,0);if(logs['errs-log'].length!==expectedErrors)bad.push('Individual errors/readback mismatch');
 const tally=rows=>rows.reduce((d,x)=>(d[x.fields['record-status']||'blank']=(d[x.fields['record-status']||'blank']||0)+1,d),{});
 const out={read_at:new Date().toISOString(),result:bad.length||preservation.length?'FAIL':'PASS',reports,source_verified:source,source_counts:{groups:inventory.groups.length,class_appearances:inventory.classes.length,unique_classes:new Set(inventory.classes.map(a=>a['class-id'])).size},membership_counts:{groups:tally(groups),appearances:tally(appearances)},appearance_link_errors:bad,preservation:{ids_checked:idsChecked,durable_links_checked:linksChecked,errors:preservation},logs,tables};
 fs.writeFileSync(path.join(__dirname,output+'.json'),JSON.stringify(out,null,2));
 console.log(JSON.stringify({result:out.result,read_at:out.read_at,runs:Object.fromEntries(Object.entries(reports).map(([k,v])=>[k,{id:v.run_id,started:v.started,completed:v.completed,issues:v.issues.length,counts:v.counts}])),source_counts:out.source_counts,membership_counts:out.membership_counts,preservation:out.preservation,errors:bad,logs:Object.fromEntries(Object.entries(logs).map(([k,v])=>[k,v.length]))}));
 if(out.result!=='PASS')process.exitCode=1;
}
main().catch(e=>{console.error(e.message);process.exitCode=1;});
