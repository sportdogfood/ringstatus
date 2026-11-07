'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const hash=value=>crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const alive=pid=>{if(!Number.isInteger(pid)||pid<=0)return false;try{process.kill(pid,0);return true;}catch(e){return e.code!=='ESRCH';}};
function rows(root,table){const dir=path.join(root,table);return fs.existsSync(dir)?fs.readdirSync(dir).filter(n=>n.endsWith('.json')).map(n=>JSON.parse(fs.readFileSync(path.join(dir,n),'utf8'))):[];}
function location(root,table,id){if(!/^[A-Za-z0-9_-]+$/.test(id))throw Error('Invalid local log identifier');return path.join(root,table,id+'.json');}
function ack(root,table,id){try{const a=JSON.parse(fs.readFileSync(location(root,'acks',table+'-'+id),'utf8'));return typeof a.id==='string'&&a.id?a:null;}catch{return null;}}
// These are the existing attempt-log.js delivery payloads, including field order.
function runFields(a){const f={'attempt-id':a.id,workflow:a.workflow,started:a.started,status:a.status,stage:a.stage};for(const n of ['completed','elapsed-seconds','run-key','source-revision','show-key','counts-json','reason'])if(a[n]!==undefined)f[n]=a[n];return f;}
function errorFields(e,parent){return {'error-id':e.id,run:[parent],'occurred-at':e.at,stage:e.stage,category:e.category,operation:e.operation,target:e.target,message:e.message,'write-outcome':e['write-outcome']};}
function pruneDelivered(root,config){
 const runs=rows(root,'run-logs').sort((a,b)=>a.started.localeCompare(b.started)),errors=rows(root,'err-logs');
 const keep=new Set([runs.at(-1)?.id,runs.filter(a=>a.status==='Succeeded').sort((a,b)=>a.completed.localeCompare(b.completed)).at(-1)?.id,errors.slice().sort((a,b)=>a.at.localeCompare(b.at)).at(-1)?.attempt]);
 const counts={runs:0,errors:0,acks:0};
 try{for(const a of runs){
  if(keep.has(a.id)||!a.completed||!['Succeeded','Partial','Failed','Skipped','Interrupted','Deferred'].includes(a.status)||a.base!==config.base||a.workflow!==config.workflow||[a['owner-pid'],a['child-pid']].some(alive))continue;
  const delivered=ack(root,'run-logs',a.id),family=errors.filter(e=>e.attempt===a.id);
  if(!delivered||delivered.hash!==hash(runFields(a))||family.some(e=>ack(root,'err-logs',e.id)?.hash!==hash(errorFields(e,delivered.id))))continue;
  // Delivery has finished and the task cannot overlap itself. Keep parent until all errors are removed.
  for(const e of family){fs.unlinkSync(location(root,'err-logs',e.id));counts.errors++;}
  fs.unlinkSync(location(root,'run-logs',a.id));counts.runs++;
  for(const [table,id] of [...family.map(e=>['err-logs',e.id]),['run-logs',a.id]]){fs.unlinkSync(location(root,'acks',table+'-'+id));counts.acks++;}
 }
 }catch(error){error.localCounts={...counts};throw error;}
 return counts;
}
function atomic(file,value){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(value));fs.renameSync(tmp,file);}
function request(runtime,event,selected){
 const base=path.dirname(runtime),workflows=['wef-data','wef-data-linking'].filter(w=>fs.existsSync(path.join(base,'logging',w)));
 const value={...event,workflows,base,enabled:selected.includes('run-logs')&&selected.includes('err-logs')};
 for(const workflow of workflows)atomic(path.join(runtime,'local-'+workflow+'.json'),{run_id:event.run_id,workflow,result:'Pending'});
 atomic(path.join(runtime,'local-request.json'),value);return value;
}
function prune(root,config){
 const runtime=path.join(__dirname,'runtime','cleanup'),file=path.join(runtime,'local-request.json');
 if(!fs.existsSync(file))return {runs:0,errors:0,acks:0};
 const req=JSON.parse(fs.readFileSync(file,'utf8')),receipt=path.join(root,'cleanup-receipt.json');
 if(!req.workflows.includes(config.workflow))return {runs:0,errors:0,acks:0};
 try{if(JSON.parse(fs.readFileSync(receipt,'utf8')).run_id===req.run_id)return {runs:0,errors:0,acks:0};}catch{}
 const service=path.join(runtime,'local-'+config.workflow+'.json');
 try{const prior=JSON.parse(fs.readFileSync(service,'utf8'));if(prior.run_id===req.run_id&&['Started','Success','Error'].includes(prior.result))return {runs:0,errors:0,acks:0};}catch{}
 atomic(service,{run_id:req.run_id,workflow:config.workflow,result:'Started',pid:process.pid});
 const out={run_id:req.run_id,event_id:req.event_id,workflow:config.workflow,completed:new Date().toISOString(),result:'Success'};
 try{out.counts=req.enabled?pruneDelivered(root,config):{runs:0,errors:0,acks:0};out.completed=new Date().toISOString();atomic(receipt,out);atomic(service,out);return out.counts;}
 catch(e){out.result='Error';out.error=e.message;out.counts=e.localCounts||out.counts||{runs:0,errors:0,acks:0};out.completed=new Date().toISOString();try{atomic(receipt,out);}catch(write){e.message+='; cleanup receipt not saved: '+write.message;}
 try{atomic(service,out);}catch{}throw e;}
}
async function wait(req){
 if(!req.enabled)return [];
 for(;;){const results=req.workflows.map(workflow=>{
  const service=JSON.parse(fs.readFileSync(path.join(req.base,'cleanup','local-'+workflow+'.json'),'utf8'));
  if(service.run_id!==req.run_id)throw Error('Local cleanup service receipt belongs to another invocation');
  if(['Success','Error'].includes(service.result))return service;
  if(service.result==='Started'&&!alive(service.pid))return {run_id:req.run_id,workflow,result:'Error',error:'Registered local cleanup service ended without a terminal receipt; completion unconfirmed',counts:null};
  return null;
 });
  if(results.every(Boolean))return results;
  await new Promise(resolve=>setTimeout(resolve,1000));
 }
}
module.exports={prune,pruneDelivered,request,wait};

if(require.main===module&&process.argv.includes('--service')){
 const workflow=process.argv.includes('--linking')?'wef-data-linking':'wef-data';
 try{const config=require('./logging-schema.json').workflows[workflow];console.log(JSON.stringify({localLogCleanup:prune(path.join(__dirname,'runtime','logging',workflow),config)}));}
 catch(error){console.error(error.message);process.exitCode=1;}
}
