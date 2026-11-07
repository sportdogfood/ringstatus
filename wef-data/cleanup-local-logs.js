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
function prune(root,config){
 const runs=rows(root,'run-logs').sort((a,b)=>a.started.localeCompare(b.started)),errors=rows(root,'err-logs');
 const keep=new Set([runs.at(-1)?.id,runs.filter(a=>a.status==='Succeeded').sort((a,b)=>a.completed.localeCompare(b.completed)).at(-1)?.id,errors.slice().sort((a,b)=>a.at.localeCompare(b.at)).at(-1)?.attempt]);
 const counts={runs:0,errors:0,acks:0};
 for(const a of runs){
  if(keep.has(a.id)||!a.completed||!['Succeeded','Partial','Failed','Skipped','Interrupted','Deferred'].includes(a.status)||a.base!==config.base||a.workflow!==config.workflow||[a['owner-pid'],a['child-pid']].some(alive))continue;
  const delivered=ack(root,'run-logs',a.id),family=errors.filter(e=>e.attempt===a.id);
  if(!delivered||delivered.hash!==hash(runFields(a))||family.some(e=>ack(root,'err-logs',e.id)?.hash!==hash(errorFields(e,delivered.id))))continue;
  // Delivery has finished and the task cannot overlap itself. Keep parent until all errors are removed.
  for(const e of family){fs.unlinkSync(location(root,'err-logs',e.id));counts.errors++;}
  fs.unlinkSync(location(root,'run-logs',a.id));counts.runs++;
  for(const [table,id] of [...family.map(e=>['err-logs',e.id]),['run-logs',a.id]]){fs.unlinkSync(location(root,'acks',table+'-'+id));counts.acks++;}
 }
 return counts;
}
module.exports={prune};
