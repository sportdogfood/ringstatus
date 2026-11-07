'use strict';
// Read-only evidence: file names/hashes and delivery state, never payload backups or collection.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const digest=s=>crypto.createHash('sha256').update(s).digest('hex');
const base=path.resolve(__dirname,'../../runtime/logging'),result={at:new Date().toISOString(),workflows:{}};
for(const workflow of ['wef-data','wef-data-linking']){
 const root=path.join(base,workflow),files={};
 for(const table of ['run-logs','err-logs','acks']){files[table]={};const dir=path.join(root,table);if(fs.existsSync(dir))for(const file of fs.readdirSync(dir).filter(f=>f.endsWith('.json'))){const text=fs.readFileSync(path.join(dir,file),'utf8');files[table][file]={hash:digest(text),row:JSON.parse(text)};}}
 const ack=(table,id)=>files.acks[table+'-'+id+'.json']?.row;
 const pending=[];
 for(const [file,{row:a}] of Object.entries(files['run-logs'])){const f={'attempt-id':a.id,workflow:a.workflow,started:a.started,status:a.status,stage:a.stage};for(const n of ['completed','elapsed-seconds','run-key','source-revision','show-key','counts-json','reason'])if(a[n]!==undefined)f[n]=a[n];if(ack('run-logs',a.id)?.hash!==digest(JSON.stringify(f)))pending.push('run-logs/'+file);}
 for(const [file,{row:e}] of Object.entries(files['err-logs'])){const f={'error-id':e.id,run:[ack('run-logs',e.attempt)?.id],'occurred-at':e.at,stage:e.stage,category:e.category,operation:e.operation,target:e.target,message:e.message,'write-outcome':e['write-outcome']};if(ack('err-logs',e.id)?.hash!==digest(JSON.stringify(f)))pending.push('err-logs/'+file);}
 result.workflows[workflow]={counts:Object.fromEntries(Object.entries(files).map(([t,f])=>[t,Object.keys(f).length])),pending,files:Object.fromEntries(Object.entries(files).flatMap(([t,f])=>Object.entries(f).map(([name,r])=>[t+'/'+name,r.hash])))};
}
console.log(JSON.stringify(result,null,2));
