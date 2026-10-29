"use strict";
// Logging owns only its private outbox and three configured logging tables.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const iso=()=>new Date().toISOString();
function safe(value){let text=String(value??'');for(const name of ['AIRTABLE_TOKEN','WEF_LIVE_AIRTABLE_TOKEN'])if(process.env[name])text=text.split(process.env[name]).join('[redacted]');return text.replace(/Bearer\s+[^\s]+/gi,'Bearer [redacted]').replace(/([?&](?:token|key|authorization|cookie|access_token)=)[^\s&]+/gi,'$1[redacted]').slice(0,2000);}
function atomic(file,value){fs.mkdirSync(path.dirname(file),{recursive:true});const tmp=file+'.tmp';const fd=fs.openSync(tmp,'w');try{fs.writeFileSync(fd,JSON.stringify(value));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}fs.renameSync(tmp,file);}
function file(root,id){if(!/^[a-zA-Z0-9_-]+$/.test(id))throw Error('Invalid logging identifier');return path.join(root,'run-logs',id+'.json');}
function readAttempt(root,id){return JSON.parse(fs.readFileSync(file(root,id),'utf8'));}
function begin(root,config,id=crypto.randomUUID(),at=iso()){
 const target=file(root,id);if(fs.existsSync(target))throw Error('Attempt identifier already reserved');
 const row={id,workflow:config.workflow,base:config.base,started:at,status:'Started',stage:'Startup','owner-pid':process.pid};atomic(target,row);return row;
}
function recordError(root,id,error,stage='Logging',at=iso()){
 readAttempt(root,id);const dir=path.join(root,'err-logs');const number=files(root,'err-logs').filter(e=>e.attempt===id).length+1;const key=id+'-'+String(number).padStart(4,'0');
 atomic(path.join(dir,key+'.json'),{id:key,attempt:id,at,stage:safe(stage),category:safe(error.category||'Process'),operation:safe(error.operation),target:safe(error.target),message:safe(error.message),'write-outcome':error['write-outcome']||'Unknown'});
}
function finish(root,id,result,at=iso()){
 const row=readAttempt(root,id);if(row.status!=='Started')return row;
 const allowed=['Succeeded','Failed','Skipped','Interrupted','Deferred'];if(!allowed.includes(result.status))throw Error('Invalid terminal log status');
 if(result.error)recordError(root,id,result.error,result.stage||row.stage,at);
 Object.assign(row,{status:result.status,completed:at,stage:safe(result.stage||row.stage),'elapsed-seconds':Math.max(0,(Date.parse(at)-Date.parse(row.started))/1000)});
 for(const name of ['run-key','source-revision','show-key'])if(result[name])row[name]=safe(result[name]);
 if(result.counts)row['counts-json']=JSON.stringify(result.counts);
 if(result.reason)row.reason=safe(result.reason);atomic(file(root,id),row);return row;
}
function files(root,table){const dir=path.join(root,table);return fs.existsSync(dir)?fs.readdirSync(dir).filter(f=>f.endsWith('.json')).map(f=>JSON.parse(fs.readFileSync(path.join(dir,f),'utf8'))):[];}
function digest(value){return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');}
function same(actual,expected){return Object.entries(expected).every(([k,v])=>JSON.stringify(actual[k]??(Array.isArray(v)?[]:v===''?'':null))===JSON.stringify(v));}
async function flush(root,config,request,{milliseconds=20000,operations=30}={}){
 const started=Date.now();let calls=0;const ids={};
 async function call(...args){if(Date.now()-started>milliseconds||calls>=operations)throw Error('Logging delivery budget reached');calls++;return request(...args);}
 async function mirror(table,primary,key,fields){
  const ackfile=path.join(root,'acks',table+'-'+key+'.json');const hash=digest(fields);
  if(fs.existsSync(ackfile)){const ack=JSON.parse(fs.readFileSync(ackfile,'utf8'));if(ack.hash===hash)return ack.id;}
  const target=config.tables[table];let rows=await call('GET',target,{primary,key});if(rows.length>1)throw Error('Ambiguous logging identity');
  const old=rows[0];if(!old||!same(old.fields,fields))await call(old?'PATCH':'POST',target,{primary,key,id:old?.id,fields});
  rows=await call('GET',target,{primary,key});if(rows.length!==1||!same(rows[0].fields,fields))throw Error('Logging readback mismatch');
  atomic(ackfile,{id:rows[0].id,hash});return rows[0].id;
 }
 try{
  const attempts=files(root,'run-logs').sort((a,b)=>a.started.localeCompare(b.started));
  for(const a of attempts){if(a.base!==config.base||a.workflow!==config.workflow)throw Error('Logging base/workflow mismatch');
   const fields={'attempt-id':a.id,workflow:a.workflow,started:a.started,status:a.status,stage:a.stage};
   for(const n of ['completed','elapsed-seconds','run-key','source-revision','show-key','counts-json','reason'])if(a[n]!==undefined)fields[n]=a[n];
   ids[a.id]=await mirror('run-logs','attempt-id',a.id,fields);
  }
  const errors=files(root,'err-logs');for(const e of errors){if(!ids[e.attempt])throw Error('Missing local attempt for error');await mirror('err-logs','error-id',e.id,{'error-id':e.id,run:[ids[e.attempt]],'occurred-at':e.at,stage:e.stage,category:e.category,operation:e.operation,target:e.target,message:e.message,'write-outcome':e['write-outcome']});}
  const last=attempts.at(-1);if(last){const fields={'heartbeat-key':config.workflow,workflow:config.workflow,status:last.status,'last-wake':last.started,'last-attempt':last.started,'last-run':[ids[last.id]],message:last.reason||last.stage};const success=attempts.filter(a=>a.status==='Succeeded').sort((a,b)=>a.completed.localeCompare(b.completed)).at(-1);if(success)fields['last-success']=success.completed;const error=errors.sort((a,b)=>a.at.localeCompare(b.at)).at(-1);if(error)fields['last-error']=error.at;await mirror('heartbeat','heartbeat-key',config.workflow,fields);}
  return {ok:true,operations:calls};
 }catch(error){return {ok:false,error:safe(error.message),operations:calls};}
}
function transport(base,token){let last=0;return async(method,table,options)=>{
 const wait=250-(Date.now()-last);if(wait>0)await new Promise(resolve=>setTimeout(resolve,wait));last=Date.now();
 if(!/^app[A-Za-z0-9]+$/.test(base)||!/^tbl[A-Za-z0-9]+$/.test(table))throw Error('Invalid logging target');
 const params=method==='GET'?'?'+new URLSearchParams({filterByFormula:'{'+options.primary+'}='+JSON.stringify(options.key),pageSize:'100'}):'';
 const body=method==='GET'?undefined:JSON.stringify({records:[{...(method==='PATCH'?{id:options.id}:{}),fields:options.fields}]});
 const r=await fetch('https://api.airtable.com/v0/'+base+'/'+table+params,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body,signal:AbortSignal.timeout(5000)});
 if(!r.ok)throw Error('Logging Airtable '+method+' HTTP '+r.status);const data=await r.json();if(method==='GET'&&data.offset)throw Error('Ambiguous paginated logging identity');return method==='GET'?data.records:data.records[0];
};}
async function deliver(root,config){if(!process.env.AIRTABLE_TOKEN)return {ok:false,error:'Logging token unavailable'};return flush(root,config,transport(config.base,process.env.AIRTABLE_TOKEN));}
function recoverInterrupted(root,current){for(const a of files(root,'run-logs')){if(a.id===current||a.status!=='Started')continue;const pids=[a['owner-pid'],a['child-pid']].filter(p=>Number.isInteger(p)&&p>0);if(!pids.length)continue;const alive=pids.some(p=>{try{process.kill(p,0);return true;}catch(e){return e.code!=='ESRCH';}});if(!alive)finish(root,a.id,{status:'Interrupted',stage:a.stage,error:{category:'Process',message:'Previous invocation ended without a terminal receipt','write-outcome':'Unknown'}});}}
module.exports={begin,finish,readAttempt,flush,atomic,safe,deliver,recordError,recoverInterrupted};
