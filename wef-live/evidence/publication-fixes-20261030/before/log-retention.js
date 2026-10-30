"use strict";
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib');
const TABLES=['heartbeat','run-logs','errs-log'];
const terminal=new Set(['Success','Skipped','Partial','Failed']);
const uuid=v=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f-]{27}$/i.test(v);
function eligible(table,row,cutoff,parents={}){
 const f=row.fields||{},run=f['run-id'];if(!TABLES.includes(table)||!uuid(run))return false;
 const old=v=>Number.isFinite(Date.parse(v))&&Date.parse(v)<Date.parse(cutoff);
 if(table==='errs-log')return old(f['occurred-at'])&&old(parents[run])&&String(f['error-key']||'').startsWith(run+'|');
 return terminal.has(f.result)&&old(f.completed)&&(table!=='heartbeat'||f['heartbeat-key']===run);
}
function verified(receipt){const b=zlib.gunzipSync(fs.readFileSync(receipt.file));if(crypto.createHash('sha256').update(b).digest('hex')!==receipt.sha256)throw Error('Log archive hash mismatch');return JSON.parse(b);}
async function retainLogs({api,runtime,runId,now=new Date().toISOString(),maxRowsPerTable=250,budgetMs=240000}){
 const deadline=Date.now()+budgetMs;api=api.forRetention?api.forRetention(deadline):api;
 const stop=path.join(runtime,'log-retention-stop.json');if(fs.existsSync(stop))return{status:'Blocked',reason:'Prior retention failure requires owner review',stopFile:stop};
 const result={status:'Success',cutoff:new Date(Date.parse(now)-86400000).toISOString(),tables:[],deleted:0};
 const initial=fs.openSync(stop,'wx');try{fs.writeFileSync(initial,JSON.stringify({runId,at:now,status:'InProgress'}));fs.fsyncSync(initial);}finally{fs.closeSync(initial);}
 const archive=(kind,rows)=>require('./archive').archiveFile(path.join(runtime,'history'),kind,rows);
 try{
  const data={};for(const t of ['heartbeat','run-logs'])data[t]=await api.list(t);
  const active=new Set([...data.heartbeat,...data['run-logs']].filter(r=>!terminal.has(r.fields.result)||!Number.isFinite(Date.parse(r.fields.completed))).map(r=>r.fields['run-id']).filter(Boolean));active.add(runId);
  const parents={};for(const row of data['run-logs'])if(eligible('run-logs',row,result.cutoff)&&!active.has(row.fields['run-id']))parents[row.fields['run-id']]=row.fields.completed;
  // Parent completion remains provable after its Airtable log has been archived.
  for(const name of fs.readdirSync(runtime)){if(!/^[0-9a-f-]{36}\.json$/i.test(name))continue;let r;try{r=JSON.parse(fs.readFileSync(path.join(runtime,name),'utf8'));}catch{continue;}if(uuid(r.run_id)&&!active.has(r.run_id)&&typeof r.ok==='boolean'&&Number.isFinite(Date.parse(r.completed))&&Date.parse(r.completed)<Date.parse(result.cutoff))parents[r.run_id]=r.completed;}
  data['errs-log']=await api.oldErrors(result.cutoff,maxRowsPerTable,r=>!active.has(r.fields['run-id'])&&eligible('errs-log',r,result.cutoff,parents));
  const catalogs=await api.list('collections');
  for(const table of TABLES){
   if(Date.now()>deadline-60000){result.status='Deferred';result.reason='Retention budget reserved for next normal run';break;}
   const rows=data[table].filter(r=>!active.has(r.fields['run-id'])&&eligible(table,r,result.cutoff,parents)).sort((a,b)=>String(a.fields.completed||a.fields['occurred-at']).localeCompare(String(b.fields.completed||b.fields['occurred-at']))).slice(0,maxRowsPerTable);
   if(!rows.length){result.tables.push({table,archived:0,deleted:0});continue;}
   const key='log-archive|'+table,previous=catalogs.filter(r=>r.fields['collection-id']===key);if(previous.length>1)throw Error('Duplicate log archive catalog');
   let prior=null,total=0;if(previous.length){const f=previous[0].fields;prior={file:f['archive-file'],sha256:f.fingerprint};const manifest=verified(prior);if(manifest.table!==table||manifest.version!==1||!Number.isSafeInteger(manifest.total)||manifest.total<0)throw Error('Invalid log catalog manifest');total=manifest.total;}
   const batch=await archive('retired-log-'+table,rows);verified(batch);
   const manifest=await archive('log-retention-manifest-'+table,{version:1,table,runId,at:now,cutoff:result.cutoff,previous:prior,batch,total:total+rows.length});verified(manifest);
   const fields={'collection-id':key,'source-kind':'log-archive-catalog','source-id':table,started:now,completed:now,status:'Verified','row-count':total+rows.length,fingerprint:manifest.sha256,'archive-file':manifest.file,message:JSON.stringify({runId,batch:batch.file,records:rows.length,cleanup:'pending'}),errors:''};
   await api.upsert('collections','collection-id',[fields]);const saved=(await api.list('collections')).filter(r=>r.fields['collection-id']===key);if(saved.length!==1||Object.entries(fields).some(([k,v])=>v!==''&&saved[0].fields[k]!==v))throw Error('Log catalog readback mismatch');
   await api.retireLogs(table,rows,result.cutoff,batch,parents);
   await api.upsert('collections','collection-id',[{...fields,status:'Retired',message:JSON.stringify({runId,batch:batch.file,records:rows.length,cleanup:'complete'})}]);
   result.tables.push({table,archived:rows.length,deleted:rows.length,batch,manifest,catalog:key});result.deleted+=rows.length;
  }
  fs.unlinkSync(stop);
 }catch(error){result.status='Failed';result.error=error.message;const body=JSON.stringify({runId,at:now,result},null,2);const fd=fs.openSync(stop,'w');try{fs.writeFileSync(fd,body);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}if(fs.readFileSync(stop,'utf8')!==body)throw Error('Retention stop readback failed');}
 return result;
}
module.exports={retainLogs,eligible,verified,TABLES};
