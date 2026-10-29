"use strict";
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const settings=require('./settings');
// Preserve the proven discovery helper and its two-request validation contract.
const {discoverFocus}=require('../wef-schedules/collect');
const {archiveFile}=require('../wef-tenants/archive');
async function fetchSource(url){
 const u=new URL(url);
 if(u.origin!=='https://wef-schedules-decoded.gombcg.workers.dev'||!['/ring','/schedule'].includes(u.pathname))throw Error('Unapproved show source');
 const {fetchTextWithConfiguredTransport}=require('../wef-schedules/lib/sgl_fetch_adapter');
 const result=await fetchTextWithConfiguredTransport(url,async target=>{const response=await fetch(target,{signal:AbortSignal.timeout(30000),headers:{Accept:'application/json'}});return{response,text:await response.text()};},{env:settings});
 if(!result.response.ok)throw Error('Show source HTTP '+result.response.status);
 if(result.response.headers?.get('x-rs-decoded')!=='1')throw Error('Show source decoding not confirmed');
 const payload=JSON.parse(result.text);if(!payload||typeof payload!=='object'||Array.isArray(payload)||payload._t)throw Error('Invalid show source payload');return payload;
}
async function runShows({api=require('./airtable').airtable(),fetchJson=fetchSource,clock=()=>new Date().toISOString(),archive=(kind,value,scope)=>archiveFile(path.join(settings.RUNTIME,'history'),kind,value,scope),statePath=path.join(settings.RUNTIME,'focus.json'),logLimit=1000}={}){
 const report={ok:false,run_id:crypto.randomUUID(),started:clock(),rows:0,errors:[],archives:[]};
 let log={'run-id':report.run_id,started:report.started,result:'Running',summary:'Default-show discovery started',errors:'',archives:'','archive-count':0},ready=false;
 async function keep(kind,value,scope={}){try{const e=await archive(kind,value,scope);report.archives.push(e);return e;}catch(e){report.archives.push({kind,outcome:'Failed',error:e.message});throw e;}}
 async function save(){log['archive-count']=report.archives.length;log.archives=report.archives.map(e=>`${e.at||clock()}; ${e.kind}; ${e.outcome}; ${e.records??0} records; ${e.file||'no file'}; SHA256 ${e.sha256||'unverified'}${e.error?'; '+e.error:''}`).join('\n');await api.upsert('run-logs','run-id',[log]);}
 try{
  const logs=await api.list('run-logs');if(logs.length>=logLimit){const old=logs.filter(r=>r.fields.result!=='Running').sort((a,b)=>String(a.fields.started).localeCompare(String(b.fields.started)))[0];if(!old)throw Error('No finished log slot');await keep('run-logs',[old]);await api.call(settings.BASE_ID,'run-logs','PATCH',{records:[{id:old.id,fields:{...log,completed:null,rows:null,'show-id':null}}]});}
  await save();ready=true;
  let ring;const found=await discoverFocus({statePath,now:clock(),io:{fetchJson:async url=>{const p=await fetchJson(url);if(new URL(url).pathname==='/ring'){ring=p;await keep('default-ring',p);}return p;}}});
  const f=found.show.fields,p=found.payload,meta=p.show||{},sid=Number(f.show_id),selected=f.sgl_default_day;
  if(!selected||!Number.isSafeInteger(Number(meta.facility_id))||Number(meta.facility_id)<=0||!f.show_name||Number(ring?.show?.show_id)!==sid)throw Error('Incomplete canonical show metadata');
  const payloadArchive=await keep('default-schedule',p,{shows:[sid],dates:[selected]});
  const previous=await api.list('shows');await keep('shows-before',previous);
  const wanted={'show-key':`15|${sid}`,show_id:sid,customer_id:15,facility_id:Number(meta.facility_id),show_name:f.show_name,start_date:f.start_date,end_date:f.end_date,selected_date:selected,available_dates:require('../wef-schedules/collect').selectedDays(f).sort().join(', '),is_default:true,last_checked:clock(),'default-payload-file':payloadArchive.file,'default-payload-sha256':payloadArchive.sha256};
  await api.upsert('shows','show-key',[wanted]);
  const stale=previous.filter(r=>r.fields.is_default&&r.fields['show-key']!==wanted['show-key']&&Number(r.fields.customer_id)===15).map(r=>({'show-key':r.fields['show-key'],is_default:false}));if(stale.length)await api.upsert('shows','show-key',stale);
  report.ok=true;report.rows=1;report.show_id=sid;report.selected_date=selected;report.summary=`Default show ${sid}: ${f.show_name}; ${f.start_date} through ${f.end_date}; source-selected ${selected}. No downstream jobs triggered.`;
 }catch(e){report.errors.push(e.message);report.summary='Discovery failed; last verified show retained. '+e.message;}
 report.completed=clock();
 if(ready)try{log={...log,completed:report.completed,result:report.ok?'Success':'Failed','show-id':report.show_id??null,rows:report.rows,summary:report.summary,errors:report.errors.join('\n')};await save();await api.upsert('heartbeat','heartbeat-key',[{'heartbeat-key':'WEF-default','checked-at':report.completed,'show-id':report.show_id??null,'selected-date':report.selected_date||'',result:report.ok?'Success':'Failed',summary:report.summary,errors:report.errors.join('\n')}]);}catch(e){report.ok=false;report.errors.push('Completion evidence: '+e.message);}
 return report;
}
module.exports={runShows,fetchSource};
