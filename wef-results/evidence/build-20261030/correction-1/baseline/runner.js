"use strict";
const crypto=require('node:crypto');
const S=require('./settings');
const {evaluate,project,sourceStamp,packed}=require('./model');
function unique(rows,key){const map=new Map();for(const r of rows){const k=r.fields?.[key];if(!k||map.has(k))throw Error('Missing or duplicate '+key);map.set(k,r);}return map;}
function authority(rows,now,maxAge){
  if(!Number.isFinite(maxAge)||maxAge<=0)throw Error('Explicit authority freshness policy required');
  const selected=rows.filter(r=>r.fields.is_default===true&&r.fields.customer_id===S.CUSTOMER);
  if(selected.length!==1)throw Error('Expected one canonical default show');
  const f=selected[0].fields,age=Date.parse(now)-Date.parse(f.authority_checked_at||f.last_checked);
  if(!Number.isSafeInteger(f.show_id)||f.show_id<=0||f['show-key']!==`${S.CUSTOMER}|${f.show_id}`||!Number.isFinite(age)||age<0||age>=maxAge)throw Error('Invalid or stale canonical show');
  if(require('../wef-shows/authority-age').showAuthorityReason(f,now))throw Error('Canonical show authority validation failed');
  if(!/^\d{4}-\d{2}-\d{2}$/.test(f.start_date)||!/^\d{4}-\d{2}-\d{2}$/.test(f.end_date)||f.start_date>f.end_date)throw Error('Invalid show dates');
  return {show:f.show_id,customer:S.CUSTOMER,start:f.start_date,end:f.end_date,record:selected[0].id,checked_at:f.last_checked};
}
async function run({api,now=()=>new Date().toISOString(),runId=crypto.randomUUID(),maxSourceAgeMs,maxAuthorityAgeMs}={}){
  const started=now(),report={run_id:runId,started,result:'Running',classes:0,ready:0,withheld:0,changed:0,errors:[],logging_errors:[]};
  const errorRows=[];
  async function log(table,key,values){return api.upsert(table,key,values);}
  try{
    if(!Number.isFinite(maxSourceAgeMs)||maxSourceAgeMs<=0)throw Error('Explicit Results source freshness policy required');
    // A write/readback failure here stops before any business snapshot mutation.
    await log('run-logs','run-id',[{'run-id':runId,started,result:'Running'}]);
    await log('heartbeat','heartbeat-key',[{'heartbeat-key':'wef-results','run-id':runId,started,'checked-at':started,result:'Running'}]);
    const scope=authority(await api.list(S.SHOWS,'shows'),started,maxAuthorityAgeMs);
    report.show=scope.show;
    const rows=await api.list(S.DATA,'classes');
    const live=unique(await api.list(S.LIVE,'live-classes'),'class-key');
    const current=unique(await api.list(S.BASE,'snapshot'),'snapshot-key');
    unique(rows,'class-key');
    const inputs=rows.filter(r=>String(r.fields['class-key']).startsWith(`${scope.customer}|${scope.show}|`));
    const seen=new Set();
    for(const row of inputs){
      const key=row.fields['class-key'];seen.add(key);report.classes++;
      const liveKey=key.split('|').filter((_,i)=>i!==0&&i!==3).join('|');
      let e=evaluate(row,{now:started,maxSourceAgeMs,customer:scope.customer,show:scope.show,live:live.get(liveKey)});
      if(e.date&&(e.date<scope.start||e.date>scope.end))e={...e,status:'unavailable',reason:'Class date outside canonical show'};
      // Re-read atomic Data row before accepting. A Live overlay change does not renew source time.
      if(e.status==='ready'){
        const latest=await api.get(S.DATA,'classes',row.id);
        if(!latest||sourceStamp(latest)!==e.sourceStamp)e={...e,status:'unavailable',reason:'Data class changed during evaluation'};
      }
      const old=current.get(key)?.fields;
      let snapshot;
      try{snapshot=project(e,old,runId,now());}catch(error){e={...e,status:'unavailable',reason:error.message};snapshot=project(e,old,runId,now());}
      if(e.status==='ready')report.ready++;else report.withheld++;
      if(e.status==='unavailable')errorRows.push({'error-key':`${runId}|${key}`,'run-id':runId,'occurred-at':now(),'show-id':scope.show,date:e.date||'','err-class':Number(row.fields['class-id'])||null,message:e.reason});
      if(snapshot['content-hash']&&snapshot['content-hash']!==old?.['content-hash'])report.changed++;
      await log('snapshot','snapshot-key',[snapshot]);
    }
    for(const [key,row] of current){if(seen.has(key))continue;
      const historical=row.fields['show-id']!==scope.show;
      await log('snapshot','snapshot-key',[{'snapshot-key':key,'check-status':historical?'historical':'unavailable','check-reason':historical?'Not canonical current show':'Class absent from Data inventory; last accepted snapshot retained','checked-at':now(),'checked-run-id':runId}]);
    }
    // Authority rollover during the read invalidates current status, without erasing accepted content.
    const finalScope=authority(await api.list(S.SHOWS,'shows'),now(),maxAuthorityAgeMs);
    if(finalScope.show!==scope.show){
      for(const key of seen)await log('snapshot','snapshot-key',[{'snapshot-key':key,'check-status':'unavailable','check-reason':'Canonical show changed during Results evaluation','checked-at':now(),'checked-run-id':runId}]);
      throw Error('Canonical show changed during Results evaluation');
    }
    report.result=errorRows.length?'Partial':'Success';
  }catch(error){report.result='Failed';report.errors.push(error.message);errorRows.push({'error-key':`${runId}|run`,'run-id':runId,'occurred-at':now(),message:error.message});}
  report.completed=now();
  for(const row of errorRows){try{await log('errs-log','error-key',[row]);}catch{report.logging_errors.push('errs-log write failed');}}
  const summary=packed({classes:report.classes,ready:report.ready,withheld:report.withheld,changed:report.changed});
  try{await log('run-logs','run-id',[{'run-id':runId,started,completed:report.completed,result:report.result,'show-id':report.show??null,'class-count':report.classes,'ready-count':report.ready,'changed-count':report.changed,summary,errors:packed([...report.errors,...errorRows.map(e=>e.message)])}]);}catch{report.logging_errors.push('run-logs completion failed');}
  try{await log('heartbeat','heartbeat-key',[{'heartbeat-key':'wef-results','run-id':runId,started,completed:report.completed,'checked-at':report.completed,result:report.result,'show-id':report.show??null,summary,errors:packed(report.errors),...(report.result==='Success'?{'last-success':report.completed}:{})}]);}catch{report.logging_errors.push('heartbeat completion failed');}
  report.ok=report.result!=='Failed'&&!report.logging_errors.length;
  return report;
}
module.exports={run,authority,unique};
