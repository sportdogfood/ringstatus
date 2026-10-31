"use strict";
const fs=require('node:fs'),path=require('node:path'),{execFile}=require('node:child_process');
const {SHOWS,TIMING,DATA_TABLES:T}=require('./api');const {verifyRows}=require('./runner');const {hash}=require('./pace');const purge=require('./purge'),ROOT=path.resolve(__dirname,'..');
function defer(message){const error=Error(message);error.deferred=true;return error;}
function selectShow(rows,now){
  const defaults=rows.filter(r=>r.fields.is_default===true);if(defaults.length!==1)throw defer('Canonical default show unresolved');
  const s=defaults[0],f=s.fields,customer=Number(f.customer_id),show=Number(f.show_id);
  if(!Number.isSafeInteger(customer)||customer<=0||!Number.isSafeInteger(show)||show<=0||f['show-key']!==`${customer}|${show}`||require('../wef-shows/authority-age').showAuthorityReason(f,now))throw defer('Canonical show identity/freshness invalid');
  if(![f.start_date,f.end_date,f.selected_date].every(purge.validDay)||f.selected_date<f.start_date||f.selected_date>f.end_date)throw defer('Canonical dates invalid');
  return{record:s,context:{'show-key':f['show-key'],'show-id':show,'customer-id':customer,'start-date':f.start_date,'end-date':f.end_date,'selected-date':f.selected_date}};
}
function sameAuthority(left,right){return left.record.id===right.record.id&&hash(left.context)===hash(right.context);}
async function writeAnswers(api,writes,base=TIMING,table=T.answers){
  if(!writes.length)return [];
  const ids=new Map();
  for(let i=0;i<writes.length;i+=10){
    const batch=writes.slice(i,i+10),result=await api.upsert(table,'answer-key',batch);
    if(!Array.isArray(result?.records)||result.records.length!==batch.length)throw Error('Timing write response coverage mismatch');
    verifyRows(result.records,batch,'answer-key');
    for(const row of result.records){
      if(!/^rec[A-Za-z0-9]{14}$/.test(row.id)||ids.has(row.id))throw Error('Timing write response identity mismatch');
      ids.set(row.id,row.fields['answer-key']);
    }
  }
  // Independent GET readback; batch only the query, never the evaluated scope.
  // list() follows every offset, including an unexpected duplicate response page.
  const saved=[],recordIds=[...ids.keys()];
  for(let i=0;i<recordIds.length;i+=100){
    const batch=recordIds.slice(i,i+100);
    saved.push(...await api.list(base,table,'OR('+batch.map(id=>`RECORD_ID()='${id}'`).join(',')+')'));
  }
  if(saved.some(row=>!ids.has(row.id)||ids.get(row.id)!==row.fields['answer-key'])||new Set(saved.map(row=>row.id)).size!==saved.length)throw Error('Timing readback identity mismatch');
  verifyRows(saved,writes,'answer-key');
  if(saved.length!==writes.length)throw Error('Timing readback coverage mismatch');
  return saved;
}
function pointer(context){
  let p;try{p=JSON.parse(fs.readFileSync(path.join(ROOT,'wef-data/runtime/handoff','head-'+context['show-key'].replace('|','-')+'.json'),'utf8'));}
  catch(error){if(error.code==='ENOENT')throw defer('Current-show Data handoff unavailable');throw error;}
  const fence=['show-key','start-date','end-date','selected-date'].map(k=>context[k]);
  if(hash(p.authority)!==hash(fence)||p['show-key']!==context['show-key'])throw defer('Waiting for current Data authority');
  return p;
}
function loadData(context,deadline){
  return new Promise((resolve,reject)=>{
    const remaining=deadline-Date.now()-18000;if(remaining<2000){const e=Error('Data input budget deferred');e.deferred=true;reject(e);return;}
    const child=execFile(path.join(ROOT,'wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'data-input.py')],{
      windowsHide:true,timeout:remaining,maxBuffer:8*1024*1024,env:{SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR,TEMP:process.env.TEMP,TMP:process.env.TMP,PYTHONDONTWRITEBYTECODE:'1'}
    },(error,stdout,stderr)=>{
      if(error){const e=Error(error.killed?'Data input budget deferred':String(stderr||'Data retained input unavailable').trim());e.deferred=Boolean(error.killed)||e.message.includes('pointer changed during read');reject(e);return;}
      try{resolve(JSON.parse(stdout));}catch{reject(Error('Data input projection unreadable'));}
    });child.stdin.end(JSON.stringify(context));
  });
}

module.exports={defer,selectShow,sameAuthority,pointer,loadData,writeAnswers};
