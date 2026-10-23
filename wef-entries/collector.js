"use strict";
const path=require('node:path'),crypto=require('node:crypto');
const settings=require('./settings'),model=require('./model');
const hash=v=>crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex');
const entryFields=['entry-key','show-id','date','entry-id','class-id','horse-id','horse-name','rider-id','rider-name','trainer-ids','class-name','ring-number','status','source-json','schedule-key','last-seen','schedules'];
const project=f=>Object.fromEntries(entryFields.filter(k=>f[k]!=null).map(k=>[k,f[k]]));
async function runEntries({api=require('./airtable').airtable(),fetchPeople=require('./source').fetchPeople,fetchClass=require('./source').fetchClass,clock=()=>new Date().toISOString(),archive=(kind,value,scope)=>require('../wef-tenants/archive').archiveFile(path.join(settings.RUNTIME,'history'),kind,value,scope),logLimit=1000}={}){
 const report={ok:false,run_id:crypto.randomUUID(),started:clock(),rows:0,collections:[],errors:[],archives:[]};let log={'run-id':report.run_id,started:report.started,result:'Running',summary:'Independent CWF entries collection started.',errors:'','archive-count':0,archives:''},logReady=false,show;
 async function keep(kind,value,scope={}){try{const event=await archive(kind,value,scope);report.archives.push(event);return event;}catch(e){report.archives.push({kind,outcome:'Failed',error:e.message});e.auditFailure=true;throw e;}}
 async function saveLog(){log={...log,'archive-count':report.archives.length,archives:report.archives.map(a=>`${a.at||clock()}: ${a.kind} ${a.outcome}; ${(a.dates||[]).join(',')}; ${a.records??0} records; ${a.file||'no file'}; SHA256 ${a.sha256||'unverified'}${a.error?'; '+a.error:''}`).join('\n')};await api.upsert('run-logs','run-id',[log]);}
 try{
  const logs=await api.list('run-logs');if(logs.length>=logLimit){const oldest=logs.filter(r=>r.fields.result!=='Running').sort((a,b)=>String(a.fields.started).localeCompare(String(b.fields.started)))[0];if(!oldest)throw Error('No finished run-log slot available');await keep('run-logs',[oldest]);await api.call(settings.BASE_ID,'run-logs','PATCH',{records:[{id:oldest.id,fields:{...log,completed:null,'show-id':null,rows:null}}]});}
  await saveLog();logReady=true;
  const [tenants,trainers,shows,sourceSchedules,synced,favorites]=await Promise.all([api.list('tenant-details',settings.CWF_BASE_ID),api.list('trainers',settings.CWF_BASE_ID),api.list(settings.SHARED_SHOWS_TABLE,settings.SHOWS_BASE_ID),api.list('schedules',settings.SHARED_BASE_ID),api.list(settings.SYNC_TABLE),api.list(settings.FAVORITES_TABLE,settings.TENANTS_BASE_ID)]);
  const scope=model.scope(tenants,trainers,shows,clock());show=scope.show;report.show_source={base:settings.SHOWS_BASE_ID,record:shows.find(r=>r.fields===show).id,checked_at:show.last_checked};const sid=Number(show.show_id),schedules=model.schedulesFor(show,sourceSchedules,synced),follow=model.selections(favorites,show,scope.dates);report.show_id=sid;report.dates=scope.dates;log['show-id']=sid;await saveLog();
  const oldEntries=await api.list('entries'),oldCollections=await api.list('collections');
  const sources=[...scope.trainers.map(t=>({kind:'trainer-feed',id:String(t.fields['trainer-id'])})),...model.owners.map(id=>({kind:'user-follow',id}))];
  const plans=[],classCache=new Map();
  for(const source of sources){const record={source_kind:source.kind,source_id:source.id,result:'Failed',rows:0};report.collections.push(record);
   try{
    let rows;
    if(source.kind==='trainer-feed'){const payload=await fetchPeople(Number(source.id),show);await keep('people-'+source.id,payload,{shows:[sid],dates:scope.dates});rows=model.normalize(model.validatePeople(payload,source.id,sid),source.id,show,schedules);}
    else{rows=[];for(const item of follow[source.id]){if(!classCache.has(item.classId)){const payload=await fetchClass(item.classId,show);await keep('class-'+item.classId,payload,{shows:[sid],dates:scope.dates});classCache.set(item.classId,payload);}const trips=require('./source').classEntries(classCache.get(item.classId),item.classId,sid).filter(t=>Number(t.entry_id)===item.entry&&Number(t.class_id)===item.classId);const selected=model.normalize(trips,'',show,schedules).filter(r=>r['entry-key']===item.key);if(selected.length!==1)throw Error('Follow selection unresolved: '+item.key);rows.push(...selected);}}
    record.rows=rows.length;const datasets=[];for(const date of scope.dates){const selected=rows.filter(r=>r.date===date),event=await keep('dataset-'+source.kind+'-'+source.id,selected,{shows:[sid],dates:[date]});datasets.push({date,rows:selected,event});}
    record.result='Success';plans.push({source,record,datasets});
   }catch(e){if(e.auditFailure)throw e;record.error=e.message;report.errors.push(source.kind+' '+source.id+': '+e.message);plans.push({source,record});}
   await saveLog();
  }
  const collectionKey=(source,date)=>['CWF',sid,date,source.kind,source.id].join('|');
  const retained=[];for(const plan of plans.filter(p=>!p.datasets)){for(const date of scope.dates){const old=oldCollections.find(c=>c.fields['collection-id']===collectionKey(plan.source,date));for(const id of old?.fields.entries||[]){const row=oldEntries.find(e=>e.id===id);if(!row)throw Error('Missing retained collection entry');retained.push(project(row.fields));}}}
  const merged=model.merge([...plans.flatMap(p=>(p.datasets||[]).flatMap(d=>d.rows)),...retained]);
  // Archive all pre-change participation, including inactive and historical associations.
  await keep('entries-before',oldEntries,{shows:[sid],dates:scope.dates});
  const incoming=new Map(merged.map(r=>[r['entry-key'],r]));const wanted=merged.map(r=>{const matches=schedules.filter(s=>s.fields['stage-key']===r['schedule-key']);if(matches.length!==1)throw Error('Entry schedule identity unresolved');const observed=plans.some(p=>(p.datasets||[]).some(d=>d.rows.some(x=>x['entry-key']===r['entry-key'])));return{...r,status:'active','last-seen':observed?clock():r['last-seen'],schedules:[matches[0].id]};});
  // Entries no longer selected are retained as dropped; never delete or repair records.
  for(const old of oldEntries.filter(r=>Number(r.fields['show-id'])===sid&&!incoming.has(r.fields['entry-key'])))wanted.push({...project(old.fields),status:'dropped'});
  const saved=wanted.length?await api.upsert('entries','entry-key',wanted):[];const updates=[];
  for(const plan of plans){for(const date of scope.dates){const key=collectionKey(plan.source,date),common={'collection-id':key,'tenant-key':'CWF','source-kind':plan.source.kind,'source-id':plan.source.id,'show-id':sid,date,started:report.started,completed:clock()};if(plan.datasets){const d=plan.datasets.find(d=>d.date===date);updates.push({...common,status:'complete','row-count':d.rows.length,'dataset-id':hash(d.rows),fingerprint:hash(d.rows),message:d.rows.length?'Validated selected participation':'Success: zero entries'+(plan.source.kind==='user-follow'&&!follow[plan.source.id].length?' (no private selections configured)':''),'archive-file':d.event.file,entries:d.rows.map(r=>{const record=saved.find(x=>x.fields['entry-key']===r['entry-key']);if(!record)throw Error('Missing saved collection entry');return record.id;})});}else updates.push({...common,status:'failed',message:plan.record.error});}}
  await api.upsert('collections','collection-id',updates);
  report.rows=wanted.filter(r=>r.status==='active').length;report.ok=report.errors.length===0;report.summary=report.collections.map(r=>`${r.source_kind} ${r.source_id}: ${r.result}; ${r.rows} selected participation${r.error?'; '+r.error:''}`).join('\n')+`\nCWF show ${sid}; ${report.rows} unique active participation; ${scope.dates.length} dates. Inactive horses/riders remain collected. Private selections are not team exports.`;
 }catch(e){report.errors.push(e.message);report.ok=false;report.summary='Collection stopped; prior unverified data preserved. '+e.message;}
 report.completed=clock();
 if(logReady){log={...log,completed:report.completed,result:report.ok?'Success':'Failed',rows:report.rows,summary:report.summary,errors:report.errors.join('\n')};try{await saveLog();await api.upsert('heartbeat','heartbeat-key',[{'heartbeat-key':'CWF','checked-at':report.completed,'selected-date':show?.selected_date||'','show-id':show?Number(show.show_id):null,result:report.ok?'Success':'Failed',summary:report.summary,errors:report.errors.join('\n')}]);}catch(e){report.ok=false;report.errors.push('Completion evidence failed: '+e.message);}}
 return report;
}
module.exports={runEntries};
