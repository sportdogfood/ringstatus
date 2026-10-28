"use strict";
const crypto=require('node:crypto'),path=require('node:path');
const settings=require('./settings'),schema=require('./schema.json'),model=require('./model');
const fieldsFor=(table,fields)=>{const names=new Set(schema.tables.find(t=>t.name===table).fields.map(f=>f.name));return Object.fromEntries(Object.entries(fields).filter(([k])=>!(table==='entries'&&['show-details','rings'].includes(k))&&(names.has(k)||['shows','synced-rings','schedules','rings-helper','horses','riders','trainers'].includes(k))));};
const hash=value=>crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const pick=(fields,names)=>Object.fromEntries(names.filter(k=>fields[k]!=null).map(k=>[k,fields[k]]));
async function runTenant({api=require('./airtable').airtable(),fetchPeople=require('./source').fetchPeople,standingSource=require('./standing-roster').loadStandingRoster,clock=()=>new Date().toISOString(),archive=(kind,value,scope)=>require('./archive').archiveFile(path.join(settings.RUNTIME,'history'),kind,value,scope),logLimit=1000,entrySourceMode=settings.ENTRY_SOURCE_MODE,tenantSlicesEnabled=settings.TENANT_SLICES_ENABLED}={}){
 const started=clock(),runId=crypto.randomUUID(),report={ok:false,run_id:runId,started,rows:0,days:[],trainers:[],errors:[],archives:[]};
 let log={'run-id':runId,started,result:'Running',summary:'Tenant collector started.',errors:'','archive-count':0,archives:''};
 let logReady=false,knownShow,knownIndex;
 async function saveLog(){await api.upsert('run-logs','run-id',[log]);}
 async function keep(kind,value,scope={}){try{const event=await archive(kind,value,scope);report.archives.push(event);await archiveLog();return event;}catch(e){e.auditFailure=true;if(!report.archives.some(a=>a.kind===kind&&a.outcome==='Failed'))report.archives.push({kind,outcome:'Failed',error:e.message});try{await archiveLog();}catch{}throw e;}}
 async function archiveLog(){log={...log,'archive-count':report.archives.length,archives:report.archives.map(a=>`${a.at||clock()}: ${a.kind} ${a.outcome}; shows ${(a.shows||[]).join(',')||'none'}; dates ${(a.dates||[]).join(',')||'none'}; ${a.records??0} records; ${a.file||'no file'}; SHA256 ${a.sha256||'unverified'}${a.error?'; '+a.error:''}`).join('\n')};await saveLog();}
 try{
  if(!['legacy','shadow','synced'].includes(entrySourceMode))throw Error('Unknown entry source mode');
  report.entry_source_mode=entrySourceMode;report.people_fetches=0;
  const priorLogs=await api.list('run-logs');
  if(priorLogs.length>=logLimit){const oldest=priorLogs.filter(r=>r.fields.result!=='Running').sort((a,b)=>String(a.fields.started).localeCompare(String(b.fields.started)))[0];if(!oldest)throw Error('Log capacity occupied by unfinished runs');
   const event=await archive('run-logs',[oldest],{shows:oldest.fields['show-id']?[oldest.fields['show-id']]:[]});report.archives.push(event);
   // Replace only an archived finished slot. Do not create a 1001st record.
   await api.call(settings.BASE_ID,'run-logs','PATCH',{records:[{id:oldest.id,fields:{...log,completed:null,'show-id':null,rows:null,'days-complete':0,'days-failed':0}}]});
  }
  await archiveLog();logReady=true;
  const tenants=await api.list('tenant-details'),tenant=tenants.filter(r=>r.fields['tenant-key']==='CWF');if(tenant.length!==1||tenant[0].fields.active!==true||Number(tenant[0].fields['tenant-id'])!==8778)throw Error('Active CWF tenant configuration required');
  const trainers=model.validateTrainers(await api.list('trainers'));
  // Standing helpers are independent of show discovery and this week's entries.
  const standing=require('./standing-roster');
  const standingData=await standingSource(api,tenant[0],trainers);
  const standingPlan=standing.planStandingRoster({source:standingData,tenant:tenant[0],trainers,horses:await api.list('horses'),riders:await api.list('riders'),now:clock()});
  await keep('standing-roster',standingData,{records:standingData.horses.length+standingData.riders.length+1});
  for(const table of ['horses','riders'])if(standingPlan[table].length)await api.upsert(table,table==='horses'?'horse-key':'rider-key',standingPlan[table].map(fields=>fieldsFor(table,fields)));
  report.roster={horses:standingPlan.horses.length,riders:standingPlan.riders.length,missing_ids:standingPlan.missing_ids.length,source_counts:standingPlan.source_counts};
  const [sourceShows,sourceRings,syncedSchedules]=await Promise.all([settings.SYNC_SHOWS_TABLE,settings.SYNC_RINGS_TABLE,settings.SYNC_SCHEDULES_TABLE].map(t=>api.list(t)));
  const references=require('./synced-references').validateReferences({shows:sourceShows,rings:sourceRings,schedules:syncedSchedules,now:clock()}),{show,showRecord,rings,schedules}=references,dates=model.showDays(show),sid=Number(show.show_id);
  report.show_id=sid;report.selected_date=show.selected_date;log['show-id']=sid;await saveLog();
  knownShow=show;
  const consumer=require('./entries-consumer'),synced=entrySourceMode==='legacy'?null:await consumer.load({api,show,dates,trainers,now:clock()});
  if(synced){report.entry_sync=synced.proof;await keep('synced-entries',synced.local,{shows:[sid],dates});}
  const [sourceHelpers,sharedTenants,sharedShows]=await Promise.all([api.list('rings-helper',settings.SHARED_BASE_ID),api.list('tenants',settings.SHARED_BASE_ID),api.list(settings.SHARED_SHOWS_TABLE,settings.SHARED_BASE_ID,{filterByFormula:`{show-key}='${show['show-key']}'`,fields:['show-key']})]);
  const sharedShow=sharedShows.filter(r=>r.fields['show-key']===show['show-key']);if(sharedShow.length!==1)throw Error('Shared show identity required for tenant index link');
  const sharedTenant=sharedTenants.filter(r=>r.fields['tenant-key']==='CWF');if(sharedTenant.length!==1)throw Error('Shared CWF location reference missing or ambiguous');
  const helperFields=sourceHelpers.filter(r=>Number(r.fields.customer_id)===Number(show.customer_id)&&Number(r.fields.facility_id)===Number(show.facility_id)).map(r=>({...pick(r.fields,['ring-key','customer_id','facility_id','ring_id','ring_number','ring_name','ring-nickname','ring-lat','ring-lon','notes']),'source-record':r.id,'source-base':settings.SHARED_BASE_ID}));
  for(const ring of rings){const f=ring.fields;const matches=helperFields.filter(h=>['ring_id','ring_number','ring_name'].some(k=>f[k]!=null&&h[k]!=null&&String(f[k]).trim().toLowerCase()===String(h[k]).trim().toLowerCase()));if(matches.length!==1||['ring_id','ring_number','ring_name'].some(k=>f[k]!=null&&matches[0][k]!=null&&String(f[k]).trim().toLowerCase()!==String(matches[0][k]).trim().toLowerCase()))throw Error('Shared ring identity ambiguous or conflicting');}
  const helperRows=await api.upsert('rings-helper','ring-key',helperFields.map(h=>({...h,'synced-rings':rings.filter(r=>Number(r.fields.ring_number)===Number(h.ring_number)).map(r=>r.id)})));
  const ringRows=rings;
  // Seed a tenant baseline from the existing owner-approved examples once.
  const walks=await api.list('ring-walks');const wantedWalks=[];for(const helper of sourceHelpers){const f=helper.fields;if(!(f['walk-tenant']||[]).includes(sharedTenant[0].id)||f['ring-walk']==null)continue;const key='CWF|home|'+f['ring-key'];if(walks.some(r=>r.fields['walk-key']===key))continue;const h=helperRows.find(r=>r.fields['ring-key']===f['ring-key']);if(h)wantedWalks.push({'walk-key':key,'tenant-key':'CWF','ring-key':f['ring-key'],'barn-name':tenant[0].fields['barn-name']||'home',minutes:f['ring-walk'],'is-example':f['ring-walk-is-example']===true,'confirmed-on':'2026-10-20',notes:'Owner retained demonstration minutes on 2026-10-20. Not measured walking time.','rings-helper':[h.id]});}if(wantedWalks.length)await api.upsert('ring-walks','walk-key',wantedWalks);
  const indexKey='CWF|'+sid,priorIndex=await api.list('wef-tenant-index',settings.SHARED_BASE_ID);
  knownIndex=indexKey;
  await api.upsert('wef-tenant-index','index-key',[...priorIndex.filter(r=>r.fields['tenant-key']==='CWF'&&r.fields['index-key']!==indexKey&&r.fields.active).map(r=>({'index-key':r.fields['index-key'],active:false})),{'index-key':indexKey,'tenant-key':'CWF','tenant-name':tenant[0].fields['tenant-name'],'tenant-id':8778,'tenant-base-id':settings.BASE_ID,'tenant-base-url':'https://airtable.com/'+settings.BASE_ID,'show-id':sid,active:true,...pick(tenant[0].fields,['barn-name','barn-lat','barn-lon']),'show-details':[sharedShow[0].id],'last-checked':clock(),'last-result':'Collecting',notes:'Enabled collector for this source-selected show; configuration does not assert attendance.'}],settings.SHARED_BASE_ID);
  const incoming=[],successful=[],roster=[];
  for(const trainer of trainers){const pid=Number(trainer.fields['trainer-id']),summary={trainer_id:pid,role:trainer.fields.role,result:'Failed',rows:0};report.trainers.push(summary);let trainerName;
   try{
    let payload,rows;
    if(entrySourceMode==='synced')rows=synced.byTrainer.get(String(pid));
    else{report.people_fetches++;payload=await fetchPeople(pid,sid,show.customer_id);await keep('people-'+pid,payload,{shows:[sid],dates});const trips=model.validatePeople(payload,pid,sid);rows=model.normalizeEntries(trips,trainer,show,schedules,{baseId:settings.BASE_ID,tableId:settings.SYNC_SCHEDULES_TABLE});}
    // Archive every day before changing any dataset for this trainer.
    const datasets=[];for(const date of dates){const records=rows.filter(r=>r.date===date),event=await keep('dataset-'+pid,records,{shows:[sid],dates:[date]});datasets.push({date,records,event});}
    for(const {date,records,event}of datasets)await api.upsert('collections','collection-id',[{'collection-id':`${sid}|${pid}|${date}`,'show-id':sid,'trainer-id':pid,date,status:'complete',started,completed:clock(),'dataset-id':hash(records),'row-count':records.length,fingerprint:hash(records),message:entrySourceMode==='synced'?`Consumed wef-entries sync: ${records.length} entries`:(records.length?'Validated trainer entries':'No entries for this show day'),'archive-file':event.file,shows:[showRecord.id],trainers:[trainer.id]}]);
    incoming.push(...rows);successful.push(pid);roster.push({trainer,rows});summary.rows=rows.length;summary.result=rows.length?'Success':'No entries';
    if(typeof payload?.people?.name==='string'&&payload.people.name.trim())trainerName=payload.people.name.trim();
   }catch(error){if(error.auditFailure)throw error;summary.error=String(error.message||error);report.errors.push(`Trainer ${pid}: ${summary.error}`);for(const date of dates)await api.upsert('collections','collection-id',[{'collection-id':`${sid}|${pid}|${date}`,'show-id':sid,'trainer-id':pid,date,status:'failed',started,completed:clock(),'row-count':null,message:summary.error,shows:[showRecord.id],trainers:[trainer.id]}]);}
   await api.upsert('trainers','trainer-key',[{'trainer-key':trainer.fields['trainer-key'],...(trainerName?{'trainer-name':trainerName}:{}),'last-result':summary.result,'last-checked':clock(),'last-error':summary.error||''}]);
  }
  // The standing roster grows from valid source identities; absence never disables it.
  const horseOld=await api.list('horses'),riderOld=await api.list('riders');
  for(const [table,key,idField,nameField,old,include]of [['horses','horse-key','horse-id','horse-name',horseOld,()=>true],['riders','rider-key','rider-id','rider-name',riderOld,()=>true]]){
   const wanted=new Map();for(const {trainer,rows}of roster.filter(r=>include(r.trainer)))for(const row of rows){const id=String(row[idField]),prior=old.find(r=>Number(r.fields[idField])===Number(id))?.fields||{},f=wanted.get(id)||{[key]:prior[key]||id,[idField]:Number(id),[nameField]:prior['source-record']?(prior[nameField]??''):(prior[nameField]||row[nameField]||''),name:prior['source-record']?(prior.name??''):(prior.name||row[nameField]||''),'tenant-id':8778,'trainer-id':Number(prior['trainer-id']||trainer.fields['trainer-id']),'first-seen':prior['first-seen']||clock(),'last-seen':clock(),source:prior.source||'people feed; show '+sid,'trainer-ids':prior['trainer-ids']||'',trainers:prior.trainers||[]};f['trainer-ids']=[...new Set([...f['trainer-ids'].split(',').filter(Boolean),String(trainer.fields['trainer-id'])])].sort().join(',');f.trainers=[...new Set([...f.trainers,trainer.id])];wanted.set(id,f);}if(wanted.size)await api.upsert(table,key,[...wanted.values()]);
  }
  const horses=await api.list('horses'),riders=await api.list('riders'),oldEntries=await api.list('entries');
  const reconciled=entrySourceMode==='synced'?consumer.links(synced.rows,{showRecord,rings: ringRows,schedules,horses,riders,trainers}):model.reconcileEntries(oldEntries.filter(r=>Number(r.fields['show-id'])===sid).map(r=>fieldsFor('entries',r.fields)),incoming,successful);
  for(const row of reconciled){row['last-seen']=row.status==='active'&&incoming.some(r=>r['entry-key']===row['entry-key'])?clock():row['last-seen']||clock();row.shows=[showRecord.id];if(entrySourceMode!=='synced')row.schedules=schedules.filter(r=>r.fields['stage-key']===row['schedule-key']).map(r=>r.id);const ring=ringRows.find(r=>r.fields.ring_date===row.date&&Number(r.fields.ring_number)===Number(row['ring-number']));if(ring&&entrySourceMode!=='synced')row['synced-rings']=[ring.id];row.horses=horses.filter(r=>Number(r.fields['horse-id'])===Number(row['horse-id'])).map(r=>r.id);row.riders=riders.filter(r=>Number(r.fields['rider-id'])===Number(row['rider-id'])).map(r=>r.id);row.trainers=trainers.filter(t=>String(row['trainer-ids']).split(',').includes(String(t.fields['trainer-id']))).map(t=>t.id);}
  if(entrySourceMode!=='synced'&&reconciled.length)await api.upsert('entries','entry-key',reconciled);
  if(synced){const linked=consumer.links(synced.rows,{showRecord,rings:ringRows,schedules,horses,riders,trainers});report.entry_sync.links_updated=await consumer.saveLinks(api,synced.local,linked);report.entry_sync.links_verified=linked.length;
   if(entrySourceMode==='shadow'){const actual=reconciled.filter(r=>r.status==='active'),keys=rows=>rows.map(r=>r['entry-key']).sort().join(',');if(keys(actual)!==keys(linked)||actual.some(r=>{const other=linked.find(x=>x['entry-key']===r['entry-key']);return consumer.sourceFields.some(k=>String(r[k]??'')!==String(other?.[k]??''));}))throw Error('Shadow CWF entries differ from wef-entries sync');report.entry_sync.shadow_match=true;}
  }
  // Retain observed rider/horse associations without replacing owner helper fields.
  const riderHorseLinks=[];for(const rider of riders){
   const links=[...new Set([...(rider.fields.horses||[]),...reconciled.filter(row=>row.status==='active'&&(row.riders||[]).includes(rider.id)).flatMap(row=>row.horses||[])])];
   if(links.length)riderHorseLinks.push({'rider-key':rider.fields['rider-key'],horses:links});
  }if(riderHorseLinks.length)await api.upsert('riders','rider-key',riderHorseLinks);
  if(tenantSlicesEnabled){if(entrySourceMode!=='synced'||!synced)throw Error('Tenant slices require synced entry consumption');report.tenant_slices=await require('./tenant-slices').save({api,selected:synced.rows,horses,riders,now:clock()});}
  report.ok=report.errors.length===0;report.rows=reconciled.filter(r=>r.status==='active').length;report.days=dates.map(date=>({date,ok:report.ok,row_count:reconciled.filter(r=>r.date===date&&r.status==='active').length}));
  if(report.ok){for(const table of entrySourceMode==='synced'?['collections']:['entries','collections']){const retired=(await api.list(table)).filter(r=>Number(r.fields['show-id'])!==sid);if(retired.length){await keep('retired-'+table,retired,{shows:[...new Set(retired.map(r=>r.fields['show-id']))],dates:[...new Set(retired.map(r=>r.fields.date).filter(Boolean))]});await api.remove(table,retired.map(r=>r.id));}}}
  const summary=report.trainers.map(t=>`Trainer ${t.trainer_id} (${t.role}): ${t.result}; ${t.rows} day entries${t.error?'; '+t.error:''}`).join('\n')+`\nShow ${sid}; source-selected ${show.selected_date}; ${dates.length} days; ${report.rows} active entries; ${ringRows.length} daily rings; ${helperRows.length} shared helpers.\nStanding helpers: ${report.roster.horses} horses, ${report.roster.riders} riders; ${report.roster.missing_ids} source IDs missing (source-record identity retained).`;
  const sliceSummary=report.tenant_slices?`\nTenant slice: ${report.tenant_slices.active} active team; ${report.tenant_slices.hidden_inactive} hidden for inactive horse/rider; ${report.tenant_slices.other_hidden} outside current team. Collection remains inclusive; personal follow ownership and private-only entries are not exported.`:'';
  const sourceSummary=sliceSummary+`\nEntries source: ${entrySourceMode}; people endpoint fetches: ${report.people_fetches}.`+(synced?` Synced rows: ${synced.proof.synced_rows}; links verified: ${report.entry_sync.links_verified}; source checked: ${synced.proof.checked_at}.`:'');
  await api.upsert('heartbeat','heartbeat-key',[{'heartbeat-key':'CWF','checked-at':clock(),'selected-date':show.selected_date,'show-id':sid,result:report.ok?'Success':'Failed',summary:summary+sourceSummary,errors:report.errors.join('\n')}]);
  await api.upsert('wef-tenant-index','index-key',[{'index-key':indexKey,'last-checked':clock(),'last-result':report.ok?(report.rows?'Success':'No entries'):'Failed'}],settings.SHARED_BASE_ID);
  log={...log,completed:clock(),result:report.ok?(report.rows?'Success':'No entries'):'Failed',rows:report.rows,'days-complete':report.days.filter(d=>d.ok).length,'days-failed':report.days.filter(d=>!d.ok).length,summary:summary+sourceSummary,errors:report.errors.join('\n')};await saveLog();
 }catch(error){report.ok=false;report.errors.push(String(error.message||error));log={...log,completed:clock(),result:'Failed',summary:'Collector stopped; inspect errors. Existing unverified data preserved.',errors:report.errors.join('\n')};if(logReady){try{await saveLog();}catch(e){report.errors.push('Run log save failed: '+e.message);}}else report.errors.push('Run log start outcome unresolved; no second creation attempted.');
  try{await api.upsert('heartbeat','heartbeat-key',[{'heartbeat-key':'CWF','checked-at':clock(),'selected-date':knownShow?.selected_date??null,'show-id':knownShow?Number(knownShow.show_id):null,result:'Failed',summary:knownShow?'Collector stopped; see run-logs.':'Collector stopped before verified show context; see run-logs.',errors:report.errors.join('\n')}]);}catch(e){report.errors.push('Heartbeat failure could not be recorded: '+e.message);}
  if(knownIndex)try{await api.upsert('wef-tenant-index','index-key',[{'index-key':knownIndex,'last-checked':clock(),'last-result':'Failed'}],settings.SHARED_BASE_ID);}catch(e){report.errors.push('Index failure could not be recorded: '+e.message);}
 }
 return{...report,completed:clock()};
}
module.exports={runTenant};
