'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const model=require('./model'),sources=require('./source'),settings=require('./settings');
const {store}=require('./lane-store');
const LANES=['groups','rings','classes'];
const clean=r=>Object.fromEntries(Object.entries(r).filter(([k])=>!k.startsWith('_')&&k!=='scope-status'));
function inWindow(now){
 const f=new Intl.DateTimeFormat('en-GB',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(new Date(now));
 return f>='05:00'&&f<='19:00';
}
function read(file){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code==='ENOENT')return null;throw e;}}
function write(file,value){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(value,null,2));fs.renameSync(tmp,file);}
async function lane(name,{
 api=require('./airtable').airtable(),get=sources.get,getStatus=require('./activity').readStatus,
 clock=()=>new Date().toISOString(),runtime=settings.RUNTIME,
 archive=(kind,data,scope)=>require('./archive').archiveFile(path.join(runtime,'history'),kind,data,scope),
 alert,
}={}){
 const r={lane:name,run_id:crypto.randomUUID(),started:clock(),ok:true,issues:[],stage_errors:[],logging_errors:[],changes:[],coverage:[],archives:[],timings:{collection_ms:0,save_ms:0},counts:{}};
 const issue=(message,ids={})=>r.issues.push({message,...ids});
 const db=store(api,settings.BASE_ID,issue),sharedFile=path.join(runtime,'lane-'+name+'-state.json');
 let previous={records:{}},shared,collectionStart;
 try{previous=read(sharedFile)||previous;if(!previous.records||typeof previous.records!=='object')previous.records={};}catch(e){issue('Shared state unavailable: '+e.message);}
 fs.mkdirSync(runtime,{recursive:true});
 const logIds=new Map(),uncertainLogs=new Set();
 async function log(table,key,fields){
  for(let i=0;i<fields.length;i+=10){const batch=fields.slice(i,i+10).filter(f=>!uncertainLogs.has(table+'|'+f[key])),existing=new Set(batch.filter(f=>logIds.has(table+'|'+f[key])).map(f=>f[key]));
   for(const method of ['POST','PATCH']){const selected=batch.filter(f=>existing.has(f[key])===(method==='PATCH'));if(!selected.length)continue;
    // UUID run keys are new, so logging never inventories accumulated old logs.
    try{const reply=await api.call(settings.BASE_ID,table,method,{records:selected.map(f=>({...(method==='PATCH'?{id:logIds.get(table+'|'+f[key])}:{}),fields:f}))});
     for(const f of selected){const saved=reply.records?.find(x=>x.fields?.[key]===f[key]);if(!saved)throw Error('Incomplete log response');logIds.set(table+'|'+f[key],saved.id);}
    }catch(e){for(const f of selected)uncertainLogs.add(table+'|'+f[key]);r.logging_errors.push(table+': '+e.message);}
   }
  }
 }
 async function stageFailure(stage,e){
  const failure={lane:name,run_id:r.run_id,stage,message:e.message,at:clock()};r.stage_errors.push(failure);r.ok=false;
  // The deployment must supply an established transport; a local receipt is NOT delivery.
  try{if(!alert)throw Error('Records chat alert transport not configured');await alert(failure);failure.alert='Delivered';}
  catch(a){failure.alert='Undelivered';failure.alert_error=a.message;}
 }
 async function collect(label,url,decoded=false){
  const start=clock();collectionStart??=performance.now();r.timings.collection_started||=start;
  try{const data=await get(url,decoded);const received=clock();return{data,received};}
  finally{const end=clock();r.timings.collection_ms=performance.now()-collectionStart;r.timings.collection_completed=end;(r.timings.requests||=[]).push({label,started:start,completed:end});}
 }
 async function evidence(kind,data){try{r.archives.push(await archive(kind,data,{shows:[r.scope.show],dates:[r.scope.date]}));}catch(e){issue('Source evidence '+kind+': '+e.message);}}
 async function save(table,key,rows,observedAt){
  const start=clock(),begin=performance.now();r.timings.save_started||=start;
  try{
   const result=await db.save(table,key,rows);r.changes.push(...result.changes);
   if(result.attempted&&!result.covered.length)await stageFailure('save '+table,Error('No attempted records confirmed saved'));
   for(const k of result.covered){const changed=result.changes.some(c=>c.key===k);const prior=previous.records?.[table+'|'+k];
    const stampAt=typeof observedAt==='function'?observedAt(k):observedAt;
    const stamp={table,key:k,last_success:stampAt,last_changed:changed?stampAt:prior?.last_changed||null};
    r.coverage.push(stamp);previous.records[table+'|'+k]=stamp;
   }
   r.counts[table]=(r.counts[table]||0)+result.covered.length;
   (r.timings.saves||=[]).push({table,started:start,completed:clock(),rows:result.covered.length,changed:result.changes.length,
    source_to_save:result.covered.map(k=>{const at=typeof observedAt==='function'?observedAt(k):observedAt;return{key:k,source_response_at:at,elapsed_ms:Date.parse(clock())-Date.parse(at)};})});
   return result;
  }finally{r.timings.save_ms+=performance.now()-begin;r.timings.save_completed=clock();}
 }
 const scoped=rows=>rows.filter(x=>Number(x.fields['show-id'])===r.scope.show&&x.fields.date===r.scope.date);
 const lookup=(rows,key,k)=>{const found=rows.filter(x=>x.fields[key]===k);if(found.length===1)return found[0].id;issue('Missing/ambiguous shared '+key+': '+k);return null;};
 try{
  await log('heartbeat','heartbeat-key',[{'heartbeat-key':r.run_id,'run-id':r.run_id,started:r.started,'checked-at':r.started,result:'Running',summary:name+' collection started'}]);
  const live=await getStatus({customer:15});
  if(!require('./activity').statusValue(live)){r.skipped='live-status-false';return r;}
  // Keep the proven canonical show/TODAY calculation. Invalid context is a logged check,
  // never an excuse to invent an identity or collect another day.
  try{r.scope=model.scope(await api.list('shows',settings.SHOWS_BASE),clock());}
  catch(e){issue('Shows context: '+e.message);r.skipped='show-context-unavailable';return r;}
  try{const days=await api.list('show-days',settings.SHOWS_BASE),key=`15|${r.scope.show}|${r.scope.date}`;
   const matches=days.filter(x=>x.fields['show-day-key']===key);if(matches.length===1)r.day_priority=matches[0].fields['day-priority'];else issue('Missing/ambiguous shared show-day priority');
  }catch(e){issue('Show-day priority: '+e.message);}
  if(!r.scope.active){r.skipped='no-live-day-today';return r;}
  if(name==='groups'){
   const {data,received}=await collect('groups',sources.endpoints.groups);await evidence('source-groups',data);
   const selected=Array.isArray(data)?data.filter(s=>Number(s.show_id)===r.scope.show):[];
   if(!Array.isArray(data)||selected.length>1){issue('Unreadable/ambiguous source show');return r;}
   if(!selected.length){if(data.length)issue('Source show mismatch');shared={scope:r.scope,observed_at:received,classes:[]};return r;}
   if(!Array.isArray(selected[0].json_data)){issue('Missing groups array');return r;}
   const rings=scoped(await db.list('live-rings')),groups=[],refs=new Map();
   for(const raw of selected[0].json_data){
    if(model.day(raw.day)!==r.scope.date){if(!model.day(raw.day))issue('Group date missing');continue;}
    try{
     const g={...raw},indices=[];
     for(let i=0;i<(Array.isArray(g.classes)?g.classes:[]).length;i++){
      if(Number.isSafeInteger(Number(g.classes[i]))&&Number(g.classes[i])>0&&g.classNumbers?.[i]!==undefined&&g.classNames?.[i]!==undefined)indices.push(i);
      else issue('Incomplete class reference in group '+g.class_group_id);
     }
     if(!Array.isArray(g.classes))issue('Missing class references for group '+g.class_group_id);
     g.classes=indices.map(i=>raw.classes[i]);g.classNumbers=indices.map(i=>raw.classNumbers[i]);g.classNames=indices.map(i=>raw.classNames[i]);
     const p=model.groups([{show_id:r.scope.show,json_data:[g]}],r.scope,received),row=p.groups[0];row['source-json']=JSON.stringify(raw);
     for(const [field,source]of Object.entries({'group-name':'group_name','group-sequence':'group_sequence',status:'status','is-live':'is_live','has-json':'has_JSON','estimated-start-time':'estimated_start_time',total:'total',gone:'gone','jo-total':'jo_total','jo-gone':'jo_gone','r2-total':'r2_total','r2-gone':'r2_gone'})){
      if(raw[source]===undefined||raw[source]===null||row[field]===null)delete row[field];
     }
     const ring=lookup(rings,'ring-key',row._ring);
     // Class lane owns class.groups; Airtable maintains group.classes reciprocally.
     // Groups saves source class references, never races that native association.
     groups.push({...clean(row),...(ring?{ring:[ring]}:{})});
     for(const c of p.classes){const prior=refs.get(c['class-key']);if(prior&&prior._ring!==c._ring){issue('Conflicting class ring '+c['class-id']);continue;}
      if(prior){prior._groups=[...new Set([...prior._groups,...c._groups])];prior['detail-available']||=c['detail-available'];}else refs.set(c['class-key'],c);
     }
    }catch(e){issue(e.message,{group_id:Number(raw.class_group_id)||null});}
   }
   await save('live-groups','group-key',groups,received);shared={scope:r.scope,observed_at:received,classes:[...refs.values()],all_completed:require('./activity').allCompleted(groups)};
  }else if(name==='rings'){
   const {data,received}=await collect('rings',sources.endpoints.widget);await evidence('source-widget',data);
   const selected=Array.isArray(data)?data.filter(s=>Number(s.show_id)===r.scope.show):[];
   if(selected.length!==1){if(data?.length)issue('Widget show missing/ambiguous');return r;}
   const live=selected[0].live_data;if(!live||typeof live!=='object'){issue('Missing widget ring data');return r;}
   const groups=scoped(await db.list('live-groups')),old=scoped(await db.list('live-rings')),rows=[];
   for(const [number,raw]of Object.entries(live)){
    try{
     const all=['livenow','upcoming','completed'].flatMap(k=>Array.isArray(raw[k])?raw[k]:[]).filter(g=>model.day(g.day)===r.scope.date);
     const ids=[...new Set(all.map(g=>Number(g.ring_id)).filter(n=>Number.isSafeInteger(n)&&n>0))];
     const previousRing=old.filter(x=>Number(x.fields['ring-number'])===Number(number));
     const rid=ids.length===1?ids[0]:ids.length===0&&previousRing.length===1?previousRing[0].fields['ring-id']:null;
     if(!rid){issue('Missing/conflicting ring ID '+number);continue;}
     const row={...model.common(r.scope,received,raw),'ring-key':model.key(r.scope.show,r.scope.date,rid),'ring-id':rid,'ring-number':model.integer(number,'ring number'),'ring-name':raw.name||all[0]?.ring||''};
     delete row['scope-status'];
     for(const [src,dest]of [['livenow','current-group'],['upcoming','next-group'],['completed','completed-groups']]){
      if(!Array.isArray(raw[src])){if(raw[src]!==undefined)issue('Invalid widget '+src+' for ring '+number);continue;}
      const matches=raw[src].filter(g=>model.day(g.day)===r.scope.date),links=matches.map(g=>lookup(groups,'group-key',model.key(r.scope.show,r.scope.date,g.class_group_id)));
      if(links.every(Boolean))row[dest]=links;
     }rows.push(row);
    }catch(e){issue(e.message);}
   }await save('live-rings','ring-key',rows,received);
  }else if(name==='classes'){
   let latest;try{latest=read(path.join(runtime,'lane-groups-state.json'));}catch(e){issue('Shared class references unreadable: '+e.message);return r;}
   if(latest?.shared?.scope?.show!==r.scope.show||latest.shared.scope.date!==r.scope.date||!Array.isArray(latest.shared.classes)){issue('No matching saved class references');return r;}
   if(latest.shared.all_completed){r.skipped='all-groups-completed';return r;}
   const rings=scoped(await db.list('live-rings')),groups=scoped(await db.list('live-groups'));
   const references=latest.shared.classes,results=[];let next=0,successfulPairs=0;
   await Promise.all(Array.from({length:Math.min(4,references.length)},async()=>{
    while(next<references.length){const c=references[next++];
     try{
      if(!c['detail-available']){results.push({c,entries:[],received:latest.shared.observed_at});continue;}
      const pair=await Promise.allSettled([collect('class-'+c['class-id'],sources.endpoints.detail(r.scope,c['class-id'])),collect('identity-'+c['class-id'],sources.endpoints.decoded(r.scope,c['class-id']),true)]);
      const failed=pair.filter(x=>x.status==='rejected');if(failed.length){for(const f of failed)issue(f.reason.message,{class_id:c['class-id']});continue;}
      successfulPairs++;
      const [live,decoded]=pair.map(x=>x.value),payload=live.data,identity=decoded.data;
      await evidence('class-'+c['class-id'],{detail:payload,decoded:identity});
      // Validate class scope independently, then isolate each malformed/unresolved trip.
      const empty=model.classDetail({...payload,rows:[],recs:0},identity,r.scope,c,live.received),entries=[];
      if(!Array.isArray(payload.rows)||Number(payload.recs)!==payload.rows.length){issue('Incomplete class detail',{class_id:c['class-id']});continue;}
      for(const row of payload.rows){try{
       const observed=model.classDetail({...payload,rows:[row],recs:1},identity,r.scope,c,live.received).entries[0];
       for(const [field,source]of Object.entries({'entry-number':'ENo','horse-name':'Hor','rider-name':'Rid','order-of-go':'OOG','actual-order':'Actual_OOG','scratch':'Scr','gone':'Gone'}))if(row[source]===undefined||row[source]===null||observed[field]===null)delete observed[field];
       if(row.ActualPos===undefined&&row.Pos===undefined)delete observed.position;
       for(const field of ['horse-id','rider-id','trainer-id'])if(observed[field]===null)delete observed[field];
       if(!observed['trainer-name'])delete observed['trainer-name'];
       entries.push(observed);
      }catch(e){issue(e.message,{class_id:c['class-id'],trip_id:e.trip_id});}}
      for(const [field,source]of [['gone','gone_trips'],['jo-gone','jo_gone_trips'],['r2-gone','r2_gone_trips']])if(typeof payload[source]!=='string')delete empty.fields[field];
      if(payload.curr===undefined||payload.curr===null)delete empty.fields['current-trip-id'];
      empty.fields['source-json']=JSON.stringify(Object.fromEntries(Object.entries(payload).filter(([k])=>k!=='rows')));
      results.push({c:{...c,...empty.fields,total:payload.rows.length,'last-seen':live.received},entries,received:live.received});
     }catch(e){issue(e.message,{class_id:c['class-id']});}
    }
   }));
   const rows=results.map(({c})=>{const ring=lookup(rings,'ring-key',c._ring),links=c._groups.map(k=>lookup(groups,'group-key',k));return{...clean(c),...(ring?{ring:[ring]}:{}),...(links.every(Boolean)?{groups:links}:{})};});
   if(references.some(c=>c['detail-available'])&&!successfulPairs)throw Error('No class source collections succeeded');
   await save('live-classes','class-key',rows,k=>results.find(x=>x.c['class-key']===k).received);
   const classes=await db.list('live-classes');
   const entries=[];for(const result of results){const id=lookup(classes,'class-key',result.c['class-key']);if(!id)continue;
    entries.push(...result.entries.map(e=>({...clean(e),class:[id]})));
   }
   await save('live-entries','live-entry-key',entries,k=>entries.find(e=>e['live-entry-key']===k)['last-seen']);
  }
 }catch(e){await stageFailure('collection/save',e);}
 finally{
  r.completed=clock();r.timings.total_ms=Date.parse(r.completed)-Date.parse(r.started);
  try{write(sharedFile,{records:previous.records,shared:shared||previous.shared,last_run:r.run_id});write(path.join(runtime,'lane-'+name+'-'+r.run_id+'.json'),r);write(path.join(runtime,'lane-'+name+'-last-run.json'),r);}catch(e){r.logging_errors.push('Local receipt: '+e.message);}
  const result=!r.ok?'Failed':r.skipped?'Skipped':r.issues.length?'Partial':'Success';
  const receipt=path.join(runtime,'lane-'+name+'-'+r.run_id+'.json');
  const summary=JSON.stringify({lane:name,skipped:r.skipped,day_priority:r.day_priority,timings:Object.fromEntries(Object.entries(r.timings).filter(([k])=>!['requests','saves'].includes(k))),counts:r.counts,changed_records:r.changes.length,issues:r.issues.length,diffs_and_coverage:receipt});
  await log('run-logs','run-id',[{'run-id':r.run_id,started:r.started,completed:r.completed,result,'show-id':r.scope?.show||null,date:r.scope?.date||'',summary,errors:r.stage_errors.map(x=>x.message).join('\n'),'ring-count':r.counts['live-rings']||0,'group-count':r.counts['live-groups']||0,'class-count':r.counts['live-classes']||0,'entry-count':r.counts['live-entries']||0}]);
  await log('run-logs','run-id',r.changes.map((change,i)=>({'run-id':r.run_id+'|diff|'+i,started:r.started,completed:r.completed,result:'Success','show-id':r.scope?.show||null,date:r.scope?.date||'',summary:JSON.stringify({lane:name,parent_run_id:r.run_id,...change})})));
  const errors=[...r.issues,...r.stage_errors,...r.logging_errors.map(message=>({message}))];
  await log('errs-log','error-key',errors.map((e,i)=>({'error-key':r.run_id+'|'+i,'run-id':r.run_id,'occurred-at':r.completed,'show-id':r.scope?.show||null,date:r.scope?.date||'',message:name+': '+e.message,'err-class':e.class_id||null,'err-trip':e.trip_id||null,'err-group':e.group_id||null})));
  await log('heartbeat','heartbeat-key',[{'heartbeat-key':r.run_id,'run-id':r.run_id,started:r.started,completed:r.completed,'checked-at':r.completed,result,summary}]);
  try{write(path.join(runtime,'lane-'+name+'-'+r.run_id+'.json'),r);write(path.join(runtime,'lane-'+name+'-last-run.json'),r);}catch{}
 }
 return r;
}
async function scheduled(options={}){
 const now=(options.clock||(()=>new Date().toISOString()))();if(!inWindow(now))return{skipped:'outside-window'};
 // Lanes start together; each owns its receipt and catches its own stage failures.
 return Promise.all(LANES.map(name=>lane(name,options)));
}
module.exports={LANES,lane,scheduled,inWindow};
