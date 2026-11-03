'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const model=require('./model'),sources=require('./source'),settings=require('./settings');
const {store}=require('./lane-store'),membership=require('./membership');
const LANES=['groups','rings','classes'];
const clean=r=>Object.fromEntries(Object.entries(r).filter(([k])=>!k.startsWith('_')&&k!=='scope-status'));
const {inWindow}=require('./live-cadence');
function read(file){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code==='ENOENT')return null;throw e;}}
function write(file,value){const tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(value,null,2));fs.renameSync(tmp,file);}
async function lane(name,{
 api=require('./airtable').airtable(),get=sources.get,getStatus=require('./activity').readStatus,
 clock=()=>new Date().toISOString(),runtime=settings.RUNTIME,
 archive=(kind,data,scope)=>require('./archive').archiveFile(path.join(runtime,'history'),kind,data,scope),
 alert, showContext,
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
  // Day 0 is selected by Shows; no status/count/age gate controls collection.
  try{
   if(showContext){r.scope={...showContext};r.day_priority=0;}
   else{
   const shows=await api.list('shows',settings.SHOWS_BASE);
   const defaults=shows.filter(x=>Number(x.fields.customer_id)===15&&x.fields.is_default===true);
   if(defaults.length!==1)throw Error('Expected one default wef-shows record');
   const f=defaults[0].fields,show=model.integer(f.show_id,'show ID');
   const days=await api.list('show-days',settings.SHOWS_BASE);
   const matches=days.filter(x=>x.fields['show-day-key']?.startsWith(`15|${show}|`)&&Number(x.fields['day-priority'])===0&&x.fields['day-priority']!==null&&x.fields['day-priority']!==undefined&&x.fields['day-priority']!=='');
   if(matches.length!==1)throw Error('Expected one canonical day-priority 0');
   const date=model.day(matches[0].fields.date)||model.day(matches[0].fields['show-day-key'].split('|')[2]);
   if(!date||matches[0].fields['show-day-key']!==`15|${show}|${date}`)throw Error('Invalid canonical day-0 identity');
   r.scope={show,customer:15,date,authority:defaults[0].id,checked_at:f.last_checked,active:true};r.day_priority=0;
   }
  }catch(e){issue('Shows context: '+e.message);return r;}
  try{r.live_status=require('./activity').statusValue(await getStatus({customer:15}));}
  catch(e){issue('Live-status check: '+e.message);}
  if(name==='groups'){
   const {data,received}=await collect('groups',sources.endpoints.groups);await evidence('source-groups',data);
   const selected=Array.isArray(data)?data.filter(s=>s&&Number(s.show_id)===r.scope.show):[];
   if(!Array.isArray(data)||selected.length>1){issue('Unreadable/ambiguous source show');return r;}
   if(!selected.length&&data.length){issue('Source show mismatch');return r;}
   if(selected.length&&!Array.isArray(selected[0].json_data)){issue('Missing groups array');return r;}
   const inventory=membership.appearances(data,r.scope);
   if(!inventory.complete)issue('Incomplete group/class membership; absent states unchanged');
   const rings=scoped(await db.list('live-rings')),groups=[],refs=new Map();
   for(const raw of selected[0]?.json_data||[]){
    if(!raw||typeof raw!=='object'){issue('Unreadable group record');continue;}
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
   // Membership labels never filter discovery, source fetching or detail saving.
   const priorGroups=scoped(await db.list('live-groups'));
   const groupStates=membership.reconcile(priorGroups.map(x=>({...x.fields,key:x.fields['group-key']})),inventory.groups,{complete:inventory.complete,scope:r.scope});
   const mapped=new Map(groups.map(g=>[g['group-key'],g]));
   const presentGroups=new Set(inventory.groups.map(g=>g.key));
   const groupRows=groupStates.map(g=>({...mapped.get(g.key),'group-key':g.key,'record-status':g['record-status'],...(presentGroups.has(g.key)?{'show-id':r.scope.show,date:r.scope.date,'class-group-id':g['group-id'],'last-seen':received}:{})}));
   await save('live-groups','group-key',groupRows,received);
   const savedGroups=scoped(await db.list('live-groups')),savedClasses=scoped(await db.list('live-classes'));
   const priorAppearances=scoped(await db.list('live-class-appearances'));
   const appearanceStates=membership.reconcile(priorAppearances.map(x=>({...x.fields,key:x.fields['appearance-key']})),inventory.classes,{complete:inventory.complete,scope:r.scope});
   const currentAppearances=new Set(inventory.classes.map(a=>a.key));
   const appearanceRows=appearanceStates.map(a=>{
    const row={'appearance-key':a.key,'record-status':a['record-status']};
    if(currentAppearances.has(a.key)){
     Object.assign(row,{'show-id':r.scope.show,date:r.scope.date,'class-group-id':a['group-id'],'class-id':a['class-id'],'last-seen':received});
     const group=lookup(savedGroups,'group-key',model.key(r.scope.show,r.scope.date,a['group-id']));
     const cls=lookup(savedClasses,'class-key',model.key(r.scope.show,r.scope.date,a['class-id']));
     if(group)row.group=[...new Set([...(a.group||[]),group])];
     if(cls)row.class=[...new Set([...(a.class||[]),cls])];
    }
    return row;
   });
   await save('live-class-appearances','appearance-key',appearanceRows,received);
   r.counts['live-groups']=inventory.groups.length;r.counts['live-class-appearances']=inventory.classes.length;
   r.membership={complete:inventory.complete,groups:inventory.groups.length,class_appearances:inventory.classes.length,unique_classes:new Set(inventory.classes.map(a=>a['class-id'])).size};
   shared={scope:r.scope,observed_at:received,classes:[...refs.values()],class_appearances:inventory.classes.length};
  }else if(name==='rings'){
   const {data,received}=await collect('rings',sources.endpoints.widget);await evidence('source-widget',data);
   const selected=Array.isArray(data)?data.filter(s=>s&&Number(s.show_id)===r.scope.show):[];
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
   const rings=scoped(await db.list('live-rings')),groups=scoped(await db.list('live-groups'));
   const references=latest.shared.classes,results=[];let next=0,successfulPairs=0;
   await Promise.all(Array.from({length:Math.min(4,references.length)},async()=>{
    while(next<references.length){const c=references[next++];
     try{
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
   const priorClasses=scoped(await db.list('live-classes'));
   const rows=results.map(({c})=>{const ring=lookup(rings,'ring-key',c._ring),links=c._groups.map(k=>lookup(groups,'group-key',k)),prior=priorClasses.find(x=>x.fields['class-key']===c['class-key']);return{...clean(c),...(ring?{ring:[ring]}:{}),...(links.every(Boolean)?{groups:[...new Set([...(prior?.fields.groups||[]),...links])]}:{})};});
   if(references.length&&!successfulPairs)throw Error('No class source collections succeeded');
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
  const summary=JSON.stringify({lane:name,skipped:r.skipped,day_priority:r.day_priority,live_status:r.live_status,membership:r.membership,timings:Object.fromEntries(Object.entries(r.timings).filter(([k])=>!['requests','saves'].includes(k))),counts:r.counts,changed_records:r.changes.length,issues:r.issues.length,diffs_and_coverage:receipt});
  await log('run-logs','run-id',[{'run-id':r.run_id,started:r.started,completed:r.completed,result,'show-id':r.scope?.show||null,date:r.scope?.date||'',summary,errors:r.stage_errors.map(x=>x.message).join('\n'),'ring-count':r.counts['live-rings']||0,'group-count':r.counts['live-groups']||0,'class-count':r.counts['live-class-appearances']??r.counts['live-classes']??0,'entry-count':r.counts['live-entries']||0}]);
  await log('run-logs','run-id',r.changes.map((change,i)=>({'run-id':r.run_id+'|diff|'+i,started:r.started,completed:r.completed,result:'Success','show-id':r.scope?.show||null,date:r.scope?.date||'',summary:JSON.stringify({lane:name,parent_run_id:r.run_id,...change})})));
  const errors=[...r.issues,...r.stage_errors,...r.logging_errors.map(message=>({message}))];
  await log('errs-log','error-key',errors.map((e,i)=>({'error-key':r.run_id+'|'+i,'run-id':r.run_id,'occurred-at':r.completed,'show-id':r.scope?.show||null,date:r.scope?.date||'',message:name+': '+e.message,'err-class':e.class_id||null,'err-trip':e.trip_id||null,'err-group':e.group_id||null})));
  await log('heartbeat','heartbeat-key',[{'heartbeat-key':r.run_id,'run-id':r.run_id,started:r.started,completed:r.completed,'checked-at':r.completed,result,summary}]);
  try{write(path.join(runtime,'lane-'+name+'-'+r.run_id+'.json'),r);write(path.join(runtime,'lane-'+name+'-last-run.json'),r);}catch{}
 }
 return r;
}
async function scheduled(options={}){
 const policy=await require('./live-cadence').check(options);
 // Lanes start together; each owns its receipt and catches its own stage failures.
 const reports=!policy.due?[]:policy.collect?await Promise.all(LANES.map(name=>lane(name,{...options,showContext:policy.scope,getStatus:async()=>policy.live_status}))):[policy];
 // Cleanup is a separate post-collection action; publication remains on hold.
 let cleanupResult;
 try{const result=await (options.cleanup||require('./cleanup').cleanup)({...options,checkShow:policy.due,protectedRuns:reports.map(r=>r.run_id)});
  cleanupResult={run_id:result.run_id,result:result.result};
  if(reports[0])reports[0].cleanup={run_id:result.run_id,result:result.result};
 }catch(e){cleanupResult={result:'Error',error:e.message};if(reports[0])reports[0].cleanup=cleanupResult;}
 if(!policy.due)return{skipped:'next-check-not-due',next_check_at:policy.next_check_at,cleanup:cleanupResult};
 return reports;
}
module.exports={LANES,lane,scheduled,inWindow};
