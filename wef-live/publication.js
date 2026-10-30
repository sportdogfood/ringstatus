'use strict';
const crypto=require('node:crypto');
const {FIELD,TARGETS,transport}=require('./publication-api');
const digest=x=>crypto.createHash('sha256').update(JSON.stringify(x)).digest('hex');
const fields=r=>r?.fields||{};
const raw=s=>{try{return JSON.parse(s||'{}');}catch{throw Error('Malformed source evidence');}};
const positive=n=>Number.isSafeInteger(Number(n))&&Number(n)>0;
const same=(a,b)=>positive(a)&&positive(b)&&Number(a)===Number(b);
const one=(rows,label)=>{if(rows.length!==1)throw Error(`${label}: ${rows.length} matches`);return rows[0];};
const links=(f,name,id)=>Array.isArray(f[name])&&f[name].length===1&&f[name][0]===id;
const fresh=(at,now)=>{const age=Date.parse(now)-Date.parse(at);return Number.isFinite(age)&&age>=-60000&&age<=360000;};
const value=(o,k)=>Object.hasOwn(o,k)?o[k]:null;
const number=(o,k)=>o[k]!==null&&o[k]!==''&&Object.hasOwn(o,k)&&Number.isFinite(Number(o[k]))?Number(o[k]):null;
function observations(input,receipt){
 const out={rings:[],groups:[],classes:[],trips:[]},scope=input.scope;
 function make(level,row,at,values,extra={}){
  const key=row[{'ring':'ring-key','group':'group-key','class':'class-key','trip':'live-entry-key'}[level]];
  const cls=level==='class'?row['class-id']:level==='trip'?Number(row._class.split('|')[2]):null;
  const sourceKinds=level==='ring'?['source-groups','source-widget']:level==='group'?['source-groups']:['class-'+cls];
  const proof=input.sourceArchives.filter(a=>sourceKinds.includes(a.kind));
  if(!proof.length)throw Error('Missing source archive for '+key);
  const component={sourceKey:key,scope:{customer:scope.customer,show:scope.show,date:scope.date},observedAt:at,sourceUpdatedAt:row['source-updated-at']||null,
   collectionKey:[scope.show,scope.date,level==='ring'?'rings':level==='group'?'groups':'class',cls||scope.show].join('|'),
   archive:{file:receipt.file,sha256:receipt.sha256},sourceArchives:proof.map(a=>({kind:a.kind,file:a.file,sha256:a.sha256})),values};
  component.revision=digest(component);return{level,row,component,...extra};
 }
 for(const r of input.rings){const src=raw(r['source-json']),v={};for(const [s,d] of [['livenow','currentGroupIds'],['upcoming','nextGroupIds'],['completed','completedGroupIds']])v[d]=Array.isArray(src[s])?src[s].filter(g=>String(g.day).slice(0,10)===scope.date).map(g=>Number(g.class_group_id)):null;out.rings.push(make('ring',r,r['last-seen'],v));}
 for(const r of input.groups){const src=raw(r['source-json']),v={status:value(src,'status'),isLive:value(src,'is_live'),hasJson:value(src,'has_JSON'),sequence:number(src,'group_sequence'),estimatedStartTime:value(src,'estimated_start_time')};for(const k of ['total','gone','jo_total','jo_gone','r2_total','r2_gone'])v[k]=number(src,k);out.groups.push(make('group',r,r['last-seen'],v));}
 for(const d of input.details){const r=d.row,src=raw(r['source-json']),count=k=>Object.hasOwn(src,k)&&typeof src[k]==='string'?src[k].split(',').filter(Boolean).length:null;
  out.classes.push(make('class',r,d.observedAt,{total:d.entries.length,gone:count('gone_trips'),joGone:count('jo_gone_trips'),r2Gone:count('r2_gone_trips'),currentTripId:number(src,'curr')}));
  for(const e of d.entries){const src=raw(e['source-json']),v={tripId:e['trip-id'],entryId:e['entry-id'],horseId:e['horse-id'],riderId:e['rider-id'],trainerId:e['trainer-id'],orderOfGo:number(src.live,'OOG'),actualOrder:number(src.live,'Actual_OOG'),scratch:value(src.live,'Scr'),gone:number(src.live,'Gone'),position:Object.hasOwn(src.live,'ActualPos')?value(src.live,'ActualPos'):value(src.live,'Pos')};out.trips.push(make('trip',e,d.observedAt,v,{classId:r['class-id'],identity:src.identity}));}
 }
 return out;
}
function plans(lane,input,obs,db){
 const scope=input.scope,prefix=`${scope.customer}|${scope.show}|${scope.date}|`,result=new Map(),issues=[];
 function add(table,record,key,ob,component=ob.component){
  if(!record.id||!key)throw Error('Missing destination identity');
  if(table==='schedules'&&lane==='schedules'&&!key.startsWith(`${scope.show}|${scope.date}|`))throw Error('Schedule key scope conflict');
  const id=table+'|'+record.id;if(!result.has(id))result.set(id,{table,id:record.id,key,expected:record,components:{}});
  const dest=result.get(id);if(dest.components[ob.level]&&dest.components[ob.level].sourceKey!==component.sourceKey)throw Error('Conflicting source components');dest.components[ob.level]=component;
 }
 function attempt(label,fn){try{fn();}catch(e){issues.push(label+': '+e.message);}}
 function ring(ob){const r=ob.level==='ring'?ob.row:input.rings.find(r=>r['ring-key']===ob.row._ring || r['ring-key']===input.classes?.find(c=>c['class-id']===ob.classId)?._ring);if(!r)throw Error('Missing source ring');
  const candidates=db.rings.filter(x=>{const f=fields(x);return lane==='schedules'?same(f.customer_id,scope.customer)&&same(f.show_id,scope.show)&&f.ring_date===scope.date&&same(f.ring_id,r['ring-id'])&&same(f.ring_number,r['ring-number']):f['ring-key']===prefix+r['ring-number']&&same(f['ring-id'],r['ring-id'])&&same(f['ring-number'],r['ring-number']);});return{source:r,dest:one(candidates,'dated ring')};}
 for(const ob of obs.rings)attempt(ob.component.sourceKey,()=>{const {dest}=ring(ob);add('rings',dest,fields(dest)['ring-key'],ob);});
 if(lane==='schedules'){
  for(const ob of obs.groups)attempt(ob.component.sourceKey,()=>{const {dest,source}=ring(ob);const matches=db.schedules.filter(x=>{const f=fields(x);return f.status==='active'&&same(f['show-id'],scope.show)&&f.date===scope.date&&same(f.ring,source['ring-number'])&&same(f['group-id'],ob.row['class-group-id'])&&links(f,'rings',dest.id);});
   if(!matches.length)throw Error('No existing group occurrences');for(const r of matches){const c={...ob.component,values:{status:ob.component.values.status,isLive:ob.component.values.isLive}};c.revision=digest({...c,revision:undefined});add('schedules',r,fields(r)['stage-key'],ob,c);}});
  for(const ob of obs.classes)attempt(ob.component.sourceKey,()=>{const {dest,source}=ring(ob);if(new Set(ob.row._groups).size!==1)throw Error('Ambiguous source group attribution');const r=one(db.schedules.filter(x=>{const f=fields(x);return f.status==='active'&&same(f['show-id'],scope.show)&&f.date===scope.date&&same(f['class-id'],ob.row['class-id'])&&same(f.ring,source['ring-number'])&&links(f,'rings',dest.id)&&ob.row._groups.includes(`${scope.show}|${scope.date}|${f['group-id']}`);}),'class occurrence');const f=fields(r);if(!links(f,'rings',dest.id)||!ob.row._groups.includes(`${scope.show}|${scope.date}|${f['group-id']}`))throw Error('Occurrence ring/group conflict');add('schedules',r,f['stage-key'],ob);});
 }else{
  const cls=new Map();
  for(const ob of obs.groups)attempt(ob.component.sourceKey,()=>{const {dest,source}=ring(ob);const key=prefix+source['ring-number']+'|'+ob.row['class-group-id'];const r=one(db.groups.filter(x=>fields(x)['group-key']===key&&same(fields(x)['group-id'],ob.row['class-group-id'])&&links(fields(x),'ring',dest.id)),'group');add('groups',r,key,ob);});
  for(const ob of obs.classes)attempt(ob.component.sourceKey,()=>{const {dest,source}=ring(ob);const key=prefix+source['ring-number']+'|'+ob.row['class-id'];const r=one(db.classes.filter(x=>fields(x)['class-key']===key&&same(fields(x)['class-id'],ob.row['class-id'])&&links(fields(x),'ring',dest.id)),'class');add('classes',r,key,ob);cls.set(ob.row['class-id'],r);
   attempt(ob.component.sourceKey+' occurrence',()=>{if(new Set(ob.row._groups).size!==1)throw Error('Ambiguous source group attribution');const g=one(db.groups.filter(x=>{const f=fields(x);return ob.row._groups.includes(`${scope.show}|${scope.date}|${f['group-id']}`)&&f['group-key']===prefix+source['ring-number']+'|'+f['group-id']&&links(f,'ring',dest.id);}), 'occurrence group');const s=one(db.schedules.filter(x=>{const f=fields(x);return links(f,'class',r.id)&&links(f,'ring',dest.id)&&links(f,'group',g.id)&&String(f['schedule-key']).startsWith(prefix+source['ring-number']+'|');}),'class occurrence');add('schedules',s,fields(s)['schedule-key'],ob);});
  });
  for(const ob of obs.trips)attempt(ob.component.sourceKey,()=>{const c=cls.get(ob.classId);if(!c)throw Error('No validated destination class');const r=one(db.trips.filter(x=>same(fields(x)['trip-id'],ob.row['trip-id'])&&links(fields(x),'class',c.id)),'trip');const f=fields(r),src=raw(f['source-json']),live=ob.identity;
   if(f['record-status']==='dropped'||!same(src.trip_id,ob.row['trip-id'])||!same(src.entry_id,ob.row['entry-id'])||!same(src.horse_id,ob.row['horse-id']))throw Error('Trip identity conflict');
   if(f['trip-key']!==fields(c)['class-key']+'|'+src.entry_id+'|'+(src.entryxclasses_uuid||src.trip_id))throw Error('Trip key conflict');
   if((f['source-uuid']||'')!==(src.entryxclasses_uuid||'')||(live.entryxclasses_uuid&&live.entryxclasses_uuid!==src.entryxclasses_uuid))throw Error('Trip UUID conflict');
   const differences={};for(const k of ['rider','trainer']){const old=src[k+'_id'],v=ob.row[k+'-id'];if(!positive(old)||!positive(v))throw Error('Missing '+k+' evidence');if(!same(old,v)){if(!Number.isFinite(Date.parse(f['last-success']))||Date.parse(f['last-success'])>Date.parse(ob.component.observedAt)||!f['source-uuid']||f['source-uuid']!==live.entryxclasses_uuid)throw Error('Unresolved '+k+' contradiction');differences[k]={collected:old,observed:v,collectedAt:f['last-success'],sourceUuid:f['source-uuid']};}}
   const component={...ob.component,values:{...ob.component.values,identityDifferences:differences}};component.revision=digest({...component,revision:undefined});add('trips',r,f['trip-key'],ob,component);
  });
 }
 return{plans:[...result.values()].sort((a,b)=>(a.table+a.key).localeCompare(b.table+b.key)),issues};
}
function merge(lane,plan,before,now){
 const old=before?raw(before):null;
 const destination={lane,base:TARGETS[lane].base,table:TARGETS[lane].tables[plan.table],key:plan.key};
 if(old&&(old.format!=='wef-live-observation-v1'||JSON.stringify(old.destination)!==JSON.stringify(destination)))throw Error('Existing observation envelope conflict');
 const next={format:'wef-live-observation-v1',destination,components:{...(old?.components||{})}};
 for(const [level,c] of Object.entries(plan.components)){
  if(!fresh(c.observedAt,now))continue;
  const prior=next.components[level];if(prior){const a=Date.parse(prior.observedAt),b=Date.parse(c.observedAt);if(!Number.isFinite(a))throw Error('Invalid previous observation time');if(a>b)continue;if(a===b){if(prior.revision!==c.revision)throw Error('Same-time observation conflict');continue;}}
  next.components[level]=c;
 }
 if(!Object.keys(next.components).length)return before;
 return JSON.stringify(next);
}
// Compare only publication identity/matching evidence and native links. Acquisition
// timestamps, raw response tokens and other producer fields remain in full audit rows.
function protectedEvidence(lane,plan,record){
 const f=fields(record),out={};
 for(const [k,v] of Object.entries(f)){
  if(k===FIELD)continue;
  if(['customer-id','customer_id','show-id','show_id','ring-id','ring_id','ring-number','group-id','class-id','trip-id','entry-id','horse-id','rider-id','trainer-id','ring-key','stage-key','group-key','class-key','schedule-key','trip-key'].includes(k)||Array.isArray(v)||['date','ring_date','ring_number','ring'].includes(k))out[k]=v;
 }
 if(lane==='schedules'&&plan.table==='schedules')out.status=f.status??null;
 if(plan.table==='trips'){
  out['record-status']=f['record-status']??null;out['source-uuid']=f['source-uuid']??null;
  const src=raw(f['source-json']);out.identity={};
  for(const k of ['trip_id','entry_id','horse_id','rider_id','trainer_id','entryxclasses_uuid'])out.identity[k]=value(src,k);
  if(Object.keys(plan.components.trip?.values.identityDifferences||{}).length)out['last-success']=f['last-success']??null;
 }
 return out;
}
const equal=require('node:util').isDeepStrictEqual;
function validateDestination(lane,plan,before,after,overlay){
 if(after.id!==plan.id||!equal(protectedEvidence(lane,plan,before),protectedEvidence(lane,plan,after)))throw Error('Destination identity, matching evidence or native links changed; defer');
 if((fields(after)[FIELD]||'')!==overlay)throw Error('Publication observation changed; defer');
}
async function publish({input,api=transport(),state=require('./publication-journal').journal(),archive,clock=()=>new Date().toISOString(),authority=async()=>{},maxPatches=60,budgetMs=45000}){
 const result={lanes:{},errors:[]};
 let obs;
 if(input&&input.scope.active&&input.scope.date===require('./model').localDay(clock())){const receipt=await archive('publication-observations',input);obs=observations(input,receipt);}
 await Promise.all(Object.keys(TARGETS).map(async lane=>{
  const stats=result.lanes[lane]={status:'Skipped',planned:0,attempted:0,patched:0,unchanged:0,deferred:0,uncertain:0,pending:0,issues:[]};
  const started=Date.now(),journal=state.data[lane],limit={deadline:started+budgetMs};
  const outcomes=new Map();let items=[],journalWritable=true,dispatches=0;
  const guard=()=>{if(Date.now()>=limit.deadline){const e=Error('Publication lane budget exhausted');e.code='PUBLICATION_BUDGET';throw e;}};
  const exhausted=e=>e?.code==='PUBLICATION_BUDGET'||Date.now()>=limit.deadline;
  const audit=(phase,value)=>archive('publication-'+lane+'-'+phase,{at:clock(),...value});
  const issue=(label,e)=>stats.issues.push(label+': '+e.message);
  // A failed save prevents new unsafe writes in this lane, not other collection
  // or a future scheduled invocation. No immediate persistence retry loop.
  const save=label=>{if(!journalWritable)return false;try{state.save();return true;}catch(e){journalWritable=false;issue(label,e);return false;}};
  const clearPending=key=>{const pending=journal.pending[key];delete journal.pending[key];if(!save('Journal completion')){journal.pending[key]=pending;return false;}return true;};
  try{
   // A conflicting/unavailable record blocks only that record. Preserve its
   // intent while reconciling and publishing other exact matches.
   for(const [key,p] of Object.entries(journal.pending)){
    try{
     guard();const row=await api.get(lane,p.table,p.id,limit);guard();if(row.id!==p.id)throw Error('Pending destination ID mismatch');
     const actual=fields(row)[FIELD]||'',outcome=actual===p.after?'Verified':actual===p.before?'NotApplied':'Conflict';
     await audit('reconcile',{intent:p.intent,table:p.table,id:p.id,outcome,readback:row});
     if(outcome==='Conflict')throw Error('Pending publication conflicts with current value');
     clearPending(key);
    }catch(e){issue('Reconciliation '+key,e);if(exhausted(e))throw e;}
   }
   if(!obs)return;
   guard();await authority(input.scope,limit);guard();
   const db={};for(const table of Object.keys(TARGETS[lane].tables)){guard();db[table]=await api.list(lane,table,input.scope,limit);guard();}
   const planned=plans(lane,input,obs,db);stats.issues.push(...planned.issues);items=planned.plans;stats.planned=items.length;
   if(!items.length)return;
   const cursor=journal.cursor%items.length;
   for(let n=0;n<items.length;n++){
    const index=(cursor+n)%items.length,plan=items[index],key=plan.table+'|'+plan.id;
    if(!journalWritable)break;
    if(Date.now()>=limit.deadline||dispatches>=maxPatches){journal.cursor=index;save('Journal cursor');break;}
    stats.attempted++;
    let preserveCursor=false,dispatched=false,checked;
    try{
     if(journal.pending[key]){outcomes.set(key,'uncertain');continue;}
     guard();const row=await api.get(lane,plan.table,plan.id,limit);guard();validateDestination(lane,plan,plan.expected,row,fields(plan.expected)[FIELD]||'');
     const before=fields(row)[FIELD]||'',after=merge(lane,plan,before,clock());
     if(!after||after===before){outcomes.set(key,'unchanged');stats.unchanged++;continue;}
     if(input.scope.date!==require('./model').localDay(clock()))throw Error('TODAY changed before publication');
     const intent=await audit('intent',{destination:{lane,table:plan.table,id:plan.id,key:plan.key},before:row,after,source:plan.components});
     journal.pending[key]={table:plan.table,id:plan.id,before,after,intent};
     if(!save('Journal intent')){preserveCursor=true;outcomes.set(key,'deferred');break;}
     const beforeDispatch=()=>{guard();const now=clock();if(input.scope.date!==require('./model').localDay(now))throw Error('TODAY changed before dispatch');const previous=before?raw(before).components:{};for(const [level,c]of Object.entries(raw(after).components))if(c.revision!==previous[level]?.revision&&!fresh(c.observedAt,now))throw Error('Observation expired before dispatch');};
     beforeDispatch();dispatched=true;dispatches++;await api.patch(lane,plan.table,plan.id,after,{...limit,beforeDispatch});
     guard();checked=await api.get(lane,plan.table,plan.id,limit);
     validateDestination(lane,plan,row,checked,after);
     await audit('outcome',{intent,readback:checked,expected:after,outcome:'Verified'});
     outcomes.set(key,'patched');stats.patched++;
     clearPending(key);
    }catch(e){
     issue(plan.table+'/'+plan.key,e);preserveCursor=exhausted(e);
     if(journal.pending[key]){
      outcomes.set(key,dispatched?'uncertain':'deferred');
      try{await audit('outcome',{intent:journal.pending[key].intent,outcome:dispatched?'Unknown':'NotDispatched',error:e.message,...(checked?{readback:checked}:{})});}catch(a){issue('Outcome audit',a);}
     }else outcomes.set(key,'deferred');
    }finally{
     journal.cursor=preserveCursor?index:(index+1)%items.length;
     save('Journal cursor');
    }
    if(preserveCursor||!journalWritable)break;
   }
  }catch(e){issue('Lane',e);}
  finally{
   // Derive counts even when a journal/audit failure exits before normal tail.
   // Pending records already verified here stay verified, never double counted.
   stats.pending=Object.keys(journal.pending).length;
   for(const plan of items){const key=plan.table+'|'+plan.id;if(outcomes.get(key)==='uncertain'||(!outcomes.has(key)&&journal.pending[key]))stats.uncertain++;}
   stats.deferred=Math.max(0,stats.planned-stats.patched-stats.unchanged-stats.uncertain);
   stats.status=stats.issues.length?'Partial':stats.uncertain?'Partial':stats.deferred?'Deferred':stats.planned?'Success':'Skipped';
   for(const message of stats.issues)result.errors.push({lane,message});
  }
 }));
 return result;
}
module.exports={publish,observations,plans,merge,fresh};
