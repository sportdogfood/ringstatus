'use strict';
const path=require('node:path');
const {LIVE}=require('./api');
const {archive,collectionToken}=require('./runner');
const {ageReason,digest,hash}=require('./pace');
const root=path.resolve(__dirname,'../wef-live/runtime/history');
const positive=n=>Number.isSafeInteger(n)&&n>0;
const one=(rows,label)=>{if(rows.length!==1)throw Error(label);return rows[0];};

// Read-only projection. No start/completion is inferred from a group's status.
// Returned identity intentionally stops at class+group: the Data adapter must
// prove unique occurrence correspondence against its complete inventory.
async function readLiveClasses({api,customer,show,now,readArchive=archive}){
 if(!positive(customer)||!positive(show)||!Number.isFinite(Date.parse(now)))throw Error('Invalid Live scope');
 const filter='{show-id}='+show;
 const tables=['live-classes','live-groups','collections'],before={};
 for(const table of tables)before[table]=await api.list(LIVE,table,filter);
 const result=new Map(),issues=[],blocked=new Set(),cache=new Map();
 const classKeys=new Map();for(const r of before['live-classes'])classKeys.set(r.fields['class-key'],(classKeys.get(r.fields['class-key'])||0)+1);
 async function dataset(c,kind,id,date){
  const f=c.fields;
  if(f['show-id']!==show||f.status!=='complete'||f['source-kind']!==kind||String(f['source-id'])!==String(id)
   ||f.date!==date||ageReason(f.completed,now,600000)||!Number.isSafeInteger(f['row-count'])||f['row-count']<0
   ||!/^[a-f0-9]{64}$/.test(f.fingerprint||''))throw Error('collection_scope');
  if(!cache.has(c.id)){
   const rows=await readArchive(f['archive-file'],root,f.fingerprint);
   if(!Array.isArray(rows)||rows.length!==f['row-count']||digest(JSON.stringify(rows))!==f.fingerprint)throw Error('archive_coverage_or_hash');
   cache.set(c.id,rows);
  }
  return cache.get(c.id);
 }
 for(const record of before['live-classes']){
  const f=record.fields;let k=null;
  if(f['scope-status']!=='current'||f['detail-available']!==true||f['show-id']!==show||f['customer-id']!==customer)continue;
  try{
   if(classKeys.get(f['class-key'])!==1)throw Error('duplicate_class');
   if(ageReason(f['last-seen'],now,600000))throw Error('class_stale');
   const groupIds=f.groups;
   if(!Array.isArray(groupIds)||groupIds.length!==1)throw Error('ambiguous_group');
   const g=one(before['live-groups'].filter(r=>r.id===groupIds[0]),'group_link'),gf=g.fields;
   if(gf['scope-status']!=='current'||gf.date!==f.date||gf['show-id']!==show||gf['customer-id']!==customer
    ||ageReason(gf['last-seen'],now,600000))throw Error('group_scope');
   const rawGroup=JSON.parse(gf['source-json']),ring=Number(rawGroup.ring_number),gid=gf['class-group-id'],cid=f['class-id'];
   if(!positive(ring)||!positive(gid)||!positive(cid)||f['class-key']!==[show,f.date,cid].join('|'))throw Error('identity');
   k=[show,f.date,ring,gid,cid].join('|');
   if(blocked.has(k)||result.has(k)){blocked.add(k);result.delete(k);throw Error('duplicate_class');}
   if(!Array.isArray(gf.collection)||gf.collection.length!==1)throw Error('group_collection_link');
   const gc=one(before.collections.filter(r=>r.id===gf.collection[0]),'group_collection');
   const groups=await dataset(gc,'groups',show,f.date);
   if(Date.parse(gf['last-seen'])>Date.parse(gc.fields.completed))throw Error('group_observation_time');
   const frozen=one(groups.filter(r=>r['group-key']===gf['group-key']),'group_archive_identity');
   for(const name of ['show-id','customer-id','date','class-group-id','source-json','last-seen','scope-status'])
    if(hash(frozen[name]??null)!==hash(gf[name]??null))throw Error('group_projection');
   if(!Array.isArray(frozen._classes)||!frozen._classes.includes(f['class-key'])
    ||new Set(frozen._classes).size!==frozen._classes.length)throw Error('class_membership');
   if(!Array.isArray(f.collection)||f.collection.length!==1)throw Error('collection_link');
   const c=one(before.collections.filter(r=>r.id===f.collection[0]),'class_collection');
   const rows=await dataset(c,'class',cid,f.date);
   if(rows.length!==f.total||Date.parse(f['last-seen'])>Date.parse(c.fields.completed))throw Error('class_projection');
   const ids=new Set(),goneIds=[];
   for(const row of rows){
    const raw=JSON.parse(row['source-json']);
    if(row['show-id']!==show||row['customer-id']!==customer||row.date!==f.date||row['scope-status']!=='current'
     ||row._class!==f['class-key']||!positive(row['trip-id'])||ids.has(row['trip-id'])
     ||Number(raw.live?.class_id)!==cid||Number(raw.live?.id)!==row['trip-id']
     ||Number(raw.identity?.trip_id)!==row['trip-id']||Number(raw.live?.Gone)!==row.gone
     ||ageReason(row['last-seen'],now,600000)||Date.parse(row['last-seen'])>Date.parse(c.fields.completed))throw Error('class_archive_identity');
    ids.add(row['trip-id']);if(row.gone===1)goneIds.push(String(row['trip-id']));
   }
   const raw=JSON.parse(f['source-json']);let completed=null;
   if(Object.hasOwn(raw,'gone_trips')){
    if(typeof raw.gone_trips!=='string')throw Error('class_counts');
    const values=raw.gone_trips.split(',').filter(Boolean);
    if(new Set(values).size!==values.length||values.some(v=>!ids.has(Number(v)))
     ||hash([...values].sort())!==hash(goneIds.sort())||f.gone!==values.length)throw Error('class_counts');
    completed=values.length;
   }
   const multipleRounds=['jo_total','r2_total','jo_gone','r2_gone'].some(n=>Number(rawGroup[n])>0)
    ||['jo_gone_trips','r2_gone_trips'].some(n=>typeof raw[n]==='string'&&raw[n].split(',').filter(Boolean).length>0);
   const times=[f['last-seen'],gf['last-seen'],...rows.map(r=>r['last-seen'])];
   const observed=times.sort((a,b)=>Date.parse(a)-Date.parse(b))[0];
   result.set(k,{identity:{customer,show,date:f.date,ring,group:gid,class:cid},source_lane:'data_live',
    observed_at:observed,valid_until:new Date(Date.parse(observed)+600000).toISOString(),
    counts:{scope:'class',total:rows.length,completed,completion_scope:multipleRounds?'phase':'unknown'},
    ref:{base:LIVE,table:'live-classes',record:record.id,source_key:f['class-key'],
     revision:hash({class:c.fields.fingerprint,group:gc.fields.fingerprint,header:f['source-json']}),source_header:f['source-json'],source_header_observed_at:f['last-seen'],
     parts:[{collection:c.id,fingerprint:c.fields.fingerprint,archive:c.fields['archive-file']},
      {collection:gc.id,fingerprint:gc.fields.fingerprint,archive:gc.fields['archive-file']}]}});
  }catch(error){if(k){blocked.add(k);result.delete(k);}issues.push({class_id:f['class-id'],reason:error.message});}
 }
 // Fence all consumed mutable projections, not only their collection pointers.
 for(const table of tables)if(collectionToken(before[table])!==collectionToken(await api.list(LIVE,table,filter))){const error=Error('Live '+table+' changed during read');error.code='SOURCE_CHANGED';throw error;}
 return{classes:result,issues};
}
module.exports={readLiveClasses};
