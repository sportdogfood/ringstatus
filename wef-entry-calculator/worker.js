'use strict';
const {adapt}=require('../wef-timing/class-adapter');
const {archive}=require('../wef-timing/runner');
const {hash,digest}=require('../wef-timing/pace');
const {evaluate,fresh,orderValue}=require('./calculator');
const {localDay}=require('../wef-timing/purge');
const path=require('node:path');
const {components:classComponents}=require('./class-proof');
const positive=n=>Number.isSafeInteger(n)&&n>0;
const integer=v=>typeof v==='number'?v:typeof v==='string'&&/^\d+$/.test(v)?Number(v):null;
const flag=v=>[0,'0',false].includes(v)?false:[1,'1',true].includes(v)?true:null;
const groupKey=i=>[i.show,i.date,i.ring,i.group,i.class].join('|');
function latest(rows){
 const map=new Map();
 for(const row of rows){const f=row.fields,k=f['scope-key'],p=map.get(k);
  if(p&&p.fields['evaluated-at']===f['evaluated-at']&&p.fields['content-hash']!==f['content-hash'])throw Error('Ambiguous latest entry/class answer');
  if(!p||f['evaluated-at']>p.fields['evaluated-at'])map.set(k,row);
 }return map;
}
async function prepare({data,liveClasses=new Map(),classRows,paceRows,answers=[],now,runId,readArchive=archive,excludedGroups=new Set()}){
 if(!runId||data?.source_lane!=='data_live'||!data.context||!Array.isArray(data.entry_classes))throw Error('Entry current-day Data projection missing');
 const today=localDay(now);
 if(today<data.context['start-date']||today>data.context['end-date'])throw Error('Entry TODAY outside show dates');
 const inventory=data.inventories?.[today];
 if(!inventory||!Array.isArray(inventory.groups)||!(excludedGroups instanceof Set))throw Error('Entry inventory contract missing');
 const issues=[],outputs=[],writes=[];let unchanged=0;
 // Count the complete raw inventory BEFORE adapt rejects malformed or duplicate
 // occurrences. A rejected second occurrence must not create a unique match.
 const occurrences=new Map(),unknownGroups=new Set(),groupCounts=new Map(),stageCounts=new Map();
 const groupScope=i=>JSON.stringify([i?.customer,i?.show,i?.date,i?.ring,i?.group]);
 for(const group of inventory.groups){
  const i=group?.identity;
  if(i)groupCounts.set(groupScope(i),(groupCounts.get(groupScope(i))||0)+1);
  for(const row of Array.isArray(group?.rows)?group.rows:[]){
   if(typeof row?.['stage-key']==='string')stageCounts.set(row['stage-key'],(stageCounts.get(row['stage-key'])||0)+1);
   if(!i)continue;
   if(positive(row?.['class-id'])){const k=JSON.stringify([i.customer,i.show,i.date,i.ring,row['class-id'],i.group]);occurrences.set(k,(occurrences.get(k)||0)+1);}
   else if(!['break','schooling'].includes(String(row?.type).toLowerCase()))unknownGroups.add(JSON.stringify([i.ring,i.group]));
  }
 }
 const safeGroups=[];
 for(const group of inventory.groups){
  const i=group?.identity;
  const valid=i&&i.customer===data.context['customer-id']&&i.show===data.context['show-id']&&i.date===today
   &&positive(i.ring)&&positive(i.group)&&Array.isArray(group.rows)
   &&group.rows.every(row=>row&&typeof row==='object'&&!Array.isArray(row));
  const duplicate=i&&groupCounts.get(groupScope(i))>1;
  if(excludedGroups.has(group)||!valid||duplicate){
   issues.push({identity:i??null,reason:duplicate?'entry_duplicate_group_excluded':'entry_malformed_or_excluded_group'});continue;
  }
  safeGroups.push(group);
 }
 const scoped={...data,inventories:{[today]:{...inventory,groups:safeGroups}}};
 const classes=adapt({data:scoped,now,paceRows,liveClasses,onIssue:(identity,reason)=>issues.push({identity,reason})});
 const savedClasses=latest(classRows.filter(r=>r.fields['answer-kind']==='class-timing'&&r.fields['source-lane']==='data_live'));
 const previous=latest(answers.filter(r=>r.fields['answer-kind']==='entry-timing'&&r.fields['source-lane']==='data_live'));
 for(const cls of classes){
  const i=cls.payload.identity;
  try{
  const k=JSON.stringify([i.customer,i.show,i.date,i.ring,i.class]),reasons=[];
  const matches=data.entry_classes.filter(c=>JSON.stringify(c.identity)===k);
  if(matches.length!==1){issues.push({identity:i,reason:'entry_class_snapshot_missing_or_ambiguous'});continue;}
  const s=matches[0],p=s.payload,related=p.class_related_data;
  // Bind class detail only to its explicit group and a unique stage in that group.
  if(occurrences.get(JSON.stringify([i.customer,i.show,i.date,i.ring,i.class,i.group]))!==1||stageCounts.get(i.occurrence)!==1||unknownGroups.has(JSON.stringify([i.ring,i.group]))||integer(related.class_group_id)!==i.group){issues.push({identity:i,reason:'entry_occurrence_ambiguous'});continue;}
  if(!Array.isArray(p.trips)||p.total_entry_trips!==p.trips.length||!Array.isArray(s.refs)||!s.refs.length
   ||s.refs.some(r=>r.base!=='appdl16HJ5ETm8N69'||!r.key||!/^[a-f0-9]{64}$/.test(r.sha256)||(!r.record&&!r.archive)))throw Error('Entry retained coverage/provenance mismatch');
  const seen=new Set();let rows=[];
  for(const t of p.trips){const entry=t.entry_id,uuid=t.entryxclasses_uuid,tid=integer(t.trip_id);
   if(!positive(entry)||(!uuid&&!positive(tid))||(uuid&&(typeof uuid!=='string'||uuid!==uuid.trim()||!uuid)))throw Error('Entry source trip identity missing');
   const sourceId=uuid?'uuid:'+uuid:'trip:'+tid;
   if(seen.has(sourceId))throw Error('Entry source trip repeated');seen.add(sourceId);
   const key=[data.context['show-key'],today,i.ring,i.class,entry,uuid||tid].join('|');
   rows.push({key,entry,tripId:tid,horse:t.horse_id,rider:t.rider_id,order:orderValue(t.order_of_go),actual:orderValue(t.actual_order),rawOrder:t.order_of_go??null,rawActual:t.actual_order??null,providerEstimate:{raw:t.estimated_go_time??null,observed_at:s.observed_at,ref:{source_key:k,revision:hash(s.refs),base:'appdl16HJ5ETm8N69',parts:s.refs}},scratch:flag(t.scratch_trip),gone:flag(t.gone_in)});
  }
  let source='data',observedAt=s.observed_at;
  const refs=[{base:'appdl16HJ5ETm8N69',source_key:k,revision:hash(s.refs),observed_at:s.observed_at,parts:s.refs,producer_proof:data.proof},
   {role:'occurrence',source_key:i.occurrence,revision:hash(scoped.inventories[today].refs),observed_at:scoped.inventories[today].observed_at,parts:scoped.inventories[today].refs}];
  const live=liveClasses.get(groupKey(i));
  if(live&&live.source_lane==='data_live'&&['customer','show','date','ring','group','class'].every(k=>live.identity?.[k]===i[k])
   &&fresh(live.observed_at,now,600000)&&fresh(scoped.inventories[today].observed_at,now,1800000)){
   const part=live.ref.parts[0],frozen=await readArchive(part.archive,path.resolve(__dirname,'../wef-live/runtime/history'),part.fingerprint);
   if(!Array.isArray(frozen)||digest(JSON.stringify(frozen))!==part.fingerprint)throw Error('Entry Live archive hash mismatch');
   const byId=new Map(frozen.map(r=>[r['trip-id'],r]));
   const compatible=byId.size===frozen.length&&frozen.length===rows.length&&new Set(rows.map(r=>r.tripId)).size===rows.length&&rows.every(r=>{
    const f=byId.get(r.tripId);if(!f)return false;const raw=JSON.parse(f['source-json']);
    return f['entry-id']===r.entry&&raw.identity?.entry_id===r.entry&&raw.identity?.trip_id===r.tripId
     &&f['horse-id']===r.horse&&f['rider-id']===r.rider&&integer(raw.live?.id)===r.tripId
     &&integer(raw.live?.class_id)===i.class&&f._class===[i.show,today,i.class].join('|')
     &&f['order-of-go']===integer(raw.live.OOG)&&f['actual-order']===integer(raw.live.Actual_OOG)
     &&flag(f.scratch)===flag(raw.live.Scr)&&flag(f.gone)===flag(raw.live.Gone);
   });
   if(compatible){
    rows=rows.map(r=>{const f=byId.get(r.tripId);return{...r,order:orderValue(f['order-of-go']),actual:orderValue(f['actual-order']),rawOrder:f['order-of-go'],rawActual:f['actual-order'],scratch:flag(f.scratch),gone:flag(f.gone)};});
    source='live';observedAt=live.observed_at;refs.push({...live.ref,role:'live_trip_roster',observed_at:live.observed_at});
   }else reasons.push('live_trip_roster_mismatch');
  }else reasons.push('live_trip_roster_missing_or_stale');
  const saved=savedClasses.get(cls.fields['scope-key']);
  const verified=classComponents(saved,cls,now),classVerified=verified!==null;
  if(classVerified)refs.push({base:'appeqxxJyPGS1iqdm',table:'tblmoBFySYW2TkDHf',record:saved.id,source_key:saved.fields['answer-key'],revision:saved.fields['content-hash'],observed_at:saved.fields['observed-at']});
  else reasons.push('class_readback_missing_or_changed');
  if(!fresh(scoped.inventories[today].observed_at,now,1800000))reasons.push('occurrence_stale');
  // Planned queue and provider estimates do not require an invented phase anchor.
  // Derived phase-specific ETA remains conditional on independently proved evidence.
  for(const target of rows){const output=evaluate({identity:{...i,entry:target.entry,trip:target.key},now,rows,targetKey:target.key,
   observedAt,refs,classTiming:verified,issues:reasons,source});
   outputs.push(output);const f=output.fields;
   if(previous.get(f['scope-key'])?.fields['content-hash']===f['content-hash'])unchanged++;
   else{f['answer-key']=f['scope-key']+'|'+runId+'|'+f['content-hash'].slice(0,16);writes.push(f);}
  }
  }catch(error){issues.push({identity:i,reason:'entry_scope_invalid: '+error.message});}
 }
 return{outputs,writes,unchanged,issues,counts:{entries_evaluated:outputs.length,entry_writes_planned:writes.length,entries_written:0,entries_unchanged:unchanged,
  entries_available:outputs.filter(o=>o.fields.availability==='available').length,entries_partial:outputs.filter(o=>o.fields.availability==='partial').length,
  entries_unavailable:outputs.filter(o=>o.fields.availability==='unavailable').length,entry_scopes_skipped:issues.length}};
}
module.exports={prepare};
