'use strict';
const {evaluateClassTiming,instant}=require('./class-timing');
const {ageReason,hash,POLICY,schedulePace}=require('./pace');
const {TIMING,SYNC_GROUPS,syncSample}=require('./api');
const DATA='appdl16HJ5ETm8N69', LIVE='appgzosUcVqA4vZdf', SHOWS='appCaSmL819KK7UFJ';
const zone='America/New_York';
const formatter=new Intl.DateTimeFormat('en-CA',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});
function localInstant(date,time){
 const match=/^(\d{2}):(\d{2}):(\d{2})$/.exec(time||'');
 if(!/^\d{4}-\d{2}-\d{2}$/.test(date||'')||!match||+match[1]>23||+match[2]>59||+match[3]>59)return null;
 const target=date+'T'+time,base=Date.parse(target+'Z');if(!Number.isFinite(base))return null;
 const local=ms=>{const p=Object.fromEntries(formatter.formatToParts(ms).map(p=>[p.type,p.value]));return p.year+'-'+p.month+'-'+p.day+'T'+p.hour+':'+p.minute+':'+p.second;};
 const offsets=new Set([-86400000,0,86400000].map(d=>Date.parse(local(base+d)+'Z')-(base+d)));
 const matches=[...offsets].map(o=>base-o).filter(ms=>local(ms)===target);
 return matches.length===1?new Date(matches[0]).toISOString():null;
}
const positive=n=>Number.isSafeInteger(n)&&n>0;
const sha=s=>typeof s==='string'&&/^[a-f0-9]{64}$/.test(s);
const key=i=>[i.show,i.date,i.ring,i.group,i.class].join('|');
const member=r=>({'stage-key':r['stage-key'],'class-id':r['class-id']??null,'start-time':r['start-time']??null,'end-time':r['end-time']??null,'total-trips':r['total-trips']??null,type:r.type??null});
const sorted=rows=>[...rows].sort((a,b)=>String(a['stage-key']).localeCompare(String(b['stage-key'])));
const refCore=r=>Object.fromEntries(Object.entries(r).filter(([k])=>k!=='observed_at'));

// Validate the producer's existing ring-pace-v1 serialization and original evidence.
function paceEvidence(rows,identity,group,inventory,now,issues){
 const scope=['data_live',identity.show,identity.date,identity.ring,identity.group,'base'].join('|');
 const candidates=rows.filter(r=>r.fields?.['scope-key']===scope);
 if(!candidates.length)return null;
 try{
  if(candidates.some(r=>instant(r.fields['evaluated-at'])===null))throw Error('evaluation_time');
  candidates.sort((a,b)=>instant(b.fields['evaluated-at'])-instant(a.fields['evaluated-at']));
  const record=candidates[0],f=record.fields;
  if(candidates[1]?.fields['evaluated-at']===f['evaluated-at']&&candidates[1].fields['content-hash']!==f['content-hash'])throw Error('ambiguous_latest');
  const p=JSON.parse(f['payload-json']),refs=JSON.parse(f['input-refs-json']);
  if(f['answer-kind']!=='ring-pace'||f['source-lane']!=='data_live'||f.phase!=='base'||p.phase!=='base'||p.schema_version!==1
   ||!['customer','show','date','ring','group'].every(k=>p.identity?.[k]===identity[k])
   ||f['show-id']!==identity.show||f.date!==identity.date||f['ring-number']!==identity.ring||f['group-id']!==identity.group)throw Error('identity');
  const progress=p.group_progress;
  let final=false;
  if(progress?.quality==='final'){
   const synced=inventory.synced_live,ref=progress.ref;
   if(!synced||ref?.base!==TIMING||ref.table!==SYNC_GROUPS)throw Error('final_source_missing');
   const found=synced.groups.filter(r=>r.id===ref.record);if(found.length!==1)throw Error('final_source_identity');
   const verified=syncSample(found[0],synced.rings,{customer:identity.customer,show:identity.show,day:identity.date,now});
   if(verified.status!=='Completed'||hash(verified.ref)!==hash(ref)||verified.at!==progress.observed_at||verified.total!==progress.total||verified.gone!==progress.completed
    ||verified.identity.ring!==identity.ring||verified.identity.group!==identity.group)throw Error('final_source_content');
   final=true;
  }
  const validClock=(at,limit)=>{const reason=ageReason(at,now,limit);return reason===null||(final&&reason==='stale_observation');};
  const method=p.pace_method,limit=method==='observed_interval'?600000:1800000;
  if(!['observed_interval','schedule_window'].includes(method)||method!==f['pace-method']
   ||f.availability!==(method==='observed_interval'?'available':'partial')
   ||!Number.isFinite(p.selected_pace_minutes_per_trip)||p.selected_pace_minutes_per_trip<=0
   ||f['pace-minutes-per-trip']!==Math.round(p.selected_pace_minutes_per_trip*10000)/10000
   ||instant(f['evaluated-at'])>instant(now)||!validClock(f['observed-at'],limit))throw Error('pace_or_freshness');
  if(!Array.isArray(refs)||!refs.length||refs.some(r=>{
   if(![DATA,LIVE,SHOWS,TIMING].includes(r.base))return true;
   if(r.base===TIMING&&!validSyncRef(r,identity,now))return true;
   const reason=r.base===SHOWS?require('../wef-shows/authority-age').showAuthorityReason({last_checked:r.observed_at,authority_checked_at:r.authority_checked_at},now):ageReason(r.observed_at,now,r.base===LIVE?600000:1800000);
   return reason!==null && !(final&&[DATA,LIVE,TIMING].includes(r.base)&&reason==='stale_observation') && !(method==='observed_interval' && r.base===DATA && reason==='stale_observation');
  }))throw Error('supporting_freshness');
  const sourceRefs=refs.filter(r=>r.base===DATA);
  if(!sourceRefs.length||sourceRefs.some(r=>r.observed_at!==inventory.observed_at)
   ||hash(sourceRefs.map(refCore).sort((a,b)=>a.key.localeCompare(b.key)))!==hash(inventory.refs.map(refCore).sort((a,b)=>a.key.localeCompare(b.key))))throw Error('data_revision');
  if(hash(sorted(p.schedule_members))!==hash(sorted(group.rows.map(member))))throw Error('occurrence_membership');
  if(!sha(f['content-hash'])||hash({policy:POLICY,payload:p,inputRefs:refs,observedAt:inventory.observed_at})!==f['content-hash'])throw Error('content_hash');
  if(typeof f['answer-key']!=='string'||!f['answer-key'].startsWith(scope+'|')
   ||!(f['answer-key']===scope+'|'+f['content-hash']||f['answer-key'].endsWith('|'+f['content-hash'].slice(0,16))))throw Error('answer_key');
  if(method==='observed_interval'){
   if(final&&p.final_transition){
    const q=p.final_transition.before,ref=progress.ref,raw=ref.material.group.fields;
    const classes=JSON.parse(raw['source-json']).classes.map(c=>`${identity.show}|${identity.date}|${Number(c)}`).sort();
    if(p.final_transition.completed_at!==progress.observed_at||q.status!=='Underway'||hash(q.classes)!==hash(classes)||q.total!==progress.total||!Number.isSafeInteger(q.gone)||q.gone<0||q.gone>q.total||!validEndpointRef(q.ref,identity,now,q)
     ||instant(q.at)===null||instant(q.at)>instant(progress.observed_at)||(q.at===progress.observed_at&&q.gone!==progress.completed)
     ||q.membership!==hash({members:q.classes,ring:identity.ring,gid:identity.group,status:q.status})
     ||p.interval.endpoints.some(e=>e.membership!==q.membership)||instant(p.interval.to)>instant(q.at))throw Error('final_transition');
   }
   const v=p.interval;
   if(!v||v.to!==f['observed-at']||!Array.isArray(v.endpoints)||v.endpoints.length!==2
    ||!validClock(v.from,600000)||!validClock(v.to,600000))throw Error('interval_evidence');
   const [a,b]=v.endpoints,dt=instant(b.at)-instant(a.at),dn=b.gone-a.gone;
   if(a.at!==v.from||b.at!==v.to||dt<=0||dn<=0||dn!==v.new_completions||a.total!==b.total
    ||a.membership!==b.membership||dt/60000/dn!==p.selected_pace_minutes_per_trip
    ||p.observed_interval_pace_minutes_per_trip!==p.selected_pace_minutes_per_trip
    ||v.endpoints.some(e=>!positive(e.total)||!Number.isSafeInteger(e.gone)||e.gone<0||e.gone>e.total
     ||!validEndpointRef(e.ref,identity,now,e)||e.ref.observed_at!==e.at||!sha(e.ref.fingerprint))
    ||!(refs.some(r=>[LIVE,TIMING].includes(r.base)&&r.fingerprint===b.ref.fingerprint&&r.observed_at===b.at) || (p.last_proven_interval?.to===v.to && hash(p.last_proven_interval.endpoints)===hash(v.endpoints) && refs.some(r=>[LIVE,TIMING].includes(r.base)&&r.fingerprint===p.group_progress?.ref?.fingerprint&&r.observed_at===p.group_progress?.observed_at))))throw Error('interval_counts');
  }else if(f['observed-at']!==inventory.observed_at||p.schedule_pace_minutes_per_trip!==p.selected_pace_minutes_per_trip
    ||schedulePace(group.rows).value!==p.selected_pace_minutes_per_trip)throw Error('schedule_pace');
  return{identity:{...p.identity,phase:p.phase},source_lane:'data_live',observed_at:f['observed-at'],
   valid_until:new Date(Date.parse(f['observed-at'])+limit).toISOString(),
   ref:{source_key:scope,revision:f['content-hash'],base:'appYOykKIy1kAquhP',table:'timing-answers',record:record.id,input_refs:refs},
   lifecycle:final?'final':'active_or_unknown',final_verified:final,answer_key:f['answer-key'],member_class_ids:[...new Set(p.schedule_members.map(r=>r['class-id']).filter(positive))],
   pace_minutes_per_trip:p.selected_pace_minutes_per_trip,method};
 }catch(error){issues.push('ring_pace_'+error.message);return null;}
}

// Input is the verified data-input.py projection; the worker owns authority
// rereads and revision publication, including A -> B -> A.
function adapt({lane='data_live',now,data,paceRows=[],liveClasses=new Map(),schedules,onIssue}){
 if(lane!=='data_live'||schedules!=null)throw Error('Only Data+Live class timing is supported');
 if(instant(now)===null||data?.source_lane!=='data_live'||!data.context||!data.proof)throw Error('Data authority missing');
 const c=data.context;
 if(!positive(c['customer-id'])||!positive(c['show-id'])||c['show-key']!==c['customer-id']+'|'+c['show-id']
  ||data.proof['show-key']!==c['show-key']||!sha(data.proof['state-sha256'])||!data.proof['run-key'])throw Error('Data authority mismatch');
 const today=require('./purge').localDay(now);
 if(today<c['start-date']||today>c['end-date']||!data.inventories?.[today])throw Error('TODAY Data date coverage missing');
 const records=[],seen=new Set(),multiplicity=new Map(),occurrences=new Map(),unknownClassGroups=new Set();
 for(const inv of Object.values(data.inventories))for(const g of inv.groups||[])for(const r of g.rows||[])if(r&&typeof r['stage-key']==='string')occurrences.set(r['stage-key'],(occurrences.get(r['stage-key'])||0)+1);
 const reject=(identity,message)=>{if(!onIssue)throw Error(message);onIssue(identity,message);};
 for(const [date,inventory] of Object.entries(data.inventories)){
  if(date!==today)continue;
  if(date<c['start-date']||date>c['end-date']||inventory.inventory_complete!==true||!Array.isArray(inventory.groups)
   ||!Array.isArray(inventory.refs)||!inventory.refs.length||inventory.refs.some(r=>r.base!==DATA||!r.key||!sha(r.sha256)||(!r.record&&!r.archive)))throw Error('Data inventory evidence invalid');
  for(const group of inventory.groups){
   const i=group.identity;
   if(i.customer!==c['customer-id']||i.show!==c['show-id']||i.date!==date||!positive(i.ring)||!positive(i.group)||!Array.isArray(group.rows))throw Error('Data group identity mismatch');
   for(const row of group.rows){
    if(['schooling','break'].includes(String(row.type).toLowerCase()))continue;
    // Count the complete roster before rejecting occurrence identities. Skipping
    // malformed rows must not turn an ambiguous Live match into a unique one.
    if(positive(row['class-id'])){const fullKey=key({...i,class:row['class-id']});multiplicity.set(fullKey,(multiplicity.get(fullKey)||0)+1);}
    else unknownClassGroups.add(key({...i,class:null}));
    if(!positive(row['class-id'])||typeof row['stage-key']!=='string'||!row['stage-key']||/\|position-\d+$/.test(row['stage-key'])){reject({...i,class:row['class-id']??null,occurrence:row['stage-key']??null},'Exact Data class occurrence missing');continue;}
    if(occurrences.get(row['stage-key'])>1){reject({...i,class:row['class-id'],occurrence:row['stage-key']},'Duplicate Data occurrence');continue;}
    if(seen.has(row['stage-key']))throw Error('Duplicate Data occurrence');seen.add(row['stage-key']);
    const identity={...i,class:row['class-id'],occurrence:row['stage-key'],phase:'base'};
    records.push({identity,row,group,inventory});
   }
  }
 }
 return records.map(({identity,row,group,inventory})=>{
  const start=localInstant(identity.date,row['start-time']),issues=[];
  const at=instant(inventory.observed_at);
  const dataEvidence={identity,source_lane:lane,observed_at:inventory.observed_at,
   valid_until:at===null?'':new Date(at+1800000).toISOString(),
   ref:{base:DATA,table:'collections',source_key:identity.occurrence,revision:hash(inventory.refs),parts:inventory.refs,authority:c,producer_proof:data.proof},
   ...(start?{start:{at:start,kind:'estimated'}}:{}),
   ...(group.rows.length===1?{counts:{scope:'class',total:row['total-trips'],completed:row['completed-trips']??null,completion_scope:'unknown'}}:{})};
  let live=liveClasses.get(key(identity))??null;
  if(live){
   if(ageReason(inventory.observed_at,now,1800000)||multiplicity.get(key(identity))!==1||unknownClassGroups.has(key({...identity,class:null}))||!['customer','show','date','ring','group','class'].every(k=>live.identity?.[k]===identity[k])){
    issues.push('live_occurrence_ambiguous');live=null;
   }else live={...live,identity:{...live.identity,occurrence:identity.occurrence,phase:'base'},source_lane:lane,
    ref:{...live.ref,occurrence_match:{stage_key:identity.occurrence,method:'unique_class_group_in_complete_data_inventory',parts:inventory.refs}}};
  }
  const ringPace=paceEvidence(paceRows,identity,group,inventory,now,issues);
  return evaluateClassTiming({identity,sourceLane:lane,timezone:zone,now,data:dataEvidence,live,ringPace,issues});
 });
}
module.exports={adapt,localInstant,paceEvidence};

function validSyncRef(ref,identity,now){try{
 if(ref.base!==TIMING||ref.table!==SYNC_GROUPS||!ref.material||ref.material.group.id!==ref.record)return false;
 const sample=syncSample(ref.material.group,[ref.material.ring],{customer:identity.customer,show:identity.show,day:identity.date,now});
 return sample.identity.ring===identity.ring&&sample.identity.group===identity.group&&hash(sample.ref)===hash(ref);
}catch{return false;}}
function validEndpointRef(ref,identity,now,endpoint){if(ref?.base===LIVE)return sha(ref.fingerprint);if(!validSyncRef(ref,identity,now))return false;const sample=syncSample(ref.material.group,[ref.material.ring],{customer:identity.customer,show:identity.show,day:identity.date,now});return sample.total===endpoint.total&&sample.gone===endpoint.gone&&sample.membership===endpoint.membership;}
