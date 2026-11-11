'use strict';
// Pure saved-input calculation. No acquisition, business writes or task execution.
const {instant}=require('../wef-timing/class-timing');
const {hash}=require('../wef-timing/pace');
const {orderValue,providerTime}=require('../wef-entry-calculator/calculator');
const BASE='appyoLTcNmLrgpHcg',PACE=2.5746;
const ids={trips:'tbl2OsXe57Pv4EHja',classes:'tblCtRRwOTEP4jqBZ',schedules:'tblYnbZti8uJcHQvm',ring:'tblFABwxZsbm4angZ',class:'tblVfzKwjQk5xRdr2',live:'tbl5ath3kHx0n7Z55'};
const link=(table,row)=>row?`https://airtable.com/${BASE}/${ids[table]}/${row.id}`:null;
const links=v=>Array.isArray(v)?v.map(x=>typeof x==='string'?x:x.id):[];
const active=r=>String(r.fields['record-status']||'').toLowerCase()==='active';
const flag=v=>[true,1,'1'].includes(v)?true:[false,0,'0'].includes(v)?false:null;
const add=(at,minutes)=>instant(at)===null?null:new Date(instant(at)+minutes*60000).toISOString();
const max=(a,b)=>instant(a)!==null&&instant(b)!==null?(instant(a)>instant(b)?a:b):a||b||null;
const material=r=>Object.fromEntries(Object.entries(r).filter(([k])=>!['snapshot-key','snapshot-at','previous-snapshot','changes'].includes(k)));
// Existing synced Class answers use class-timing-v1 payloads. Validate original material.
function classAnswer(row,i,at){
 try{
  const f=row.fields,p=JSON.parse(f['payload-json']),v={...p};
  delete v.evaluated_at;delete v.minutes_until_start;delete v.minutes_since_start;
  v.input_refs=p.input_refs.map(({producer_proof,...r})=>r);
  if(p.policy!=='class-timing-v1'||p.source_lane!=='data_live'||hash(v)!==f['content-hash']
   ||!['customer','show','date','ring','group','class','phase'].every(k=>p.identity[k]===i[k])
   ||instant(p.evaluated_at)===null||instant(p.evaluated_at)>instant(at)
   ||instant(f['observed-at'])===null||instant(f['observed-at'])>instant(at)
   ||!p.input_refs.length||p.input_refs.some(r=>!r.source_key||!r.revision||instant(r.observed_at)===null||instant(r.observed_at)>instant(at)))return null;
  return {row,start:p.start_at,end:p.actual_completed_at||p.estimated_end_at,phase:p.identity.phase};
 }catch{return null;}
}
function snapshots({tables,customer,show,day,at,previous=[]}){
 if(!Number.isSafeInteger(customer)||customer<=0||!Number.isSafeInteger(show)||show<=0||!/^\d{4}-\d{2}-\d{2}$/.test(day)||instant(at)===null)throw Error('Explicit show/day/evaluation time required');
 const input=n=>tables[n]||[],prefix=`${customer}|${show}|${day}|`;
 const trips=input('wef-data.trips').filter(r=>active(r)&&r.fields['trip-key']?.startsWith(prefix));
 const classes=input('wef-data.classes').filter(r=>active(r)&&r.fields['class-key']?.startsWith(prefix));
 const schedules=input('wef-data.schedules').filter(r=>active(r)&&r.fields['schedule-key']?.startsWith(prefix));
 const within=r=>r.fields['customer-id']===customer&&r.fields['show-id']===show&&r.fields.date===day;
 const liveClasses=input('wef-live.classes').filter(within),liveTrips=input('wef-live.entries').filter(within);
 const out=[];
 for(const t of trips){
  const f=t.fields,key=f['trip-key'],reasons=[];
  const same=trips.filter(r=>r.fields['trip-key']===key);
  if(same.length!==1)throw Error('Duplicate trip key');
  const cc=classes.filter(r=>r.fields['class-key']===f.class),c=cc.length===1?cc[0]:null;
  const parts=key.split('|'),ring=Number(parts[3]),classId=Number(parts[4]),group=Number(c?.fields.group?.split('|')[4]);
  const identity={customer,show,date:day,ring,group,class:classId,phase:'base'};
  if(!c||!Number.isSafeInteger(group)||group<=0||f.class!==parts.slice(0,5).join('|'))reasons.push('class_identity_missing_or_ambiguous');
  const ss=schedules.filter(r=>r.fields.class===f.class),schedule=ss.length===1?ss[0]:null;
  if(!schedule)reasons.push('schedule_missing_or_multiple_appearances');
  const lc=liveClasses.filter(r=>r.fields['class-id']===classId),liveClass=lc.length===1?lc[0]:null;
  const matching=liveTrips.filter(r=>String(r.fields['trip-id'])===String(f['trip-id'])&&Number(r.fields['entry-id'])===Number(parts[5])&&links(r.fields.class).includes(liveClass?.id));
  let source='data',observed=f['last-success'],order=orderValue(f['order-of-go']),actual=orderValue(f['actual-order']),scratch=flag(f.scratch),gone=flag(f['gone-in']),live=null;
  if(matching.length===1&&instant(matching[0].fields['last-seen'])!==null&&instant(matching[0].fields['last-seen'])>=instant(observed)){
   live=matching[0];const lf=live.fields;source='live';observed=lf['last-seen'];order=orderValue(lf['order-of-go']);actual=orderValue(lf['actual-order']);scratch=lf.scratch===true;gone=flag(lf.gone);
  }else if(matching.length>1)reasons.push('live_trip_ambiguous');
  if(instant(observed)===null||instant(observed)>instant(at))reasons.push('observation_missing_or_future');
  const roster=trips.filter(r=>r.fields.class===f.class).map(r=>{
   const peers=liveTrips.filter(l=>String(l.fields['trip-id'])===String(r.fields['trip-id'])&&Number(l.fields['entry-id'])===Number(r.fields['trip-key'].split('|')[5])&&links(l.fields.class).includes(liveClass?.id));
   const l=peers.length===1&&instant(peers[0].fields['last-seen'])!==null&&instant(peers[0].fields['last-seen'])>=instant(r.fields['last-success'])?peers[0]:null;
   return {key:r.fields['trip-key'],trip:String(r.fields['trip-id']),uuid:r.fields['source-uuid'],entry:Number(r.fields['trip-key'].split('|')[5]),at:l?l.fields['last-seen']:r.fields['last-success'],order:orderValue(l?l.fields['order-of-go']:r.fields['order-of-go']),scratch:l?l.fields.scratch===true:flag(r.fields.scratch),gone:flag(l?l.fields.gone:r.fields['gone-in'])};
  });
  let complete=false;
  try{const members=JSON.parse(c.fields.trips_json);complete=Array.isArray(members)&&members.length===roster.length&&new Set(members.map(x=>x.entryxclasses_uuid)).size===members.length&&members.every(m=>roster.some(r=>r.trip===String(m.trip_id)&&r.entry===Number(m.entry_id)&&r.uuid===m.entryxclasses_uuid));}catch{}
  if(!complete)reasons.push('class_trip_roster_incomplete');
  const pending=roster.filter(r=>r.scratch===false&&r.gone===false);
  const ordered=pending.every(r=>r.order!==null)&&new Set(pending.map(r=>r.order)).size===pending.length;
  const flagsKnown=roster.every(r=>r.scratch!==null&&(r.scratch||r.gone!==null)&&instant(r.at)!==null&&instant(r.at)<=instant(at));
  const status=scratch===true?'Scratched':gone===true?'Gone':scratch===false&&gone===false?'Pending':'Unknown';
  if(!flagsKnown)reasons.push('roster_eligibility_unknown');
  if(order===null)reasons.push('trip_order_unknown');
  if(!ordered)reasons.push('pending_order_unknown_or_duplicate');
  const ahead=status==='Pending'&&complete&&flagsKnown&&ordered?pending.filter(r=>r.order<order).length:null;
  const compatible=input('class-status').map(r=>classAnswer(r,identity,at)).filter(Boolean).sort((a,b)=>instant(b.row.fields['evaluated-at'])-instant(a.row.fields['evaluated-at']));
  const cls=compatible[0]||null;
  if(!cls)reasons.push('class_status_missing_or_incompatible');
  const ringRows=input('ring-status').filter(r=>{try{const f=r.fields,p=JSON.parse(f['payload-json']),i=p.result.identity;return p.policy==='ring-state-v1'&&hash(p)===f['content-hash']&&i.customer===customer&&i.show===show&&i.date===day&&i.ring===ring&&f['show-id']===show&&f.date===day&&f['ring-number']===ring&&instant(f['observed-at'])!==null&&instant(f['observed-at'])<=instant(at);}catch{return false;}}).sort((a,b)=>instant(b.fields['observed-at'])-instant(a.fields['observed-at']));
  const ringStatus=ringRows[0]||null;
  const ringState=ringStatus?JSON.parse(ringStatus.fields['payload-json']).result:null;
  if(!ringStatus)reasons.push('ring_status_missing');
  const groups=input('wef-live.groups').filter(r=>r.fields['group-key']===`${show}|${day}|${group}`);
  let phaseUnknown=true;
  if(groups.length===1){try{const raw=JSON.parse(groups[0].fields['source-json']);phaseUnknown=raw.JOUnderway===true||raw.R2Underway===true;}catch{}}
  if(phaseUnknown)reasons.push('base_phase_not_confirmed');
  const shared=schedules.some(r=>r.fields.group===c?.fields.group&&r.fields.class&&r.fields.class!==f.class);
  if(shared)reasons.push('shared_group_trip_sequence_unavailable');
  const preceding=schedule?schedules.filter(r=>r.fields.ring===schedule.fields.ring&&r.fields.kind==='class'&&r.fields.group!==schedule.fields.group&&Number.isFinite(r.fields['source-position'])&&r.fields['source-position']<schedule.fields['source-position']).sort((a,b)=>b.fields['source-position']-a.fields['source-position'])[0]:null;
  let previousEnd=null;
  if(preceding){
   const pc=classes.find(r=>r.fields['class-key']===preceding.fields.class);
   if(pc){const pi={...identity,group:Number(pc.fields.group?.split('|')[4]),class:pc.fields['class-id']};
    const pp=input('class-status').map(r=>classAnswer(r,pi,at)).filter(Boolean).sort((a,b)=>instant(b.row.fields['evaluated-at'])-instant(a.row.fields['evaluated-at']));
    previousEnd=pp[0]?.end||null;
    if(!previousEnd&&pc.fields.status!=='Completed')reasons.push('preceding_class_end_missing');
   }else reasons.push('preceding_class_identity_missing');
  }
  let start=cls?.start||null,estimated=null,basis='unavailable';
  if(status==='Pending'&&c?.fields.status==='Completed')reasons.push('class_completed_with_pending_trip');
  if(status==='Pending'&&ahead!==null&&cls&&schedule&&ringState&&!phaseUnknown&&!shared&&!reasons.some(r=>['class_completed_with_pending_trip','observation_missing_or_future','class_identity_missing_or_ambiguous','preceding_class_end_missing','preceding_class_identity_missing','live_trip_ambiguous'].includes(r))){
   // Same arithmetic as legacy start + (pending rank - 1) * pace.
   // Underway uses the saved observation as the remaining-work estimate anchor.
   if(c.fields.status==='Underway'&&source==='live'&&ringState.current_class?.group_id===group&&ringState.current_class.classes?.some(x=>x.id===classId)){start=observed;basis='remaining_from_observation';}
   else if(c.fields.status==='Underway'){start=null;reasons.push('ring_current_class_not_confirmed');}
   else {start=max(start,previousEnd);basis='class_start_and_preceding_end';}
   if(instant(start)!==null)estimated=add(start,ahead*PACE);
  }
  const provider=providerTime(f['estimated-go-time'],day);
  const prior=previous.filter(r=>r['trip-key']===key&&instant(r['snapshot-at'])<instant(at)).sort((a,b)=>instant(b['snapshot-at'])-instant(a['snapshot-at']))[0]||null;
  const result={'snapshot-key':`${key}|${at}`,'trip-key':key,'show-id':show,date:day,'ring-number':ring,'class-id':classId,'group-id':Number.isSafeInteger(group)?group:null,'trip-id':String(f['trip-id']),
   'snapshot-at':at,'observed-at':observed||null,trip:link('trips',t),class:link('classes',c),schedule:link('schedules',schedule),'ring-status':link('ring',ringStatus),'class-status':link('class',cls?.row),
   source,status,'order-of-go':order,'actual-order':actual,'trips-ahead':ahead,'pace-used':PACE,'pace-method':'fixed-default','class-start':cls?.start||null,'preceding-class-end':previousEnd,
   'estimated-go-time':estimated,'estimate-basis':estimated?basis:'unavailable','provider-go-time':provider,'provider-observed-at':f['last-success']||null,
   'unavailable-reasons':[...new Set(reasons)].join('\n'),'previous-snapshot':prior?.['snapshot-key']||null};
  const before=prior?material(prior):{};result.changes=JSON.stringify(Object.entries(material(result)).filter(([k,v])=>JSON.stringify(before[k]??null)!==JSON.stringify(v??null)).map(([field,current])=>({field,previous:before[field]??null,current})));
  out.push(result);
 }
 return out;
}
module.exports={BASE,PACE,snapshots,classAnswer};
