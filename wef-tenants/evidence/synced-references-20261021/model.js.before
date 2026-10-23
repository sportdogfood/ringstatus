"use strict";
// Pure source normalization. Legacy references: trips_dailyv2.js buildAuxiliaryRowsForTenant
// and lib/airtable/trainer_info.js; stable source identities are preserved.
const positive=(v,label)=>{const n=Number(v);if(v==null||v===''||!Number.isSafeInteger(n)||n<=0)throw Error('Invalid '+label);return n;};
function day(v){const s=String(v??'');const m=s.match(/^(\d{4}-\d{2}-\d{2})(?:$|T)/);if(!m||new Date(m[1]+'T00:00:00Z').toISOString().slice(0,10)!==m[1])throw Error('Invalid source date');return m[1];}
function validateTrainers(records){
 const active=records.filter(r=>r.fields.active===true);if(!active.length||active.length>6)throw Error('One to six active trainers required');
 const ids=new Set(),slots=new Set();let mains=0;
 for(const r of active){const f=r.fields,id=positive(f['trainer-id'],'trainer ID'),slot=positive(f.slot,'trainer slot');if(ids.has(id)||slots.has(slot)||slot>6)throw Error('Duplicate trainer ID or invalid slot');ids.add(id);slots.add(slot);if(f.role==='main'){mains++;if(slot!==1)throw Error('Main trainer must use slot 1');}else if(f.role!=='additional'||slot===1)throw Error('Invalid trainer role');}
 if(mains!==1)throw Error('Exactly one active main trainer required');return active.sort((a,b)=>a.fields.slot-b.fields.slot);
}
function validatePeople(p,trainerId,showId){
 if(!p||typeof p!=='object'||Array.isArray(p)||Number(p.people?.people_id)!==Number(trainerId)||!Array.isArray(p.trips))throw Error('Invalid people identity or missing explicit trips array');
 for(const sid of [p.show_id,p.people?.show_id,p.show?.show_id].filter(v=>v!=null))if(Number(sid)!==Number(showId))throw Error('People show mismatch');
 if(p.total_trips!=null&&(!Number.isFinite(Number(p.total_trips))||Number(p.total_trips)<0||(!p.trips.length&&Number(p.total_trips)!==0)))throw Error('Inconsistent people trip count');
 return p.trips;
}
function validateShow(rows,now){
 const current=rows.filter(f=>f.is_default===true);if(current.length!==1)throw Error('Exactly one default shared show required');const f=current[0];
 positive(f.show_id,'show ID');positive(f.customer_id,'customer ID');positive(f.facility_id,'facility ID');
 const start=day(f.start_date),end=day(f.end_date),selected=day(f.selected_date),age=Date.parse(now)-Date.parse(f.last_checked);
 if(start>end||selected<start||selected>end||!Number.isFinite(age)||age< -60000||age>30*60000)throw Error('Invalid or stale source-selected show');return f;
}
function showDays(show){const result=[];for(let d=day(show.start_date);d<=day(show.end_date);d=new Date(Date.parse(d+'T00:00:00Z')+86400000).toISOString().slice(0,10)){result.push(d);if(result.length>31)throw Error('Show exceeds supported 31 days');}return result;}
const value=(o,...keys)=>keys.map(k=>o[k]).find(v=>v!=null&&v!=='');
function normalizeEntries(trips,trainer,show,schedules){
 const out=[],pid=positive(trainer.fields['trainer-id'],'trainer ID');
 for(const trip of trips){
  if(!trip||typeof trip!=='object'||Array.isArray(trip))throw Error('Invalid trip row');
  const sid=value(trip,'show_id','showId');if(sid!=null&&Number(sid)!==Number(show.show_id))throw Error('Trip show mismatch');
  const entry=positive(value(trip,'entry_id','entryId'),'entry ID'),horse=positive(value(trip,'horse_id','horseId'),'horse ID'),rider=positive(value(trip,'rider_id','riderId'),'rider ID'),classId=positive(value(trip,'class_id','classId'),'class ID');
  const rawDate=value(trip,'show_date','schedule_show_date','date','class_date'),date=rawDate==null?null:day(rawDate);
  const matches=schedules.filter(r=>Number(r.fields['show-id'])===Number(show.show_id)&&Number(r.fields['class-id'])===classId&&r.fields.status!=='dropped'&&(!date||r.fields.date===date));
  if(!matches.length)throw Error('No shared schedule for entry '+entry+' class '+classId);
  if(!date&&new Set(matches.map(r=>r.fields.date)).size>1)throw Error('Ambiguous entry date for class '+classId);
  const dates=new Set();for(const schedule of matches){const f=schedule.fields,d=day(f.date);if(dates.has(d))throw Error('Ambiguous class schedule '+classId+' '+d);dates.add(d);if(d<show.start_date||d>show.end_date)throw Error('Entry date outside show');
   out.push({'entry-key':`${show.show_id}|${d}|${entry}|${classId}`,date:d,'show-id':Number(show.show_id),'entry-id':entry,'horse-id':horse,'rider-id':rider,'class-id':classId,'ring-number':positive(f.ring,'schedule ring'),
    'class-name':String(f['class-name']||value(trip,'class_name','className')||''),'horse-name':String(value(trip,'horse','Horse','horse_name','horseName')||''),'rider-name':String(value(trip,'rider_name','riderName')||''),'trainer-ids':String(pid),status:'active','source-json':JSON.stringify(trip),'schedule-key':f['stage-key'],'schedule-link':`https://airtable.com/appUq5Qz9kARqFija/tblqiTFqqO3zk2KJC/${schedule.id}`});
  }
 }
 return mergeEntries(out);
}
function mergeEntries(rows){
 const byKey=new Map();for(const row of rows){const old=byKey.get(row['entry-key']);if(!old){byKey.set(row['entry-key'],{...row});continue;}
  for(const k of ['show-id','date','entry-id','class-id','horse-id','rider-id','ring-number','schedule-key'])if(old[k]!==row[k])throw Error('Conflicting entry identity '+row['entry-key']);
  old['trainer-ids']=[...new Set((old['trainer-ids']+','+row['trainer-ids']).split(','))].sort((a,b)=>Number(a)-Number(b)).join(',');
 }return [...byKey.values()].sort((a,b)=>a['entry-key'].localeCompare(b['entry-key']));
}
function reconcileEntries(previous,incoming,successfulIds){
 const success=new Set(successfulIds.map(String)),current=mergeEntries(incoming),result=new Map(current.map(r=>[r['entry-key'],r]));
 for(const old of previous){const key=old['entry-key'],prior=(old['trainer-ids']||'').split(',').filter(Boolean);const retained=old.status==='dropped'?[]:prior.filter(id=>!success.has(id));
  if(retained.length){const keep={...old,'trainer-ids':retained.join(',')};const merged=mergeEntries([keep,...(result.has(key)?[result.get(key)]:[])]);result.set(key,merged[0]);}
  else if(!result.has(key))result.set(key,{...old,status:'dropped'});
 }return [...result.values()];
}
module.exports={positive,day,validateTrainers,validatePeople,validateShow,showDays,normalizeEntries,mergeEntries,reconcileEntries};
