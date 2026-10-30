'use strict';
const {hash}=require('../wef-timing/pace');
const {instant}=require('../wef-timing/class-timing');
const {localDay}=require('../wef-timing/purge');
const POLICY='entry-timing-v1';
const positive=n=>Number.isSafeInteger(n)&&n>0;
const keys=['customer','show','date','ring','group','class','occurrence','phase','entry','trip'];
function fresh(at,now,limit){const a=instant(at),n=instant(now);return a!==null&&n!==null&&a<=n&&n-a<limit;}
function evaluate({identity,now,rows,targetKey,observedAt,refs,classTiming,issues=[],phaseProof=null,source='data'}){
 if(!identity||!['customer','show','ring','group','class','entry'].every(k=>positive(identity[k]))
  ||!identity.occurrence||!identity.trip||identity.phase!=='base'||identity.date!==localDay(now)||instant(now)===null)throw Error('Entry identity/date invalid');
 if(!Array.isArray(rows)||!Array.isArray(refs)||!refs.length)throw Error('Entry evidence missing');
 const reasons=[...issues],target=rows.find(r=>r.key===targetKey);
 if(!target||rows.filter(r=>r.key===targetKey).length!==1)throw Error('Entry target ambiguous');
 const unique=new Set(rows.map(r=>r.key)).size===rows.length;
 const current=fresh(observedAt,now,source==='live'?600000:1800000);
 if(!current)reasons.push('entry_source_stale');
 if(!unique)reasons.push('trip_identity_ambiguous');
 const phaseProven=Boolean(phaseProof&&phaseProof.phase===identity.phase&&phaseProof.occurrence===identity.occurrence
  &&phaseProof.sequence_hash===hash(rows)&&phaseProof.ref?.source_key&&phaseProof.ref?.revision
  &&fresh(phaseProof.observed_at,now,source==='live'?600000:1800000));
 if(!phaseProven)reasons.push('phase_unproven');
 const flags=rows.every(r=>typeof r.scratch==='boolean'&&typeof r.gone==='boolean');
 if(!flags)reasons.push('eligibility_unknown');
 const active=rows.filter(r=>r.scratch===false);
 const anyActual=active.some(r=>positive(r.actual));
 const method=anyActual?'actual-order':'order-of-go';
 const value=r=>anyActual?r.actual:r.order;
 const ordered=active.every(r=>positive(value(r)))&&new Set(active.map(value)).size===active.length;
 if(!ordered)reasons.push('order_missing_or_ambiguous');
 const sorted=[...active].sort((a,b)=>value(a)-value(b));
 // A gone trip after an un-gone trip means this is not a proved execution frontier.
 let pending=false,frontier=true;
 for(const r of sorted){if(r.gone===false)pending=true;else if(r.gone===true&&pending)frontier=false;}
 if(!frontier)reasons.push('progress_order_conflict');
 const eligible=current&&unique&&flags&&target.scratch===false&&target.gone===false;
 const usable=eligible&&ordered&&frontier&&phaseProven;
 const before=usable?sorted.slice(0,sorted.findIndex(r=>r.key===targetKey)).filter(r=>!r.gone):null;
 const tripsAhead=before?.length??null,entriesAhead=before?new Set(before.map(r=>r.entry)).size:null;
 const pace=classTiming?.pace_minutes_per_trip;
 const workload=tripsAhead!==null&&Number.isFinite(pace)&&pace>0?tripsAhead*pace:null;
 let predicted=null;
 const anchorProof=phaseProven?phaseProof.anchor:null;
 // A pre-progress phase estimate can use a validated compatible phase start.
 // Observed-at is never a progress anchor. No in-progress model is established.
 if(workload!==null&&anchorProof?.kind==='phase_start'&&anchorProof.at===classTiming?.start_at
  &&['estimated','actual'].includes(classTiming?.start_kind)&&active.every(r=>r.gone===false)){
  const anchor=instant(anchorProof.at);
  if(anchor!==null){const ms=anchor+workload*60000;
   if(Number.isFinite(ms)&&Math.abs(ms)<=8640000000000000&&ms>=instant(now)&&localDay(new Date(ms).toISOString())===identity.date)predicted=new Date(ms).toISOString();
   else reasons.push('prediction_elapsed_or_outside_day');}
 }else reasons.push('phase_start_or_progress_anchor_unproven');
 const payload={schema_version:1,policy:POLICY,identity,source_lane:'data_live',timezone:'America/New_York',
  evaluated_at:now,source,observed_at:observedAt,eligibility:!current?'stale':target.scratch===true?'scratched':target.gone===true?'gone':eligible?'eligible':'unknown',
  order_method:ordered?method:null,order:ordered?value(target)??null:null,entries_ahead:entriesAhead,trips_ahead:tripsAhead,
  workload_minutes:workload,predicted_go_at:predicted,prediction_kind:predicted?'estimated':'unknown',
  minutes_until_go:predicted?(instant(predicted)-instant(now))/60000:null,pace_minutes_per_trip:pace??null,
  reasons:[...new Set(reasons)].sort(),phase_proof:phaseProven?phaseProof:null,input_refs:refs};
 const {evaluated_at,minutes_until_go,...material}=payload;material.input_refs=refs.map(({producer_proof,...r})=>r);
 const digest=hash(material),scope=JSON.stringify(['data_live','entry-timing',...keys.map(k=>identity[k])]);
 const fields={'answer-key':scope+'|'+digest,'scope-key':scope,'answer-kind':'entry-timing','source-lane':'data_live',
  'show-id':identity.show,date:identity.date,'ring-number':identity.ring,'group-id':identity.group,'class-id':identity.class,
  'entry-id':identity.entry,'trip-key':identity.trip,phase:'base',availability:predicted?'available':tripsAhead!==null?'partial':'unavailable',
  'pace-method':classTiming?.pace_method??'unknown','evaluated-at':now,'observed-at':observedAt,
  'reason-codes-json':JSON.stringify(payload.reasons),'input-refs-json':JSON.stringify(refs),'payload-json':JSON.stringify(payload),'content-hash':digest};
 if(Number.isFinite(pace))fields['pace-minutes-per-trip']=Math.round(pace*10000)/10000;
 return{payload,fields};
}
module.exports={POLICY,evaluate,fresh};
