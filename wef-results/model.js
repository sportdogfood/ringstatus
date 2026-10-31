"use strict";
const crypto=require('node:crypto');
function canonical(value){if(Array.isArray(value))return value.map(canonical);if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonical(value[k])]));return value;}
const packed=value=>JSON.stringify(canonical(value));
const hash=value=>crypto.createHash('sha256').update(typeof value==='string'?value:packed(value)).digest('hex');
const fields=row=>row?.fields||{};
const flag=value=>value===true||value===1||value==='1';
const id=value=>{if(!/^[1-9]\d*$/.test(String(value))||!Number.isSafeInteger(Number(value)))throw Error('Invalid source identity');return String(value);};
function day(value){const s=String(value||'').slice(0,10);if(!/^\d{4}-\d{2}-\d{2}$/.test(s)||new Date(s+'T00:00:00Z').toISOString().slice(0,10)!==s)throw Error('Invalid date');return s;}
const localDay=now=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(now));
const sourceStamp=row=>hash(Object.fromEntries(['class-key','class-id','status','last-success','last-run','source-json','fingerprint'].map(k=>[k,fields(row)[k]??null])));
const RESULT_FIELDS=['place','placing','position','rank','rank_placing','score','score1','score2','score3','score4','score5','score6','time_one','time_two','time_three','faults_one','faults_two','faults_three','time_fault_one','time_fault_two','time_fault_three','total_prize_money','points_earned','gone_in','scratch_trip','disqualify_status_one','disqualify_status_two','disqualify_status_three','r3_trip','split_section'];
function evaluate(row,{now,maxSourceAgeMs,customer,show,live}={}){
  if(!Number.isFinite(maxSourceAgeMs)||maxSourceAgeMs<=0||!Number.isFinite(Date.parse(now)))throw Error('Explicit valid Results freshness policy/time required');
  const f=fields(row),out={key:f['class-key'],recordId:row.id,sourceAt:f['last-success']||'',sourceStamp:sourceStamp(row),status:'unavailable',reason:'Invalid source evidence',results:[]};
  try{
    const parts=String(out.key).split('|');if(parts.length!==5)throw Error('Invalid Data class key');
    const [c,s,d,ring,cls]=parts;[c,s,ring,cls].forEach(id);day(d);
    if(c!==id(customer)||s!==id(show)||cls!==id(f['class-id']))throw Error('Class outside canonical show/customer scope');
    Object.assign(out,{customer:Number(c),show:Number(s),date:d,ring:Number(ring),classId:Number(cls)});
    const j=JSON.parse(f['source-json']),detail=j.class_related_data,cl=j.class;
    if(!cl||!detail||id(cl.class_id)!==cls||id(cl.show_id)!==s||id(cl.customer_id)!==c||day(detail.date)!==d||id(detail.ring)!==ring)throw Error('Source class scope mismatch');
    if(f.status!==detail.status)throw Error('Data class status/source disagreement');
    const at=Date.parse(out.sourceAt),age=Date.parse(now)-at;
    if(!Number.isFinite(at)||age<0)throw Error('Invalid or future source observation');
    out.className=String(cl.name||f['class-name']||'');out.classNumber=String(cl.number??f['class-number']??'');out.sourceStatus=String(detail.status||'');out.verified=flag(cl.results_verified);out.visible=flag(detail.show_results);
    out.groupId=detail.class_group_id==null?null:Number(id(detail.class_group_id));
    out.source={base:'appdl16HJ5ETm8N69',table:'classes',record_id:row.id,class_key:out.key,source_at:out.sourceAt,source_hash:hash(f['source-json']),data_fingerprint:f.fingerprint??null,last_run:f['last-run']??[],verification_detail:cl.verification_detail??null,completed_trips:detail.completed_trips??null,total_trips:detail.total_trips??null,remaining_trips:detail.remaining_trips??null,grouped_class:detail.grouped_class??null};
    const l=fields(live);
    if(l['class-key']===`${s}|${d}|${cls}`&&l['scope-status']==='current'&&Number(l['customer-id']??c)===Number(c)&&Number(l['show-id']??s)===Number(s))out.live={record_id:live.id??null,observed_at:l['last-seen']??null,total:l.total??null,gone:l.gone??null,jo_gone:l['jo-gone']??null,r2_gone:l['r2-gone']??null,advisory_only:true};
    if(age>=maxSourceAgeMs)return {...out,status:'stale',reason:'Data class observation expired'};
    if(d>localDay(now))return {...out,status:'waiting',reason:'Future competition date'};
    if(detail.status!=='Completed')return {...out,status:'waiting',reason:'Class not source-reported Completed'};
    if(!out.verified||!out.visible)return {...out,status:'waiting',reason:'Results verification/publication not reported'};
    if(!Array.isArray(j.trips)||!Number.isSafeInteger(j.total_entry_trips)||j.total_entry_trips<0||j.total_entry_trips!==j.trips.length)throw Error('Incomplete class trips: total_entry_trips must match the class array');
    if(!j.trips.length)return {...out,status:'empty',reason:'Verified class contains no result trips; no placings inferred'};
    const seen=new Set(),participations=new Set();
    out.results=j.trips.map(t=>{
      const trip=id(t.trip_id),entry=id(t.entry_id);
      if(t.class_id!=null&&id(t.class_id)!==cls)throw Error('Trip class mismatch');
      if(seen.has(trip))throw Error('Duplicate source trip');seen.add(trip);
      const uuid=t.entryxclasses_uuid;
      if(typeof uuid!=='string'||!uuid.trim()||uuid!==uuid.trim()||participations.has(uuid))throw Error('Missing, malformed or duplicate source participation UUID');
      participations.add(uuid);
      const values=Object.fromEntries(RESULT_FIELDS.filter(k=>Object.hasOwn(t,k)).map(k=>[k,t[k]]));
      if(!Object.keys(values).some(k=>!['gone_in','scratch_trip','r3_trip','split_section'].includes(k)))throw Error('Trip result values missing');
      return {trip_id:trip,entry_id:entry,participation_uuid:t.entryxclasses_uuid,entry_key:`${s}|${d}|${entry}|${cls}`,live_entry_key:`${s}|${d}|${cls}|${trip}`,horse_id:t.horse_id??null,horse:t.horse??null,rider_id:t.rider_id??null,rider:t.rider_name??t.rider??null,values};
    }).sort((a,b)=>a.trip_id.localeCompare(b.trip_id));
    out.phases={jumper_table:cl.jumper_table??null,multiphase_rule:cl.multiphase_rule??null,jumper_table_info:j.jumper_table_info??null,phase_columns:j.phase_columns??null,class_phase_nav:j.class_phase_nav??null};
    // Source verification is not a promise of permanent official finality.
    out.finality='source-verified';out.contentHash=hash({results:out.results,phases:out.phases});
    return {...out,status:'ready',reason:'Source Completed, results verified and visible; exact class trips preserved'};
  }catch(e){return {...out,status:'unavailable',reason:e.message,results:[]};}
}
function project(e,previous,runId,checkedAt){
  const base={...(previous||{}),'snapshot-key':e.key,'check-status':e.status,'check-reason':e.reason,'checked-at':checkedAt,'checked-run-id':runId};
  for(const [name,value]of Object.entries({'customer-id':e.customer,'show-id':e.show,date:e.date,'ring-number':e.ring,'class-id':e.classId,'class-name':e.className,'class-number':e.classNumber}))if(value!==undefined)base[name]=value;
  if(e.source)base['check-evidence-json']=packed({data:e.source,live:e.live??null,source_stamp:e.sourceStamp});
  if(e.status!=='ready')return base;
  if(previous?.['source-observed-at']&&Date.parse(previous['source-observed-at'])>Date.parse(e.sourceAt))throw Error('Refusing older accepted source');
  const changed=previous?.['content-hash']!==e.contentHash;
  return {...base,'customer-id':e.customer,'show-id':e.show,date:e.date,'ring-number':e.ring,'class-id':e.classId,'class-number':e.classNumber,'class-name':e.className,'class-status':e.sourceStatus,'results-verified':e.verified,'show-results':e.visible,'finality':e.finality,'source-observed-at':e.sourceAt,'accepted-at':changed?checkedAt:previous['accepted-at'],'content-hash':e.contentHash,'previous-hash':changed?(previous?.['content-hash']||''):(previous?.['previous-hash']||''),'revision-number':changed?(previous?.['revision-number']||0)+1:previous['revision-number'],'result-count':e.results.length,'results-json':packed(e.results),'phase-json':packed(e.phases),'source-evidence-json':packed(e.source)};
}
module.exports={evaluate,project,hash,packed,sourceStamp,localDay};
