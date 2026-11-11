'use strict';
const {localDay}=require('./purge');
const {hash}=require('./pace');
const {writeAnswers}=require('./data-source');
const BASE='appYOykKIy1kAquhP';
const TABLES={shows:'tblbIE6RygVMtiEmS',days:'tblgRdGfOmwLRUlrT',rings:'tbleertfMY7ISGZCh',groups:'tblSmVxVsHlpudxlQ',classes:'tblREU7auaqFoeKZ8',trips:'tblPUJ9teCL1LL0V6',schedules:'tbl02nCQk1caPoeO2'};
const LINK_FIELDS=['w-shows','w-days','wef-live.rings','wef-data.schedules','current-group','current-classes','current-trips','next-group','next-classes','next-trips'];
const ids=value=>(value||[]).map(v=>typeof v==='string'?v:v.id);
const count=value=>value!==null&&value!==undefined&&value!==''&&Number.isSafeInteger(Number(value))&&Number(value)>=0?Number(value):null;
const round=value=>value===null?null:Math.round(value*10000)/10000;
const clock=iso=>iso?new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23',timeZoneName:'short'}).format(new Date(iso)):null;
function easternTime(day,time){
 if(!/^\d{2}:\d{2}:\d{2}$/.test(time||'')||time==='00:00:00')return null;
 const target=Date.parse(day+'T'+time+'Z');if(!Number.isFinite(target))return null;
 let at=target;for(let i=0;i<3;i++){const f=new Intl.DateTimeFormat('sv-SE',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).format(new Date(at));at+=target-Date.parse(f.replace(' ','T')+'Z');}return at;
}
function selectGroup(ring,groups,role,status,issues){
 const keys=String(ring.fields[role+'-group']||'').split(',').map(s=>s.trim());
 const found=groups.filter(g=>keys.includes(g.fields['group-key'])||ids(g.fields[role+'-in-ring']).includes(ring.id));
 if(found.length!==1){if(found.length>1)issues.push(role+'_group_ambiguous');return null;}
 if(found[0].fields.status!==status){issues.push(role+'_group_status_mismatch');return null;}
 return found[0];
}
function groupClasses(group,classes,show,customer,day){
 if(!group)return[];
 const linked=new Set(ids(group.fields['live-classes']?.length?group.fields['live-classes']:group.fields.classes));
 return classes.filter(c=>linked.has(c.id)&&c.fields.date===day&&Number(c.fields['show-id'])===show&&Number(c.fields['customer-id'])===customer);
}
function groupTrips(group,classes,trips,ring,day){
 if(!group)return[];
 const classIds=new Set(classes.map(c=>c.id)),classKeys=new Set(classes.map(c=>c.fields['class-key']));
 return trips.filter(t=>{
  const f=t.fields,groupIds=ids(f.groups),classLinks=ids(f.class);
  const classMatches=classLinks.length?classLinks.some(id=>classIds.has(id)):classKeys.has(f['class-key']);
  return f.date===day&&(Number(f['ring-id'])===Number(ring.fields['ring-id'])||ids(f.ring).includes(ring.id))
   &&classMatches&&(!groupIds.length||groupIds.includes(group.id));
 });
}
function buildSnapshot({showRow,dayRow,ring,groups,classes,trips,schedules,day,now,runId,issues=[]}){
 const show=Number(showRow.fields.show_id),customer=Number(showRow.fields.customer_id),rf=ring.fields;
 const inRing=groups.filter(g=>g.fields.date===day&&g.fields['group-key']?.startsWith(show+'|'+day+'|')&&ids(g.fields.ring).includes(ring.id));
 const current=selectGroup(ring,inRing,'current','Underway',issues),next=selectGroup(ring,inRing,'next','Upcoming',issues);
 const currentClasses=groupClasses(current,classes,show,customer,day),nextClasses=groupClasses(next,classes,show,customer,day);
 if(current&&!currentClasses.length)issues.push('current_class_links_unavailable');
 if(next&&!nextClasses.length)issues.push('next_class_links_unavailable');
 const currentTrips=groupTrips(current,currentClasses,trips,ring,day),nextTrips=groupTrips(next,nextClasses,trips,ring,day);
 const c=current?.fields,n=next?.fields;
 const asOf=(current?c['last-seen']:rf['last-seen'])||null,at=Date.parse(asOf);
 const validTime=Number.isFinite(at)&&at<=Date.parse(now)&&localDay(asOf)===day;
 const total=c?count(c.total):null,gone=c?count(c.gone):null;
 const validCounts=total!==null&&gone!==null&&gone<=total;
 const start=c?easternTime(day,c['estimated-start-time']):null;
 const elapsed=validTime&&start!==null&&at>start?(at-start)/60000:null;
 const remaining=validCounts?total-gone:null,pace=validCounts&&gone>0&&elapsed!==null?elapsed/gone:null;
 const endIn=pace!==null?remaining*pace:null,estimatedEnd=endIn===null?null:new Date(at+endIn*60000).toISOString();
 if(current&&!validCounts)issues.push('invalid_current_counts');
 if(current&&!validTime)issues.push('invalid_current_as_of');
 if(current&&start===null)issues.push('current_start_unavailable');
 if(current&&start!==null&&validTime&&at<=start)issues.push('current_start_not_before_as_of');
 const dayKey=[customer,show,day].join('|'),ringNumber=Number(rf['ring-number']);
 const currentSchedules=current?schedules.filter(s=>s.fields['w-days']===dayKey&&s.fields['w-shows']===customer+'|'+show&&Number(s.fields.ring?.split('|').at(-1))===ringNumber&&Number(s.fields.group?.split('|').at(-1))===Number(c['class-group-id'])&&s.fields.kind==='class'&&!String(s.fields['record-status']||'').startsWith('Dropped')):[];
 const scheduleEnds=currentSchedules.map(s=>easternTime(day,s.fields['end-time'])).filter(Number.isFinite);
 const scheduledEnd=scheduleEnds.length?new Date(Math.max(...scheduleEnds)).toISOString():null;
 const offset=estimatedEnd&&scheduledEnd?(Date.parse(estimatedEnd)-Date.parse(scheduledEnd))/60000:null;
 const names=rows=>rows.length?rows.map(r=>[r.fields['class-number'],r.fields['class-name']].filter(v=>v!==null&&v!==undefined).join(' ')).join('\n'):null;
 const detail=(group,cs,ts)=>group?{record:group.id,status:group.fields.status,total:count(group.fields.total),gone:count(group.fields.gone),start:group.fields['estimated-start-time']||null,as_of:group.fields['last-seen']||null,classes:cs.map(r=>r.id),trips:ts.map(r=>r.id)}:null;
 const payload={schema_version:1,policy:'current-group-snapshot-v1',run_id:runId,date:day,ring_id:Number(rf['ring-id']),as_of:validTime?asOf:null,current:detail(current,currentClasses,currentTrips),next:detail(next,nextClasses,nextTrips),elapsed_minutes:elapsed,pace_minutes_per_trip:pace,end_in_minutes:endIn,estimated_end_at:estimatedEnd,scheduled_end_at:scheduledEnd,offset_minutes:offset};
 return{
  'answer-key':'ring-snapshot|'+[customer,show,day,rf['ring-id'],runId].join('|'),'scope-key':'ring-current-group|'+[customer,show,day,rf['ring-id']].join('|'),'answer-kind':'ring-observation','source-lane':'data_live',
  'show-id':show,date:day,'ring-number':ringNumber,'group-id':c?Number(c['class-group-id']):null,'evaluated-at':now,'observed-at':validTime?asOf:null,
  'w-shows':[showRow.id],'w-days':dayRow?[dayRow.id]:[],'wef-live.rings':[ring.id],'wef-data.schedules':currentSchedules.map(s=>s.id),
  'current-group':current?[current.id]:[],'current-classes':currentClasses.map(r=>r.id),'current-trips':currentTrips.map(r=>r.id),
  'next-group':next?[next.id]:[],'next-classes':nextClasses.map(r=>r.id),'next-trips':nextTrips.map(r=>r.id),
  status:c?.status||'no_current_group',total,gone,'remaining-trips':remaining,'source-updated-at':c?.['source-updated-at']==null?null:String(c['source-updated-at']),
  'estimated-start-time':c?.['estimated-start-time']||null,'pace-elapsed-minutes':round(elapsed),'pace-completed-trips':gone,
  'pace-method':pace===null?'unavailable':'elapsed_from_estimated_start','pace-minutes-per-trip':round(pace),'estimated-end-time':clock(estimatedEnd),'end-in':round(endIn),
  'scheduled-end-time':clock(scheduledEnd),'minutes-early-late':round(offset),'ring-state':offset===null?null:offset>0?'late':offset<0?'early':'on_time',
  'current-class':names(currentClasses),'next-class':names(nextClasses),'next-status':n?.status||null,'next-total':n?count(n.total):null,'next-gone':n?count(n.gone):null,'next-estimated-start-time':n?.['estimated-start-time']||null,'next-as-of':n?.['last-seen']||null,
  'pace-evidence':pace===null?null:round(elapsed)+' minutes / '+gone+' gone = '+round(pace)+' minutes per trip; '+remaining+' remaining = '+round(endIn)+' minutes. Start basis: estimated-start-time.',
  'reason-codes-json':JSON.stringify(issues),'payload-json':JSON.stringify(payload),'content-hash':hash(payload)
 };
}
async function run({api,now=new Date().toISOString(),runId=require('node:crypto').randomUUID()}){
 const day=localDay(now),report={day,run_id:runId,started:now,ok:false,rings:[],issues:[],written:0};
 try{
  // 1. Read today's current/next source records.
  const shows=await api.list(BASE,TABLES.shows),defaults=shows.filter(s=>s.fields.is_default===true);
  if(defaults.length!==1){report.issues.push({message:'current_show_ambiguous_or_missing'});return report;}
  const showRow=defaults[0],show=Number(showRow.fields.show_id),customer=Number(showRow.fields.customer_id);
  if(!Number.isSafeInteger(show)||show<=0||!Number.isSafeInteger(customer)||customer<=0||showRow.fields['show-key']!==customer+'|'+show){report.issues.push({message:'invalid_show_identity'});return report;}
  const days=await api.list(BASE,TABLES.days,"{show-day-key}="+JSON.stringify([customer,show,day].join('|')));
  const rings=await api.list(BASE,TABLES.rings,"AND({date}="+JSON.stringify(day)+",{show-id}="+show+",{customer-id}="+customer+")");
  const groups=await api.list(BASE,TABLES.groups,"{date}="+JSON.stringify(day)),classes=await api.list(BASE,TABLES.classes,"{date}="+JSON.stringify(day));
  const schedules=await api.list(BASE,TABLES.schedules,"{w-days}="+JSON.stringify([customer,show,day].join('|')));
  for(const ring of rings.filter(r=>r.fields.date===day&&Number(r.fields['show-id'])===show&&Number(r.fields['customer-id'])===customer)){
   const issues=[];
   try{
    const scoped=groups.filter(g=>g.fields.date===day&&g.fields['group-key']?.startsWith(show+'|'+day+'|')&&ids(g.fields.ring).includes(ring.id));
    const current=selectGroup(ring,scoped,'current','Underway',[]),next=selectGroup(ring,scoped,'next','Upcoming',[]);
    const selectedClasses=[...groupClasses(current,classes,show,customer,day),...groupClasses(next,classes,show,customer,day)];
    const keys=[...new Set(selectedClasses.map(c=>c.fields['class-key']))];let trips=[];
    if(keys.length)try{trips=await api.list(BASE,TABLES.trips,"AND({date}="+JSON.stringify(day)+",{ring-id}="+Number(ring.fields['ring-id'])+",OR("+keys.map(k=>"{class-key}="+JSON.stringify(k)).join(',')+"))");}catch(e){issues.push('trip_read_failed:'+e.message);}
    // 2. Calculate only this current group's elapsed-time pace and finish.
    const fields=buildSnapshot({showRow,dayRow:days.find(d=>d.fields['show-day-key']===[customer,show,day].join('|')),ring,groups,classes,trips,schedules,day,now,runId,issues});
    const normalize=row=>({...row,fields:{...Object.fromEntries(LINK_FIELDS.map(k=>[k,[]])),...row.fields}});
    const checkedApi={list:async(...args)=>(await api.list(...args)).map(normalize),upsert:async(...args)=>{const response=await api.upsert(...args);return{...response,records:response.records?.map(normalize)};}};
    // 3. Save one immutable ring snapshot; the next group stays on this row.
    await writeAnswers(checkedApi,[fields]);report.written++;report.rings.push({ring:fields['ring-number'],as_of:fields['observed-at'],pace:fields['pace-minutes-per-trip'],end_in:fields['end-in'],estimated_end:fields['estimated-end-time']});
   }catch(e){issues.push(e.message);}
   for(const message of issues){report.issues.push({ring_id:Number(ring.fields['ring-id']),message});try{await api.upsert('tbliHowblBdSL51ov','error-key',[{'error-key':'ring-current-group|'+runId+'|'+ring.id+'|'+report.issues.length,'run-id':runId,'occurred-at':now,date:day,message}]);}catch(e){report.issues.push({message:'issue_log_failed:'+e.message});}}
  }
  report.ok=rings.length>0&&report.rings.length===rings.length;
 }catch(e){report.issues.push({message:e.message});}
 report.completed=new Date().toISOString();return report;
}
module.exports={BASE,TABLES,LINK_FIELDS,easternTime,selectGroup,groupClasses,groupTrips,buildSnapshot,run};
