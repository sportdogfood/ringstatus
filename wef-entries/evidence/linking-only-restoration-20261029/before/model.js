"use strict";
// Candidate replacement for wef-entries/model.js. Pure linking; no tenant inputs.
const positive=(value,label)=>{const n=Number(value);if(value==null||value===''||!Number.isSafeInteger(n)||n<=0)throw Error('Invalid '+label);return n;};
function day(value){const s=String(value??'').slice(0,10);if(!/^\d{4}-\d{2}-\d{2}$/.test(s)||!Number.isFinite(Date.parse(s+'T00:00:00Z'))||new Date(s+'T00:00:00Z').toISOString().slice(0,10)!==s)throw Error('Invalid source date');return s;}
function scope(shows,now){
 const selected=shows.filter(r=>r.fields.is_default===true);if(selected.length!==1)throw Error('Exactly one canonical default show required');
 const show=selected[0].fields;const sid=positive(show.show_id,'show ID'),customer=positive(show.customer_id,'customer ID');
 if(show['show-key']!==`${customer}|${sid}`)throw Error('Canonical show key mismatch');
 const start=day(show.start_date),end=day(show.end_date),selectedDate=day(show.selected_date),age=Date.parse(now)-Date.parse(show.last_checked);
 if(start>end||selectedDate<start||selectedDate>end||!Number.isFinite(age)||age< -60000||age>1800000)throw Error('Invalid or stale canonical show');
 const dates=[];for(let d=start;d<=end;d=new Date(Date.parse(d+'T00:00:00Z')+86400000).toISOString().slice(0,10)){dates.push(d);if(dates.length>31)throw Error('Show exceeds supported 31 days');}
 return{show,dates,showRecord:selected[0].id};
}
function schedulesFor(show,local){
 return local.filter(r=>Number(r.fields['show-id'])===Number(show.show_id)&&r.fields.status==='active');
}
function sharedClassKey(showId,date,classId,ring){return [Number(showId),String(date).slice(0,10),Number(classId),Number(ring)].join('|');}
// Shared tenant-independent matcher; no production collector invocation.
const {linkClass}=require('./participation-linker');
function dataAssociation(row,show,schedules=[]){
 const candidates=schedules.filter(s=>s.kind==='class'&&Number(s.customerId)===Number(show.customer_id)&&Number(s.showId)===Number(show.show_id)&&s.date===row.date&&Number(s.classId)===row['class-id']);
 if(candidates.length>1)return{status:'Conflict',reason:'Ambiguous Data class/date occurrence'};
 const match=candidates[0];
 if(!match||Number(match.ringNumber)!==row['ring-number'])return{status:'unresolved',reason:'No matching Data class/date/ring occurrence'};
 if(!match.key||!match.recordId)return{status:'unresolved',reason:'Missing retained Data identity'};
 return{status:'linked',basis:'class-date-ring',...match,groupStatus:'unresolved',groupReason:'Class occurrence does not prove operational group eligibility'};
}
function scheduleAssociation(row){
 return{status:row['schedule-link-status'],basis:row['schedule-link-basis']??null,key:row['schedule-key']??null,recordIds:row.schedules??[],reason:row['schedule-link-reason']??null,groupStatus:row['group-link-status'],groupEvidence:row['group-link-evidence']??null};
}
module.exports={scope,schedulesFor,sharedClassKey,linkClass,dataAssociation,scheduleAssociation};
