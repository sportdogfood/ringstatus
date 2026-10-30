"use strict";
const {hash,ageReason}=require('./pace');
function validDay(value){
  return typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)&&Number.isFinite(Date.parse(value))&&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value;
}
function localDay(now){return new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(now));}
function authority(show,scopeProblems,now){
  const f=show.fields,day=f.selected_date,sid=Number(f.show_id),customer=Number(f.customer_id);
  if(f.is_default!==true||!Number.isSafeInteger(sid)||sid<=0||!Number.isSafeInteger(customer)||customer<=0||f['show-key']!==`${customer}|${sid}`)return{reason:'purge_show_identity_unproven'};
  if(ageReason(f.last_checked,now,1800000))return{reason:'purge_show_not_fresh'};
  if(!validDay(day)||!validDay(f.start_date)||!validDay(f.end_date)||day<f.start_date||day>f.end_date)return{reason:'purge_working_day_unproven'};
  // Discovery/preparation of a future day must not retire today's observations.
  if(day!==localDay(now))return{reason:'purge_working_day_not_today'};
  if(!scopeProblems.has(day)||scopeProblems.get(day))return{reason:'purge_new_scope_not_verified'};
  const context={customer,show:sid,day,show_record:show.id,retired_shows:[]};return{context,token:hash(context),reason:null};
}
function eligible(row,context){
  const f=row.fields;
  const lane=context.source_lane||'schedules_live';
  if(f['answer-kind']!=='ring-pace'||f['source-lane']!==lane||!validDay(f.date)||!Number.isSafeInteger(f['show-id']))return false;
  let p;try{p=JSON.parse(f['payload-json']);}catch{return false;}
  const i=p.identity;
  if(!i||i.customer!==context.customer||i.show!==f['show-id']||i.date!==f.date||i.ring!==f['ring-number']||i.group!==f['group-id'])return false;
  const scope=[lane,i.show,i.date,i.ring,i.group,'base'].join('|');
  if(f['scope-key']!==scope||!String(f['answer-key']).startsWith(scope+'|'))return false;
  return i.show===context.show?i.date<context.day:(context.retired_shows||[]).includes(i.show);
}
function plan(rows,verified,state,runId){
  if(verified.reason)return{reason:verified.reason,rows:[],confirmed:false};
  const prior=state.purge_authority;
  const confirmed=prior?.token===verified.token&&prior.run_id!==runId;
  return{reason:confirmed?null:'purge_confirmation_pending',rows:confirmed?rows.filter(r=>eligible(r,verified.context)):[],confirmed,
    next:{token:verified.token,context:verified.context,run_id:runId}};
}
module.exports={validDay,localDay,authority,eligible,plan};
