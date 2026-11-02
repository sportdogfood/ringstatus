"use strict";
const policy=require('./discovery-policy');
const valid=day=>typeof day==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(day)&&Number.isFinite(Date.parse(day))&&new Date(day+'T00:00:00Z').toISOString().slice(0,10)===day;
// Only skip work with a consistent completed local report. Unknown state follows
// the existing collector validation/logging path; this gate never fetches data.
function shouldRun(now,{focus,last,discovery,priority}){
 const instant=Date.parse(now),fields=focus?.fields;
 if(!Number.isFinite(instant))throw Error('Invalid cadence clock');
 if(!fields||!valid(fields.start_date)||!valid(fields.end_date)||fields.start_date>fields.end_date||!last?.ok||last.show_id!==focus.show_id||fields.show_id!==focus.show_id||!valid(last.selected_date)||last.selected_date<fields.start_date||last.selected_date>fields.end_date||!Number.isFinite(Date.parse(last.started))||!Number.isFinite(Date.parse(last.completed))||Date.parse(last.completed)<Date.parse(last.started)||Date.parse(last.completed)>instant||!Number.isFinite(Date.parse(focus.checked_at))||Date.parse(focus.checked_at)>Date.parse(last.started))return true;
 const today=policy.local(now);
 if(today.date>=fields.start_date&&today.date<=fields.end_date)return true;
 if(!discovery||!priority||priority.version!==2||priority.anchor!==last.selected_date)return true;
 try{if(policy.discoveryPlan(now,discovery,last.show_id).due)return true;}catch{return true;}
 // Keep the first normal evening decision even between hourly heartbeats.
 if(today.hour>=19&&(priority.date!==today.date||priority.evening!==true))return true;
 return instant-Date.parse(last.started)>=3600000;
}
module.exports={shouldRun};
