'use strict';
// Independent lane with injected transport. No timer, production API or Records alerts.
// Selection follows existing ring-cleanup-plan eligibility/oldest-first patterns.
const {isDeepStrictEqual}=require('node:util');
const {BASE}=require('./trip-status');
const LOGS=['run-logs','heartbeat','errs-log'];
const OWNED=['trip-status','timing-answers','engine-checkpoints',...LOGS];
const terminal=new Set(['Success','Skipped','Partial','Failed','Error','complete','deferred','failed','partial']);
const valid=t=>Number.isFinite(Date.parse(t));
function due(state,show,now){
 if(!Number.isSafeInteger(show)||show<=0||!valid(now))throw Error('Verified default show and clock required');
 const p=Object.fromEntries(new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',hourCycle:'h23'}).formatToParts(new Date(now)).map(x=>[x.type,x.value]));
 const day=`${p.year}-${p.month}-${p.day}`;
 if(Number.isSafeInteger(state.show)&&state.show!==show)return {trigger:'new-show',oldShow:state.show,show,day};
 if(Number(p.hour)>=19&&state.dailyDay!==day)return {trigger:'daily-1900',show,day};
 if(!valid(state.quickAt)||Date.parse(now)-Date.parse(state.quickAt)>=10800000)return {trigger:'three-hour',show,day};
 return null;
}
function plan({table,rows,event,now,checked=false,protection}){
 if(!OWNED.includes(table)||!checked||!event)return [];
 if(!['daily-1900','three-hour','new-show'].includes(event.trigger)||!valid(now)||!Number.isSafeInteger(event.show)||event.show<=0)throw Error('Invalid cleanup event');
 // Explicit complete protection inventory is required; absent state is not empty state.
 if(!protection||protection.complete!==true||protection.pending===true)return [];
 for(const key of ['recordIds','runIds','resolvedErrorIds','retiredCheckpointIds','unreferencedAnswerIds'])if(!Array.isArray(protection[key]))throw Error('Incomplete cleanup protections');
 if(event.trigger==='three-hour'&&(!LOGS.includes(table)||rows.length<1000))return [];
 if(event.trigger==='daily-1900'&&!LOGS.includes(table))return [];
 if(event.trigger==='new-show'&&(!Number.isSafeInteger(event.oldShow)||event.oldShow<=0||event.oldShow===event.show))throw Error('Show ID must change');
 const candidates=rows.filter(row=>{
  const f=row.fields||{};
  if(!/^rec[A-Za-z0-9]{14}$/.test(row.id)||!valid(row.createdTime)||Date.parse(row.createdTime)>Date.parse(now)||protection.recordIds.includes(row.id)||protection.runIds.includes(f['run-id']))return false;
  // Protect current/future dates for any show, not only the default show.
  if(!LOGS.includes(table)&&typeof f.date==='string'&&f.date>=event.day)return false;
  if(event.trigger==='new-show'&&f['show-id']!==event.oldShow)return false;
  if(table==='engine-checkpoints')return event.trigger==='new-show'&&protection.retiredCheckpointIds.includes(row.id);
  if(['timing-answers','trip-status'].includes(table))return event.trigger==='new-show'&&protection.unreferencedAnswerIds.includes(row.id);
  if(table==='errs-log')return protection.resolvedErrorIds.includes(row.id)&&valid(f['occurred-at'])&&Date.parse(f['occurred-at'])<=Date.parse(now);
  return terminal.has(f.result)&&valid(f.completed)&&Date.parse(f.completed)<=Date.parse(now);
 }).sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));
 return event.trigger==='three-hour'?candidates.slice(0,500):candidates;
}
async function cleanup({api,table,rows,event,now,checked,protection}){
 if(api?.base!==BASE)throw Error('Trip base isolation required');
 const selected=plan({table,rows,event,now,checked,protection});
 // Transport must persist/read back intent and verify absence; errors stop immediately.
 if(!selected.length)return {selected:0,deleted:0};
 if(!api.beginIntent||!api.finishIntent)throw Error('Durable intent transport required');
 const fresh=await api.read(table,selected.map(r=>r.id));
 if(!isDeepStrictEqual(fresh,selected))throw Error('Cleanup records changed; no deletion');
 const intent=await api.beginIntent({base:BASE,table,records:selected,event});
 await api.remove(table,selected.map(r=>r.id));
 const remaining=await api.read(table,selected.map(r=>r.id));
 if(remaining.length)throw Error('Cleanup absence unverified; preserve pending intent');
 await api.finishIntent(intent);
 return {selected:selected.length,deleted:selected.length};
}
module.exports={LOGS,OWNED,due,plan,cleanup};
