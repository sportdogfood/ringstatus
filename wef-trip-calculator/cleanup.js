'use strict';
// Independent lane with injected transport. No timer, production API or Records alerts.
const {isDeepStrictEqual}=require('node:util');
const {base:BASE}=require('./schema.json');
const LOGS=Object.freeze(['run-logs','heartbeat','errs-log']);
const terminal=new Set(['Success','Skipped','Partial','Failed','Error','complete','deferred','failed','partial']);
const valid=t=>Number.isFinite(Date.parse(t));
function due(state,show,now){
 if(!Number.isSafeInteger(show)||show<=0||!valid(now))throw Error('Verified default show and clock required');
 const p=Object.fromEntries(new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',hourCycle:'h23'}).formatToParts(new Date(now)).map(x=>[x.type,x.value]));
 const day=`${p.year}-${p.month}-${p.day}`;
 const dailyDue=Number(p.hour)>=19&&state.dailyDay!==day;
 if(Number.isSafeInteger(state.show)&&state.show!==show)return {trigger:'new-show',oldShow:state.show,show,day,dailyDue};
 return {trigger:dailyDue?'daily-1900':'log-limit',show,day};
}
function plan({table,rows,event,now,checked=false,protection,pendingTrim=false}){
 if(!LOGS.includes(table)||checked!==true||!event)return [];
 if(!['daily-1900','log-limit','new-show'].includes(event.trigger)||!valid(now)||!Number.isSafeInteger(event.show)||event.show<=0)throw Error('Invalid cleanup event');
 // Explicit complete protection inventory is required; absent state is not empty state.
 if(!protection||protection.complete!==true||protection.pending===true)return [];
 for(const key of ['recordIds','runIds','resolvedErrorIds'])if(!Array.isArray(protection[key]))throw Error('Incomplete cleanup protections');
 if(event.trigger==='log-limit'&&(rows.length<=500||(rows.length<1000&&pendingTrim!==true)))return [];
 if(event.trigger==='new-show'&&(!Number.isSafeInteger(event.oldShow)||event.oldShow<=0||event.oldShow===event.show))throw Error('Show ID must change');
 const candidates=rows.filter(row=>{
  const f=row.fields||{};
  if(!/^rec[A-Za-z0-9]{14}$/.test(row.id)||!valid(row.createdTime)||Date.parse(row.createdTime)>Date.parse(now)||protection.recordIds.includes(row.id)||protection.runIds.includes(f['run-id']))return false;
  if(table==='errs-log')return protection.resolvedErrorIds.includes(row.id)&&valid(f['occurred-at'])&&Date.parse(f['occurred-at'])<=Date.parse(now);
  return terminal.has(f.result)&&valid(f.completed)&&Date.parse(f.completed)<=Date.parse(now);
 }).sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));
 return event.trigger==='log-limit'?candidates.slice(0,rows.length-500):candidates;
}
async function cleanup({api,table,rows,event,now,checked,protection,pendingTrim=false}){
 if(api?.base!==BASE)throw Error('Trip base isolation required');
 const selected=plan({table,rows,event,now,checked,protection,pendingTrim});
 const remainingCount=rows.length-selected.length;
 // Only a later complete table read can confirm the target after deleting rows.
 const trimPending=LOGS.includes(table)&&(remainingCount>500||selected.length>0)&&(pendingTrim===true||(checked===true&&rows.length>=1000));
 const result={selected:selected.length,deleted:selected.length,remaining:remainingCount,trimPending};
 // Transport must persist/read back intent and verify absence; errors stop immediately.
 if(!selected.length)return result;
 if(!api.beginIntent||!api.finishIntent)throw Error('Durable intent transport required');
 const fresh=await api.read(table,selected.map(r=>r.id));
 const byId=new Map(fresh.map(row=>[row.id,row]));
 if(fresh.length!==selected.length||!isDeepStrictEqual(selected.map(row=>byId.get(row.id)),selected))throw Error('Cleanup records changed; no deletion');
 const intent=await api.beginIntent({base:BASE,table,records:selected,event});
 await api.remove(table,selected.map(r=>r.id));
 const remaining=await api.read(table,selected.map(r=>r.id));
 if(remaining.length)throw Error('Cleanup absence unverified; preserve pending intent');
 await api.finishIntent(intent,result);
 return result;
}
module.exports={LOGS,OWNED:LOGS,due,plan,cleanup};
