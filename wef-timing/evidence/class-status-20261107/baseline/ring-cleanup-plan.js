'use strict';
// Delete-only selection. Actual Ring run records are run-logs, not a separate runs table.
const {localDay}=require('./purge');
const LOGS=['run-logs','heartbeat','errs-log'];
const OWNED=['timing-answers',...LOGS];
const idOK=x=>typeof x==='string'&&/^rec[A-Za-z0-9]{14}$/.test(x);
const terminal=new Set(['Success','Skipped','Partial','Failed','Error','complete','deferred','failed','partial']);
function due(state,now){
 const ms=Date.parse(now),day=localDay(now),hour=Number(new Intl.DateTimeFormat('en-GB',{timeZone:'America/New_York',hour:'2-digit',hourCycle:'h23'}).format(new Date(ms))),previous=Date.parse(state.last_quick_check_at);
 return{day,daily:hour>=19&&state.daily_cleared!==day,quick:!Number.isFinite(previous)||ms-previous>=10800000};
}
function dailyCutoff(now){
 const day=localDay(now),noon=Date.parse(day+'T12:00:00Z'),hour=Number(new Intl.DateTimeFormat('en-GB',{timeZone:'America/New_York',hour:'2-digit',hourCycle:'h23'}).format(new Date(noon)));
 return new Date(Date.parse(day+'T19:00:00Z')+(12-hour)*3600000).toISOString();
}
function eligible(table,row,{now,trigger='quick-cleanup',cutoff=now,protectedIds=[],protectedRuns=[],resolvedErrorIds=[],referencedAnswerKeys=[],referencesVerified=false,parentRunIds=[]}){
 const f=row?.fields||{},ms=Date.parse(now),created=Date.parse(row?.createdTime),end=Date.parse(cutoff);
 if(!OWNED.includes(table)||!idOK(row?.id)||protectedIds.includes(row.id)||!Number.isFinite(created)||created>ms||!Number.isFinite(end))return false;
 if(protectedRuns.includes(f['run-id'])||(['run-logs','heartbeat'].includes(table)&&parentRunIds.includes(f['run-id'])))return false;
 if(table==='errs-log')return resolvedErrorIds.includes(row.id)&&Number.isFinite(Date.parse(f['occurred-at']))&&Date.parse(f['occurred-at'])<=Math.min(ms,end);
 if(table==='timing-answers')return trigger==='coordinator-new-show'&&referencesVerified===true&&typeof f['answer-key']==='string'&&!!f['answer-key']&&!referencedAnswerKeys.includes(f['answer-key'])&&['ring-current','ring-observation','ring-pace'].includes(f['answer-kind'])&&Number.isFinite(Date.parse(f['evaluated-at']))&&Date.parse(f['evaluated-at'])<=ms;
 return terminal.has(f.result)&&Number.isFinite(Date.parse(f.completed))&&Date.parse(f.completed)<=Math.min(ms,end);
}
function plan({table,rows,checked=false,trigger,alert,context,now,protection={},pending=false,cutoff=now}){
 if(!checked||!OWNED.includes(table)||pending)return[];
 if(!context||!Number.isSafeInteger(context.customer)||context.customer<=0||!Number.isSafeInteger(context.show)||context.show<=0)throw Error('Cleanup canonical identity missing');
 let candidates=rows.filter(r=>eligible(table,r,{...protection,now,trigger,cutoff}));
 if(trigger==='daily-1900')return LOGS.includes(table)?candidates:[];
 if(trigger==='quick-cleanup'){
  if(!LOGS.includes(table)||rows.length<1000)return[];
  candidates.sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));
  return candidates.slice(0,500);
 }
 if(trigger==='coordinator-new-show'){
  // Transport is intentionally unbound pending the coordinator's actual handoff.
  if(!alert||alert.coordinator!=='wef-timing-coordinator'||typeof alert.event_id!=='string'||!alert.event_id||alert.customer!==context.customer||alert.new_show!==context.show||!Number.isSafeInteger(alert.old_show)||alert.old_show<=0||alert.old_show===alert.new_show||!Number.isFinite(Date.parse(alert.discovered_at))||Date.parse(alert.discovered_at)>Date.parse(now))throw Error('Verified coordinator new-show alert required');
  return candidates.filter(r=>Number.isSafeInteger(r.fields['show-id'])&&r.fields['show-id']>0&&r.fields['show-id']!==context.show);
 }
 throw Error('Unsupported Ring cleanup trigger');
}
module.exports={LOGS,OWNED,eligible,plan,due,dailyCutoff,terminal};
