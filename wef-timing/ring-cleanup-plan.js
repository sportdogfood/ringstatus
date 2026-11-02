'use strict';
// Delete-only selection. Execution requires durable intent and exact deletion readback.
const {localDay}=require('./purge');
const LOGS=['run-logs','heartbeat','errs-log'];
const OWNED=['timing-answers',...LOGS];
const idOK=x=>typeof x==='string'&&/^rec[A-Za-z0-9]{14}$/.test(x);
function oldDay(value,today){return typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)&&Number.isFinite(Date.parse(value))&&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value&&value<today;}
function eligible(table,row,{now,protectedIds=[],protectedRuns=[],resolvedErrorIds=[],referencedAnswerKeys=[],referencesVerified=false,parentRunIds=[]}){
 const f=row?.fields||{},ms=Date.parse(now),created=Date.parse(row?.createdTime),today=localDay(now);
 if(!OWNED.includes(table)||!idOK(row?.id)||protectedIds.includes(row.id)||!Number.isFinite(created)||ms-created<86400000||!oldDay(f.date,today)||!Number.isSafeInteger(f['show-id'])||f['show-id']<=0)return false;
 if(protectedRuns.includes(f['run-id'])||(table==='run-logs'&&parentRunIds.includes(f['run-id'])))return false;
 if(table==='errs-log')return resolvedErrorIds.includes(row.id)&&Number.isFinite(Date.parse(f['occurred-at']))&&Date.parse(f['occurred-at'])<=ms;
 if(table==='timing-answers')return referencesVerified===true&&typeof f['answer-key']==='string'&&!!f['answer-key']&&!referencedAnswerKeys.includes(f['answer-key'])&&['ring-current','ring-observation','ring-pace'].includes(f['answer-kind']);
 return ['Success','Skipped','complete','deferred'].includes(f.result)&&!f.errors&&Number.isFinite(Date.parse(f.completed))&&Date.parse(f.completed)<=ms;
}
function plan({table,rows,checked=false,trigger,alert,context,now,protection={},pending=false}){
 if(!checked||!OWNED.includes(table)||pending)return[];
 if(!context||!Number.isSafeInteger(context.customer)||context.customer<=0||!Number.isSafeInteger(context.show)||context.show<=0)throw Error('Cleanup canonical identity missing');
 let candidates=rows.filter(r=>eligible(table,r,{...protection,now}));
 if(trigger==='quick-cleanup'){
  if(!LOGS.includes(table)||rows.length<1000)return[];
  candidates.sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));
  return candidates.length>=500?candidates.slice(0,500):[];
 }
 if(trigger==='coordinator-new-show'){
  // Transport is intentionally unbound pending the coordinator's actual handoff.
  if(!alert||alert.coordinator!=='wef-timing-coordinator'||typeof alert.event_id!=='string'||!alert.event_id||alert.customer!==context.customer||alert.new_show!==context.show||!Number.isSafeInteger(alert.old_show)||alert.old_show<=0||alert.old_show===alert.new_show||!Number.isFinite(Date.parse(alert.discovered_at))||Date.parse(alert.discovered_at)>Date.parse(now))throw Error('Verified coordinator new-show alert required');
  return candidates.filter(r=>r.fields['show-id']!==context.show);
 }
 throw Error('Unsupported Ring cleanup trigger');
}
module.exports={LOGS,OWNED,eligible,plan};
