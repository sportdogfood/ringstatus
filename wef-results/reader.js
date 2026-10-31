"use strict";
const {packed,hash}=require('./model');
// Consumers must validate at read time; stored ready is not a forever-current flag.
function readSnapshot(row,{now,show,customer=15,maxSourceAgeMs,maxCheckAgeMs,heartbeat}={}){
 const f=row?.fields||row||{},h=heartbeat?.fields||heartbeat||{};
 const unavailable=reason=>({status:'unavailable',reason,results:[]});
 if(!Number.isFinite(Date.parse(now))||![maxSourceAgeMs,maxCheckAgeMs].every(n=>Number.isFinite(n)&&n>0))return unavailable('Explicit read freshness policy required');
 if(f['check-status']!=='ready'||f['show-id']!==show||f['customer-id']!==customer)return unavailable('Snapshot not ready in requested canonical scope');
 if(!['Success','Partial'].includes(h.result)||h['run-id']!==f['checked-run-id']||h.completed!==h['checked-at'])return unavailable('Results run not confirmed complete');
 for(const [at,limit]of [[f['source-observed-at'],maxSourceAgeMs],[f['checked-at'],maxCheckAgeMs],[h.completed,maxCheckAgeMs]]){const age=Date.parse(now)-Date.parse(at);if(!Number.isFinite(age)||age<0||age>=limit)return unavailable('Snapshot evidence stale or clock invalid');}
 try{
  const results=JSON.parse(f['results-json']),phases=JSON.parse(f['phase-json']);
  if(!Array.isArray(results)||!results.length||results.length!==f['result-count']||hash({results,phases})!==f['content-hash'])return unavailable('Snapshot content/hash mismatch');
  return {status:'available',snapshot_key:f['snapshot-key'],revision:f['revision-number'],content_hash:f['content-hash'],source_observed_at:f['source-observed-at'],finality:f.finality,results:JSON.parse(packed(results)),phases};
 }catch{return unavailable('Malformed accepted snapshot');}
}
module.exports={readSnapshot};
