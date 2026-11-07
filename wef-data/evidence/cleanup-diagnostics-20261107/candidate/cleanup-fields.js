'use strict';
// Data field bindings for the existing Live cleanup policy; original rows are never rewritten.
const PRIMARY={shows:'show-key',days:'day-key',rings:'ring-key',groups:'group-key',classes:'class-key',schedules:'schedule-key',entries:'entry-key',trips:'trip-key',results:'result-key'};
const LOGS=['heartbeat','run-logs','err-logs'];
const terminal=new Set(['Succeeded','Partial','Failed','Skipped','Interrupted','Deferred']);
function showOf(table,row,runShows={}){
 const f=row.fields||{};
 if(table==='shows')return Number.isSafeInteger(f['show-id'])?f['show-id']:null;
 const key=f[PRIMARY[table]];
 if(typeof key==='string'){
  const bits=key.split('|'),n=Number(bits[table==='entries'?0:1]);
  return Number.isSafeInteger(n)&&n>0?n:null;
 }
 if(table==='runs')return runShows[String(f['run-key']||'').split('|')[0]]??null;
 if(table==='err-logs')return runShows[row.id]??null;
 if(table==='run-logs'){const n=Number(String(f['show-key']||'').split('|')[1]);return Number.isSafeInteger(n)&&n>0?n:null;}
 return null;
}
function completed(table,row){
 const f=row.fields||{};
 // Data heartbeat is a mutable workflow singleton, not Live's per-invocation history.
 if(table==='heartbeat')return false;
 if(table==='err-logs')return Number.isFinite(Date.parse(f['occurred-at']));
 if(table==='run-logs'||table==='runs')return terminal.has(f.status)&&Number.isFinite(Date.parse(f.completed));
 return true;
}
function protectedRows(rows,errors,heartbeats){
 const ids=new Set(heartbeats.flatMap(r=>r.fields['last-run']||[]));
 // Preserve latest per-workflow invocation even between scheduled wakes.
 const latest=new Map();
 for(const r of rows){const k=r.fields.workflow;const prev=latest.get(k);if(!prev||String(r.fields.started)>String(prev.fields.started))latest.set(k,r);}
 for(const r of latest.values())ids.add(r.id);
 for(const r of rows)if(!completed('run-logs',r))ids.add(r.id);
 const errorsProtected=new Set(errors.filter(r=>(r.fields.run||[]).some(id=>ids.has(id))).map(r=>r.id));
 return new Set([...ids,...errorsProtected,...heartbeats.map(r=>r.id)]);
}
module.exports={PRIMARY,LOGS,showOf,completed,protectedRows};

