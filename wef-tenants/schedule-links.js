"use strict";
// Transport and destination-local resolution only. Never infer group eligibility.
function keys(row){
 const scalar=row['schedule-key'];
 if(!Object.hasOwn(row,'schedule-keys')){
  if(typeof scalar!=='string'||!scalar.trim())throw Error('Missing schedule key');
  return[scalar];
 }
 let values;try{values=JSON.parse(row['schedule-keys']);}catch{throw Error('Malformed schedule-keys');}
 if(!Array.isArray(values)||!values.length||values.some(k=>typeof k!=='string'||!k.trim())||JSON.stringify(values)!==JSON.stringify([...new Set(values)].sort()))throw Error('Invalid sorted unique schedule-keys');
 if(values.length===1?scalar!==values[0]:Boolean(scalar))throw Error('Conflicting scalar schedule-key');
 return values;
}
function resolve(row,schedules){
 return keys(row).map(key=>{
  const matches=schedules.filter(r=>r.fields['stage-key']===key);
  if(matches.length!==1)throw Error('Schedule reference missing or ambiguous');
  const f=matches[0].fields;
  if(Number(f['show-id'])!==Number(row['show-id'])||f.date!==row.date||Number(f['class-id'])!==Number(row['class-id'])||f.status==='dropped'||(Object.hasOwn(row,'schedule-keys')&&f.status!=='active'))throw Error('Schedule reference identity or status mismatch');
  return matches[0];
 });
}
function fresh(value,now){const age=Date.parse(now)-Date.parse(value);return Number.isFinite(age)&&age>=-60000&&age<=1800000;}
function health(collections,show,dates,now){
 for(const date of dates){const rows=collections.filter(r=>Number(r.fields['show-id'])===Number(show.show_id)&&r.fields.date===date);
  if(rows.length!==1||rows[0].fields.status!=='complete'||!fresh(rows[0].fields.completed,now))throw Error('Schedule date collection missing, failed or stale: '+date);
 }
}
// The caller must supply attributable participation evidence, never class membership.
// Current people payloads provide none; production intentionally uses only unique matches.
function eligible(candidates,groupIds){
 const active=candidates.filter(r=>r.fields.status==='active');
 if(groupIds===undefined){if(active.length!==1)throw Error('Participation group eligibility unresolved');return active;}
 if(!Array.isArray(groupIds)||!groupIds.length||groupIds.some(x=>!Number.isSafeInteger(x)||x<=0)||new Set(groupIds).size!==groupIds.length)throw Error('Invalid participation group evidence');
 return groupIds.map(id=>{const rows=active.filter(r=>Number(r.fields['group-id'])===id);if(rows.length!==1)throw Error('Eligible group schedule missing or ambiguous');return rows[0];});
}
function transport(schedules){
 const values=schedules.map(r=>r.fields['stage-key']).sort();
 if(!values.length||new Set(values).size!==values.length)throw Error('Invalid eligible schedule set');
 const rings=[...new Set(schedules.map(r=>r.fields.ring))];
 return{'schedule-key':values.length===1?values[0]:'','schedule-keys':JSON.stringify(values),'ring-number':rings.length===1?rings[0]:null};
}
module.exports={keys,resolve,fresh,health,eligible,transport};
