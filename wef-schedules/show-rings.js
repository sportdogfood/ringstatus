"use strict";
const settings=require('./settings'),legacy=require('./collector-core');
const blank=v=>v===undefined||v===null||v==='';
const normalized=v=>blank(v)?null:String(v).trim().toLowerCase();
const positive=(v,label)=>{const n=Number(v);if(blank(v)||!Number.isSafeInteger(n)||n<=0)throw Error('Invalid '+label);return n;};
const same=(a,b)=>typeof b==='boolean'?Boolean(a)===b:JSON.stringify(a??null)===JSON.stringify(b??null);
function resolveHelper(records,scope,ring){
 const candidates=records.filter(r=>Number(r.fields.customer_id)===scope.customer_id&&Number(r.fields.facility_id)===scope.facility_id);
 const names=['ring_id','ring_number','ring_name'];
 const matches=candidates.filter(r=>names.some(k=>!blank(ring[k])&&!blank(r.fields[k])&&normalized(ring[k])===normalized(r.fields[k])));
 if(matches.length>1)throw Error('Ambiguous rings-helper identifiers');
 const found=matches[0]||null;
 if(found&&names.some(k=>!blank(ring[k])&&!blank(found.fields[k])&&normalized(ring[k])!==normalized(found.fields[k])))throw Error('Conflicting rings-helper identifiers');
 return found;
}
function plan({show,payload,payloads,days,now}){
 const f=show.fields,showId=positive(f.show_id,'show ID'),customer=positive(f.customer_id,'customer ID');
 const facility=positive(payload?.show?.facility_id,'facility ID'),date=require('./collect').isoDate;
 const start=date(f.start_date),end=date(f.end_date),selected=date(f.sgl_default_day);
 if(start>end||selected<start||selected>end||date(payload.show_date??payload.showDate)!==selected)throw Error('Invalid source-selected schedule date');
 if(!days.length||days.some(d=>!d.ok))throw Error('Complete schedule days required before show/ring sync');
 const expected=require('./collect').selectedDays(f).sort(),actual=days.map(d=>d.date).sort();
 if(!same(expected,actual))throw Error('Show/ring date coverage mismatch');
 const scope={customer_id:customer,facility_id:facility},rings=[];
 for(const day of days){
  const p=payloads.get(day.date);
  if(!p||Number(p.show?.show_id??p.show_id)!==showId||date(p.show_date??p.showDate)!==day.date||!Array.isArray(p.rings))throw Error('Invalid dated ring payload '+day.date);
  if(p.show?.facility_id!=null&&Number(p.show.facility_id)!==facility)throw Error('Dated ring facility mismatch');
  const seen=new Set();
  for(const raw of p.rings){
   const ring={ring_number:positive(raw.ring_number,'ring number'),ring_id:blank(raw.ring_id)?null:positive(raw.ring_id,'ring ID'),ring_name:blank(raw.ring_name)?null:String(raw.ring_name).trim()};
   if(!ring.ring_id&&!ring.ring_name)throw Error('Ring identity missing');
   if(seen.has(ring.ring_number))throw Error('Duplicate dated ring number');seen.add(ring.ring_number);
   rings.push({...scope,...ring,show_id:showId,ring_date:day.date,'ring-key':`${customer}|${showId}|${day.date}|${ring.ring_number}`,is_selected_date:day.date===selected});
  }
 }
 return {scope,rings,show:{'show-key':`${customer}|${showId}`,show_id:showId,...scope,show_name:String(f.show_name||''),start_date:start,end_date:end,selected_date:selected,available_dates:expected.join(', '),is_default:true,last_checked:now||new Date().toISOString()}};
}
function showRings({api=legacy,request=globalThis.fetch,showsTable=settings.SHOWS_TABLE,pause=ms=>new Promise(resolve=>setTimeout(resolve,ms))}={}){
 const base=settings.SCHEDULE_STAGING_BASE_ID;
 const list=table=>api.airtableList(table,{},base);
 function unique(records,key){const seen=new Set();for(const r of records){const v=r.fields[key];if(blank(v)||seen.has(v))throw Error('Missing or duplicate '+key);seen.add(v);}}
 const verify=(records,wanted,key)=>{
  unique(records,key);
  return wanted.map(fields=>{const r=records.find(r=>r.fields[key]===fields[key]);if(!r)throw Error('Show/ring readback missing '+key);for(const [k,v]of Object.entries(fields))if(!same(r.fields[k],v)&&!(Array.isArray(v)&&v.length===0&&blank(r.fields[k])))throw Error('Show/ring readback mismatch '+k);return r;});
 };
 async function patch(table,rows){if(!rows.length)return;const result=await api.airtablePatchRecords(table,rows,base);if(result.failedRows?.length)throw Error('Show/ring patch failed '+table);}
 async function upsert(table,key,wanted,prior){
  unique(prior,key);const patches=[],creates=[];
  for(const fields of wanted){const old=prior.find(r=>r.fields[key]===fields[key]);if(old){const delta=Object.fromEntries(Object.entries(fields).filter(([k,v])=>!same(old.fields[k],v)));if(Object.keys(delta).length)patches.push({id:old.id,fields:delta});}else creates.push({fields});}
  await patch(table,patches);
  for(let i=0;i<creates.length;i+=10){
   const batch=creates.slice(i,i+10);let error;
   await pause(500);
   // Never repeat a creation POST. A lost response permits read-only lookup
   // by stable keys; an unresolved write fails this stage and the run log.
   try {const response=await request(`https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}`,{method:'POST',headers:{Authorization:`Bearer ${settings.AIRTABLE_TOKEN}`,'Content-Type':'application/json'},body:JSON.stringify({records:batch}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('HTTP '+response.status);}catch(e){error=e;}
   try {verify(await list(table),batch.map(r=>r.fields),key);}catch(e){throw Error((error?'Show/ring creation outcome unresolved: ':'')+e.message);}
  }
  return verify(await list(table),wanted,key);
 }
 return {async sync(input){
  const p=plan(input),showId=p.show.show_id;
  const [shows,helpers,oldRings,schedules]=await Promise.all([showsTable,'rings-helper','rings','schedules'].map(list));
  unique(shows,'show-key');unique(helpers,'ring-key');unique(oldRings,'ring-key');
  // Resolve the entire source and all schedule references before any writes.
  const helperPlans=[],helperByRing=new Map();
  for(const ring of p.rings){
   const known=[...helpers.map(r=>({...r,fields:helperPlans.find(f=>f['ring-key']===r.fields['ring-key'])||r.fields})),...helperPlans.filter(f=>!helpers.some(r=>r.fields['ring-key']===f['ring-key'])).map(fields=>({fields}))];
   const found=resolveHelper(known,p.scope,ring),fields=found?{...found.fields}:{'ring-key':`${p.scope.customer_id}|${p.scope.facility_id}|${ring.ring_id?'id:'+ring.ring_id:'number:'+ring.ring_number}`,...p.scope};
   for(const k of ['ring_id','ring_number','ring_name'])if(blank(fields[k])&&!blank(ring[k]))fields[k]=ring[k];
   // Only identity fields belong to the collector. Owner notes/static fields
   // are never sent in a write, even when an identity is first filled in.
   const owned=Object.fromEntries(['ring-key','customer_id','facility_id','ring_id','ring_number','ring_name'].filter(k=>!blank(fields[k])).map(k=>[k,fields[k]]));
   const index=helperPlans.findIndex(r=>r['ring-key']===owned['ring-key']);if(index<0)helperPlans.push(owned);else helperPlans[index]=owned;
   helperByRing.set(ring['ring-key'],owned['ring-key']);
  }
  const scopedSchedules=schedules.filter(r=>Number(r.fields['show-id'])===showId),schedulePlan=[];
  for(const record of scopedSchedules){
   const matches=p.rings.filter(r=>r.ring_date===record.fields.date&&r.ring_number===Number(record.fields.ring));
   if(matches.length===1){schedulePlan.push({record,ringKey:matches[0]['ring-key']});continue;}
   if(matches.length||record.fields.status!=='dropped')throw Error('Schedule ring mapping missing or ambiguous '+record.id);
   const retained=oldRings.filter(r=>Number(r.fields.show_id)===showId&&Number(r.fields.customer_id)===p.scope.customer_id&&Number(r.fields.facility_id)===p.scope.facility_id&&r.fields.ring_date===record.fields.date&&Number(r.fields.ring_number)===Number(record.fields.ring));
   if(retained.length>1)throw Error('Historical schedule ring mapping ambiguous '+record.id);
   // Dropped rows are retained by the existing collector. Reuse their proven
   // daily ring, or leave the new link empty on first adoption; never invent
   // a current-source ring or prevent active schedule collection.
   schedulePlan.push({record,retainedId:retained[0]?.id||null});
  }
  const savedHelpers=await upsert('rings-helper','ring-key',helperPlans,helpers);
  const showRows=await upsert(showsTable,'show-key',[p.show],shows),showRecord=showRows[0];
  const wantedRings=p.rings.map(r=>({...r,'show-details':[showRecord.id],'rings-helper':[savedHelpers.find(h=>h.fields['ring-key']===helperByRing.get(r['ring-key'])).id]}));
  const savedRings=await upsert('rings','ring-key',wantedRings,oldRings);
  const links=schedulePlan.map(({record,ringKey,retainedId})=>({id:record.id,fields:{'show-details':[showRecord.id],rings:ringKey?[savedRings.find(r=>r.fields['ring-key']===ringKey).id]:retainedId?[retainedId]:[]}}));
  await patch('schedules',links.filter(r=>Object.entries(r.fields).some(([k,v])=>!same(scopedSchedules.find(old=>old.id===r.id).fields[k],v))));
  const savedSchedules=await list('schedules');for(const r of links){const saved=savedSchedules.find(s=>s.id===r.id);for(const[k,v]of Object.entries(r.fields))if(!same(saved?.fields[k]??(Array.isArray(v)?[]:null),v))throw Error('Schedule link readback mismatch '+r.id);}
  // Old show/day records remain available. Only selection flags change;
  // this addition never deletes records or bypasses schedule archives.
  const staleShows=shows.filter(r=>r.id!==showRecord.id&&r.fields.customer_id===p.scope.customer_id&&r.fields.is_default).map(r=>({id:r.id,fields:{is_default:false}}));
  const wantedKeys=new Set(wantedRings.filter(r=>r.is_selected_date).map(r=>r['ring-key']));
  const staleRings=oldRings.filter(r=>r.fields.customer_id===p.scope.customer_id&&r.fields.is_selected_date&&!wantedKeys.has(r.fields['ring-key'])).map(r=>({id:r.id,fields:{is_selected_date:false}}));
  await patch(showsTable,staleShows);await patch('rings',staleRings);
  for(const[table,stale]of [[showsTable,staleShows],['rings',staleRings]])if(stale.length){const saved=await list(table);for(const r of stale)for(const[k,v]of Object.entries(r.fields))if(Boolean(saved.find(s=>s.id===r.id)?.fields[k])!==v)throw Error('Selection readback mismatch');}
  return {ok:true,show_id:showId,selected_date:p.show.selected_date,shows:1,rings:savedRings.length,selected_rings:wantedRings.filter(r=>r.is_selected_date).length,helpers:savedHelpers.length,schedule_links:links.filter(r=>r.fields.rings.length).length,dropped_without_ring:links.filter(r=>!r.fields.rings.length).length};
 }};
}
module.exports={showRings,resolveHelper};
