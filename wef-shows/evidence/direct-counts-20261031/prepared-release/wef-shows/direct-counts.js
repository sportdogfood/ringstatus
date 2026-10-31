"use strict";
// Source detail lives only in memory. This module never writes files or entity records.
const crypto=require('node:crypto');
const ORIGIN='https://wef-schedules-decoded.gombcg.workers.dev';
const integer=v=>{const n=typeof v==='number'?v:typeof v==='string'&&/^\d+$/.test(v)?Number(v):NaN;return Number.isSafeInteger(n)&&n>0?n:null;};
const nonnegative=v=>{const n=typeof v==='number'?v:typeof v==='string'&&/^\d+$/.test(v)?Number(v):NaN;return Number.isSafeInteger(n)&&n>=0?n:null;};
const day=v=>typeof v==='string'?v.slice(0,10):'';
function target(route,sid,date){if(!integer(sid)||!(route==='/schedule'||/^\/classes\/[1-9]\d*$/.test(route)))throw Error('Unapproved count scope');const u=new URL(route,ORIGIN);u.search=new URLSearchParams({show_id:sid,customer_id:15,...(route==='/schedule'?{date}:{})});return u.toString();}
async function source(url,{deadline,request=globalThis.fetch,time=Date.now}={}){
 const {fetchSourceText}=require('./source-fetch');
 const result=await fetchSourceText(url,{request:async(u,options)=>{const left=deadline-time();if(left<=0)throw Error('Count request budget exhausted');return request(u,{...options,redirect:'error',signal:AbortSignal.timeout(Math.max(1,Math.min(18000,left)))});}});
 if(result.response.headers?.get('x-rs-decoded')!=='1')throw Error('Count source decoding not confirmed');
 if(Buffer.byteLength(result.text)>8*1024*1024)throw Error('Count response exceeds memory limit');
 let payload;try{payload=JSON.parse(result.text.replace(/^\uFEFF/,''));}catch{throw Error('Invalid count JSON response');}if(!payload||typeof payload!=='object'||Array.isArray(payload)||payload._t)throw Error('Invalid count response');
 // These existing routes return full dated schedules/class trips. Never label an
 // explicitly paginated/short response complete or invent unsupported page URLs.
 if(payload.next||payload.next_page||payload.pagination?.next)throw Error('Paginated count response incomplete');
 return{payload,sha256:crypto.createHash('sha256').update(result.text).digest('hex')};
}
async function pool(items,fn){let cursor=0;await Promise.all(Array.from({length:Math.min(4,items.length)},async()=>{while(cursor<items.length){const index=cursor++;await fn(items[index]);}}));}
async function countDays({show,runId,clock=()=>new Date().toISOString(),deadline=Date.now()+150000,time=Date.now,fetchCount=source}){
 const f=show.fields,sid=integer(f.show_id);if(!sid||Number(f.customer_id)!==15||f['show-key']!==`15|${sid}`)throw Error('Invalid count show authority');
 const dates=require('./show-days').dates(show),ledger=crypto.createHash('sha256'),states=dates.map(date=>({date,expected:new Map(),results:[],errors:[],observed:null,inventory:null}));let requests=0;
 const read=async(route,date)=>{if(time()>=deadline)throw Error('Count request budget exhausted');const at=clock();requests++;const value=await fetchCount(target(route,sid,date),{deadline,time});if(!value||!value.payload||!/^[a-f0-9]{64}$/.test(value.sha256))throw Error('Missing count response receipt');ledger.update(route+'|'+(date||'')+'|'+value.sha256+'\n');return{...value,at};};
 const inventory=p=>{const classes=new Map(),meta=p.show||{},rings=new Set();if(Number(meta.show_id)!==sid||day(meta.start_date)!==f.start_date||day(meta.end_date)!==f.end_date||!Array.isArray(p.rings))throw Error('Count inventory scope/shape mismatch');
  for(const ring of p.rings){const rn=integer(ring.ring_number);if(!rn||rings.has(rn)||!Array.isArray(ring.classes))throw Error('Count inventory ring ambiguous');rings.add(rn);
   for(const c of ring.classes){if([true,1,'1','true'].includes(c.schedule_break))continue;const cid=integer(c.class_id);if(!cid)throw Error('Count inventory class identity missing');if(classes.has(cid)&&classes.get(cid)!==rn)throw Error('Count inventory class association ambiguous');classes.set(cid,rn);}}
  return classes;};
 await pool(states,async st=>{try{const v=await read('/schedule',st.date);if(v.payload.show_date!==st.date)throw Error('Wrong count inventory date');st.expected=inventory(v.payload);st.observed=v.at;st.inventory=true;}catch(e){st.errors.push(e.message);}});
 const jobs=states.flatMap(st=>[...st.expected].map(([cid,ring])=>({st,cid,ring})));
 await pool(jobs,async({st,cid,ring})=>{try{const {payload:p,at}=await read('/classes/'+cid,st.date),related=p.class_related_data||{};
  if(Number(p.show_id)!==sid||integer(p.class?.class_id)!==cid||day(related.date)!==st.date||integer(related.ring)!==ring)throw Error('Count class/date/ring mismatch');
  if(!Array.isArray(p.trips)||nonnegative(p.total_entry_trips)!==p.trips.length)throw Error('Incomplete class trip response');
  const entries=new Set(),trips=new Map();for(const t of p.trips){const eid=integer(t.entry_id),uuid=t.entryxclasses_uuid,tid=uuid?typeof uuid==='string'&&uuid.trim()===uuid&&uuid?('uuid:'+uuid):null:integer(t.trip_id)?'trip:'+integer(t.trip_id):null;
   if(!eid||!tid)throw Error('Missing count entry/trip identity');const owner=cid+'|'+eid;if(trips.has(tid)&&trips.get(tid)!==owner)throw Error('Conflicting count trip identity');trips.set(tid,owner);entries.add(eid);}
  st.results.push({cid,entries,trips,at});
 }catch(e){st.errors.push('Class '+cid+': '+e.message);}});
 // Confirm the class roster did not change while counts were obtained.
 await pool(states.filter(st=>st.inventory),async st=>{try{const v=await read('/schedule',st.date);if(v.payload.show_date!==st.date)throw Error('Count inventory date changed');const again=inventory(v.payload);const sorted=m=>JSON.stringify([...m].sort((a,b)=>a[0]-b[0]));if(sorted(again)!==sorted(st.expected))throw Error('Count inventory changed during check');}catch(e){st.inventory=false;st.errors.push(e.message);}});
 const receipt=ledger.digest('hex'),days=[],failures=[],diagnostics=[];
 for(const st of states){const entries=new Set(),trips=new Map();let accepted=0;for(const c of st.results){let conflict=false;for(const [tid,owner]of c.trips)if(trips.has(tid)&&trips.get(tid)!==owner)conflict=true;if(conflict){st.errors.push('Conflicting trip identity across classes');continue;}accepted++;for(const eid of c.entries)entries.add(eid);for(const [tid,owner]of c.trips)trips.set(tid,owner);}
  const complete=st.inventory&&accepted===st.expected.size&&st.errors.length===0,known=st.inventory&&(accepted>0||st.expected.size===0);
  diagnostics.push({date:st.date,expected_classes:st.expected.size,counted_classes:accepted,complete,errors:st.errors.slice(0,12)});
  if(!st.inventory){failures.push({date:st.date,message:st.errors.join('; ').slice(0,1500)});continue;}
  days.push({format:'wef-day-count-summary-v1','policy-version':'direct-proxy-counts-v1','customer-id':15,'show-key':f['show-key'],'day-key':`${f['show-key']}|${st.date}`,date:st.date,coverage:'competition-class-participation',completeness:complete?'complete':known?'partial':'unknown','unique-entry-count':known?entries.size:null,'proven-trip-count':known?trips.size:null,'accepted-as-of':known?st.observed:null,'verified-at':clock(),reasons:st.errors.length?st.errors.slice(0,12):['Scheduled competition-class counts; schooling and classless coverage is unproved.'],evidence:{'run-key':runId,'state-sha256':receipt,source:'direct-proxy','expected-class-count':st.expected.size,'counted-class-count':accepted}});
 }
 return{mode:'direct-proxy',days,failures,diagnostics,request_count:requests,origin:ORIGIN};
}
module.exports={source,target,countDays};
