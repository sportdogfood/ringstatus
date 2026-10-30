"use strict";
const settings=require('./settings'),{readAirtable}=require('./airtable-read');
const own=new Set(['entries','trips','entry-collection']);
function entryAPI({request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 const base=settings.SCHEDULE_STAGING_BASE_ID,headers={Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'};
 const url=table=>{if(!own.has(table)&&!['schedules','rings','shows'].includes(table))throw Error('Unapproved Schedules entry table');return `https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}`;};
 async function list(table){const out=[];let offset;do{await pause(220);const u=new URL(url(table));u.searchParams.set('pageSize','100');if(offset)u.searchParams.set('offset',offset);const d=await readAirtable(u.href,{request,headers});out.push(...d.records);offset=d.offset;}while(offset);return out;}
 const cell=x=>x===''||x==null?null:x;
 const equal=(a,b)=>JSON.stringify(cell(a)??(Array.isArray(b)?[]:null))===JSON.stringify(cell(b));
 const index=(rows,key)=>{const m=new Map();for(const r of rows){const k=r.fields[key];if(!k||m.has(k))throw Error('Duplicate/missing '+key);m.set(k,r);}return m;};
 async function save(table,key,rows){if(!own.has(table))throw Error('Forbidden entry write');const wanted=index(rows.map(fields=>({fields})),key),old=index(await list(table),key),changes=[];
  for(const [k,r] of wanted){const prior=old.get(k);if(!prior||Object.entries(r.fields).some(([f,v])=>!equal(prior.fields[f],v)))changes.push({fields:r.fields});}
  for(let i=0;i<changes.length;i+=10){const batch=changes.slice(i,i+10);await pause(220);let error;
   try{const response=await request(url(table),{method:'PATCH',headers,body:JSON.stringify({performUpsert:{fieldsToMergeOn:[key]},records:batch}),signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Entry upsert HTTP '+response.status);}
   catch(e){error=e;}
   // Never blindly repeat an uncertain write. Exact keys and fields decide it.
   if(error){const found=index(await list(table),key);for(const r of batch){const actual=found.get(r.fields[key]);if(!actual||Object.entries(r.fields).some(([f,v])=>!equal(actual.fields[f],v)))throw Error('Entry write outcome unresolved: '+error.message);}}
  }
  const found=index(await list(table),key);for(const [k,r]of wanted){const actual=found.get(k);if(!actual||Object.entries(r.fields).some(([f,v])=>!equal(actual.fields[f],v)))throw Error('Entry readback mismatch '+table+' '+k);}
  return [...wanted.keys()].map(k=>found.get(k));
 }
 return{list,save};
}
module.exports={entryAPI};
