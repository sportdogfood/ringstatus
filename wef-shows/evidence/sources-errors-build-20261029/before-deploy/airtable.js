"use strict";
// Adapted from wef-tenants/airtable.js. Destination-only writes; no record deletes.
const settings=require('./settings');
const {same,unique}=require('../wef-tenants/airtable');
function airtable({token=settings.AIRTABLE_TOKEN,request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 const writable=new Set(['shows','run-logs','heartbeat','show-days']),unresolved=new Set();
 const readable=new Map([[settings.BASE_ID,new Set([...writable])],['appdl16HJ5ETm8N69',new Set(['runs','collections'])]]);
 async function call(base,table,method='GET',body,query=''){
  if(!readable.get(base)?.has(table))throw Error('Unapproved read target');
  if(method!=='GET'&&(base!==settings.BASE_ID||!writable.has(table)||!['POST','PATCH'].includes(method)))throw Error('Read-only source or forbidden mutation');
  await pause(250);const r=await request(`https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}${query}`,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
  if(!r.ok)throw Error(`Airtable ${method} ${table} HTTP ${r.status}`);return r.json();
 }
 async function list(table,base=settings.BASE_ID,filter){const rows=[];let offset;do{const q=new URLSearchParams({pageSize:'100',...(offset?{offset}:{}),...(filter?{filterByFormula:filter}:{})});const d=await call(base,table,'GET',undefined,'?'+q);rows.push(...d.records);offset=d.offset;}while(offset);return rows;}
 const iso=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)&&Number.isFinite(Date.parse(value));
 const equal=(a,b)=>iso(a)&&iso(b)?Date.parse(a)===Date.parse(b):Array.isArray(a)&&Array.isArray(b)?JSON.stringify([...a].sort())===JSON.stringify([...b].sort()):same(a,b);
 async function verify(table,key,fields){const rows=await list(table);unique(rows,key);return fields.map(f=>{const r=rows.find(r=>r.fields[key]===f[key]);if(!r)throw Error('Missing saved '+table+' '+f[key]);for(const[k,v]of Object.entries(f))if(!equal(r.fields[k],v))throw Error('Readback mismatch '+table+'.'+k);return r;});}
 async function upsert(table,key,fields){if(!writable.has(table))throw Error('Read-only destination');if(!fields.length)return[];unique(fields.map(fields=>({fields})),key);const before=await list(table);unique(before,key);const creates=[],patches=[];
  for(const f of fields){const old=before.find(r=>r.fields[key]===f[key]);if(old){const delta=Object.fromEntries(Object.entries(f).filter(([k,v])=>!equal(old.fields[k],v)));if(Object.keys(delta).length)patches.push({id:old.id,fields:delta});}else{const id=table+'|'+f[key];if(unresolved.has(id))throw Error('Creation outcome unresolved');creates.push({fields:f});}}
  for(let i=0;i<patches.length;i+=10)await call(settings.BASE_ID,table,'PATCH',{records:patches.slice(i,i+10)});
  for(let i=0;i<creates.length;i+=10){const batch=creates.slice(i,i+10);try{await call(settings.BASE_ID,table,'POST',{records:batch});}catch(error){try{await verify(table,key,batch.map(r=>r.fields));}catch{for(const r of batch)unresolved.add(table+'|'+r.fields[key]);throw Error('Creation outcome unresolved: '+error.message);}}await verify(table,key,batch.map(r=>r.fields));}
  return verify(table,key,fields);
 }
 return{list,upsert,call};
}
module.exports={airtable};
