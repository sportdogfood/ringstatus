"use strict";
const settings=require('./settings');
const {assertProspectiveRoster}=require('./identity-policy');
const same=(a,b)=>JSON.stringify(a==null||a===''?null:a)===JSON.stringify(b==null||b===''?null:b)||((a==null||a==='')&&(b===false||Array.isArray(b)&&!b.length));
function unique(rows,key){const seen=new Set();for(const r of rows){const k=r.fields[key];if(k==null||k===''||seen.has(k))throw Error('Missing or duplicate '+key);seen.add(k);}}
function airtable({token=settings.AIRTABLE_TOKEN,request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 const unresolved=new Set();
 const legacyBase='apptdhhNzduxm5gjn',legacyTables=new Set(['ww_tenants','ww_horses','ww_riders']);
 const identity=(base,table,key,value)=>JSON.stringify([base,table,key,value]);
 const readOnlyReferences=new Set([settings.SYNC_SHOWS_TABLE,settings.SYNC_RINGS_TABLE,settings.SYNC_SCHEDULES_TABLE,'wef-schedules.shows','wef-schedules.rings','wef-schedules.schedules','tblxutSCBQzmkJLLO','tblsVT0Poao9wKhs4','show-details','rings','retired-show-details','retired-rings']);
 async function call(base,table,method='GET',body,query=''){
  if(base===legacyBase){if(method!=='GET'||!legacyTables.has(table))throw Error('Legacy roster is read-only and table restricted');}
  else if(![settings.BASE_ID,settings.SHARED_BASE_ID].includes(base))throw Error('Unapproved base');
  if(method!=='GET'&&base===settings.SHARED_BASE_ID&&table!=='wef-tenant-index')throw Error('Shared schedule writes forbidden');
  if(method!=='GET'&&base===settings.BASE_ID&&readOnlyReferences.has(table))throw Error('Schedule references are read-only');
  await pause(250);const r=await request(`https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}${query}`,{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(30000)});
  if(!r.ok)throw Error('Airtable '+method+' '+table+' HTTP '+r.status);return r.json();
 }
 async function list(table,base=settings.BASE_ID,options={}){const rows=[];let offset;do{const q=new URLSearchParams({pageSize:'100',...(options.filterByFormula?{filterByFormula:options.filterByFormula}:{}),...(offset?{offset}:{})});for(const field of options.fields||[])q.append('fields[]',field);const r=await call(base,table,'GET',undefined,'?'+q);rows.push(...r.records);offset=r.offset;}while(offset);return rows;}
 async function verify(table,key,fields,base){const rows=await list(table,base);unique(rows,key);return fields.map(f=>{const r=rows.find(r=>r.fields[key]===f[key]);if(!r)throw Error('Missing saved '+table+' '+f[key]);for(const[k,v]of Object.entries(f))if(!same(r.fields[k],v))throw Error('Readback mismatch '+table+'.'+k);return r;});}
 async function upsert(table,key,fields,base=settings.BASE_ID){const before=await list(table,base);unique(before,key);unique(fields.map(fields=>({fields})),key);assertProspectiveRoster(table,key,before,fields);const creates=[],patches=[];
  for(const f of fields){const old=before.find(r=>r.fields[key]===f[key]);if(!old){if(unresolved.has(identity(base,table,key,f[key])))throw Error('Creation outcome unresolved; failure handler cannot resubmit '+table+' '+f[key]);creates.push({fields:f});}else{unresolved.delete(identity(base,table,key,f[key]));const delta=Object.fromEntries(Object.entries(f).filter(([k,v])=>!same(old.fields[k],v)));if(Object.keys(delta).length)patches.push({id:old.id,fields:delta});}}
  for(let i=0;i<patches.length;i+=10)await call(base,table,'PATCH',{records:patches.slice(i,i+10)});
  for(let i=0;i<creates.length;i+=10){const batch=creates.slice(i,i+10);let problem;try{await call(base,table,'POST',{records:batch});}catch(e){problem=e;}
   // Creation POSTs are never retried, including after a lost response.
   try{await verify(table,key,batch.map(r=>r.fields),base);}catch(e){for(const r of batch)unresolved.add(identity(base,table,key,r.fields[key]));throw Error('Creation outcome unresolved: '+e.message);}
  }return verify(table,key,fields,base);
 }
 async function remove(table,ids){for(let i=0;i<ids.length;i+=10){const batch=ids.slice(i,i+10),q=new URLSearchParams();for(const id of batch)q.append('records[]',id);const r=await call(settings.BASE_ID,table,'DELETE',undefined,'?'+q);if(r.records?.length!==batch.length||r.records.some(r=>!r.deleted||!batch.includes(r.id)))throw Error('Archive cleanup mismatch');}}
 return{list,upsert,remove,call};
}
module.exports={airtable,same,unique};
