"use strict";
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const settings=require('./settings'),legacy=require('./collector-core');
const positive=v=>Number.isSafeInteger(Number(v))&&Number(v)>0;
const same=(a,b)=>typeof b==='boolean'?Boolean(a)===b:JSON.stringify(a??null)===JSON.stringify(b??null)||((a==null||a==='')&&(b===''||Array.isArray(b)&&!b.length));
function archiveReferences(logs,shows=[]){
 const files=new Map();
 for(const log of [...logs,...shows.map(r=>({fields:{archives:r.fields['archive-files']}}))]){
  const pattern=/(?:File:\s*|;\s*)([^;\r\n]+\.json\.gz)(?:;\s*SHA256\s+|\r?\nVerified decompressed SHA256:\s*)([a-f0-9]{64})/g;
  for(const match of String(log.fields.archives||'').matchAll(pattern)){
   const file=match[1].trim();if(!/[\\/]retired[\\/]/.test(file))continue;
   if(files.has(file)&&files.get(file).sha256!==match[2])throw Error('Conflicting retired archive hashes '+file);
   const prior=files.get(file);files.set(file,{file,sha256:match[2],logs:[...(prior?.logs||[]),log]});
  }
 }
 return [...files.values()];
}
function readVerifiedArchive(reference,root){
 const relative=path.relative(path.resolve(root),path.resolve(reference.file));
 if(relative.startsWith('..')||path.isAbsolute(relative))throw Error('Archive outside schedules history '+reference.file);
 const body=zlib.gunzipSync(fs.readFileSync(reference.file));
 if(crypto.createHash('sha256').update(body).digest('hex')!==reference.sha256)throw Error('Show archive hash mismatch '+reference.file);
 const records=JSON.parse(body);if(!Array.isArray(records))throw Error('Retired show archive is not a record array');return records;
}
function historicalDetails(logs){
 for(const log of logs){
  const m=String(log.fields.summary||'').match(/^(.+?)\s+[—–]\s+([A-Za-z]+)\s+(\d{1,2})[–-](\d{1,2}),\s*(\d{4})/);
  if(!m)continue;const month=['January','February','March','April','May','June','July','August','September','October','November','December'].indexOf(m[2])+1;
  if(month&&Number(m[3])<=Number(m[4]))return{show_name:m[1],start_date:`${m[5]}-${String(month).padStart(2,'0')}-${m[3].padStart(2,'0')}`,end_date:`${m[5]}-${String(month).padStart(2,'0')}-${m[4].padStart(2,'0')}`};
 }
 return{};
}
function planCatalog({logs,shows,collections,schedules,readArchive,verifiedAt}){
 const grouped=new Map();
 for(const reference of archiveReferences(logs,shows)){
  const records=readArchive(reference),parts=new Map();
  for(const record of records){
   const f=record.fields||{},id=Number(f['show-id']??f.show_id);
   if(!positive(id)||!record.id)throw Error('Archived show identity missing '+reference.file);
   const kind=f['stage-key']||f['row-json']?'schedules':f['collection-id']?'show-collection':null;if(!kind)throw Error('Unknown retired archive record type');
   const key=id+'|'+kind;if(!parts.has(key))parts.set(key,{id,kind,records:[]});parts.get(key).records.push(record);
  }
  for(const part of parts.values()){
   if(!grouped.has(part.id))grouped.set(part.id,{files:[],logs:[],schedules:new Set(),collections:new Set()});
   const group=grouped.get(part.id);group.files.push({kind:part.kind,file:reference.file,sha256:reference.sha256,records:part.records.length});group.logs.push(...reference.logs);
   for(const record of part.records)group[part.kind==='schedules'?'schedules':'collections'].add(record.id);
  }
 }
 const result=[];
 const ids=new Set([...shows.map(r=>Number(r.fields.show_id)),...grouped.keys()]);
 for(const id of ids){
  if(!positive(id))throw Error('Invalid show catalog identity');const found=shows.filter(r=>Number(r.fields.show_id)===id);if(found.length>1)throw Error('Ambiguous show catalog identity '+id);
  const prior=found[0]?.fields,group=grouped.get(id),active=prior?.is_default===true;
  const retained=[...schedules,...collections].some(r=>Number(r.fields['show-id'])===id);
  const fields={'show-key':prior?.['show-key']||`${settings.CUSTOMER_ID}|${id}`,show_id:id,'show-status':active?'current':group&&!retained?'archived':'retained'};
  if(!prior)Object.assign(fields,{customer_id:Number(settings.CUSTOMER_ID),is_default:false,...historicalDetails(group?.logs||[])});
  if(group)Object.assign(fields,{'archive-status':'Verified','archive-files':group.files.sort((a,b)=>a.file.localeCompare(b.file)).map(f=>`${f.kind}: ${f.records} records; ${f.file}; SHA256 ${f.sha256}`).join('\n'),'archived-schedule-count':group.schedules.size,'archived-collection-count':group.collections.size,'archive-verified-at':verifiedAt,'archive-notes':'Verified local runner files; paths are not hosted download links. Counts identify unique archived records. Original archive time is not inferred from verification time.'});
  result.push(fields);
 }
 return result;
}
function showIndex({api=legacy,request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms)),historyRoot=path.join(path.dirname(settings.SCHEDULE_FOCUS_FILE),'history'),readArchive=r=>readVerifiedArchive(r,historyRoot)}={}){
 const base=settings.SCHEDULE_STAGING_BASE_ID,list=table=>api.airtableList(table,{},base);
 async function patch(table,records){if(!records.length)return;const saved=await api.airtablePatchRecords(table,records,base);if(saved.failedRows?.length)throw Error('Show index patch failed '+table);}
 function unique(rows,key){const seen=new Set();for(const r of rows){const value=r.fields[key];if(!value||seen.has(value))throw Error('Missing or duplicate show index '+key);seen.add(value);}}
 async function saveShows(wanted,old){
  unique(old,'show-key');const creates=[];
  await patch(settings.SHOWS_TABLE,wanted.flatMap(fields=>{const prior=old.find(r=>r.fields['show-key']===fields['show-key']);if(!prior){creates.push({fields});return[];}const delta=Object.fromEntries(Object.entries(fields).filter(([k,v])=>!same(prior.fields[k],v)));return Object.keys(delta).length?[{id:prior.id,fields:delta}]:[];}));
  for(let i=0;i<creates.length;i+=10){const batch=creates.slice(i,i+10);await pause(500);try{const r=await request(`https://api.airtable.com/v0/${base}/${settings.SHOWS_TABLE}`,{method:'POST',headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({records:batch}),signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('HTTP '+r.status);}catch(error){const saved=await list(settings.SHOWS_TABLE);if(batch.some(r=>saved.filter(s=>s.fields['show-key']===r.fields['show-key']).length!==1))throw Error('Show index creation outcome unresolved: '+error.message);}}
  const rows=await list(settings.SHOWS_TABLE);unique(rows,'show-key');for(const fields of wanted){const saved=rows.find(r=>r.fields['show-key']===fields['show-key']);if(!saved)throw Error('Show index readback missing');for(const[k,v]of Object.entries(fields))if(!same(saved.fields[k],v))throw Error('Show index readback mismatch '+k);}return rows;
 }
 return{async sync(run){
  const [shows,logs,collections,schedules]=await Promise.all([settings.SHOWS_TABLE,'run-logs',settings.SCHEDULE_COLLECTIONS_TABLE,'schedules'].map(list));
  // Verify every referenced retired file and all identities before index writes.
  const plan=planCatalog({logs,shows,collections,schedules,readArchive,verifiedAt:run.fields.completed});
  const saved=await saveShows(plan,shows),byId=new Map(saved.map(r=>[Number(r.fields.show_id),r.id]));
  for(const[table,records]of [['run-logs',logs],[settings.SCHEDULE_COLLECTIONS_TABLE,collections]]){
   const patches=records.flatMap(r=>{const id=byId.get(Number(r.fields['show-id'])),wanted=id?[id]:table==='run-logs'&&r.id===run.id?[]:null;return wanted&&!same(r.fields.shows,wanted)?[{id:r.id,fields:{shows:wanted}}]:[];});await patch(table,patches);
   if(patches.length){const checked=await list(table);for(const p of patches)if(!same(checked.find(r=>r.id===p.id)?.fields.shows,p.fields.shows))throw Error('Show relation readback failed '+table);}
  }
  const linked=(await list('run-logs')).find(r=>r.id===run.id);if(!linked)throw Error('Indexed run-log missing');
  return{...linked,fields:{...linked.fields,shows:linked.fields.shows||[]}};
 }};
}
module.exports={archiveReferences,readVerifiedArchive,planCatalog,showIndex};
