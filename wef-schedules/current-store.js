"use strict";
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const settings=require('./settings'),legacy=require('./collector-core');
const COLUMNS={
 'show-id':'show_id','date':'show_date','ring':'ring_number','group-id':'class_group_id',
 'group':'group_name','sequence':'class_group_sequence','class-id':'class_id',
 'class-number':'class_number','class-name':'class_name','type':'class_type',
 'sequence-type':'schedule_sequencetype','start-time':'estimated_start_time','end-time':'estimated_end_time',
 'total-trips':'total_trips','completed-trips':'completed_trips'
};
const NUMBERS=new Set(['show-id','ring','group-id','sequence','class-id','class-number','total-trips','completed-trips']);
function columns(row,show,date,dataset,changedAt) {
 const f={'stage-key':`${show}|${date}|${row.key}`,'dataset-id':dataset,'row-json':JSON.stringify(row)};
 for(const [name,source] of Object.entries(COLUMNS)) {
  const v=name==='show-id'?show:name==='date'?date:row.fields[source];
  if(v===undefined||v===null||v==='') f[name]=null;
  else if(NUMBERS.has(name)){if(!Number.isFinite(Number(v)))throw Error('Invalid numeric source '+source);f[name]=Number(v);}
  else f[name]=String(v);
 }
 f.status=row.inactive?'dropped':'active';f['changed-at']=changedAt;
 return f;
}
function currentStore({baseId=settings.SCHEDULE_STAGING_BASE_ID,collections='collections',schedules='schedules',api=legacy,
 historyRoot=path.join(path.dirname(settings.SCHEDULE_FOCUS_FILE),'history'),removeRecords}={}) {
 if(baseId!==settings.SCHEDULE_STAGING_BASE_ID) throw Error('Current store is restricted to the independent schedules base');
 const list=(table,q={})=>api.airtableList(table,q,baseId);
 const checked=r=>{if(r.failedRows?.length)throw Error('Required staging write failed: '+JSON.stringify(r.failedRows));};
 const archive=(name,value)=>{
  const file=path.join(historyRoot,name+'.json.gz');fs.mkdirSync(path.dirname(file),{recursive:true});
  const body=Buffer.from(JSON.stringify(value));
  if(fs.existsSync(file)) {if(!zlib.gunzipSync(fs.readFileSync(file)).equals(body))throw Error('History collision '+name);return;}
  const tmp=file+'.tmp';fs.writeFileSync(tmp,zlib.gzipSync(body));
  if(!zlib.gunzipSync(fs.readFileSync(tmp)).equals(body))throw Error('History verification failed');fs.renameSync(tmp,file);
 };
 const read=name=>JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(historyRoot,name+'.json.gz'))));
 const safe=s=>{if(!/^[A-Za-z0-9_-]+$/.test(String(s)))throw Error('Invalid history identifier');return String(s);};
 const checkpoint=path.join(historyRoot,'current.json');let state={},ready=false;
 function save(){fs.mkdirSync(historyRoot,{recursive:true});const data=JSON.stringify(state);fs.writeFileSync(checkpoint+'.tmp',data);fs.renameSync(checkpoint+'.tmp',checkpoint);if(fs.readFileSync(checkpoint,'utf8')!==data)throw Error('Checkpoint verification failed');}
 async function remove(table,ids){
  if(!ids.length)return;
  if(removeRecords)return removeRecords(table,ids,baseId);
  for(let i=0;i<ids.length;i+=10){
   // Pace archived-record cleanup; never retry a failed deletion in a loop.
   await new Promise(resolve=>setTimeout(resolve,500));
   const batch=ids.slice(i,i+10),u=new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
   for(const id of batch)u.searchParams.append('records[]',id);
   const response=await fetch(u,{method:'DELETE',headers:{Authorization:`Bearer ${settings.AIRTABLE_TOKEN}`},signal:AbortSignal.timeout(30000)});
   if(!response.ok)throw Error('Archive cleanup failed: '+response.status);
   const result=await response.json();if(result.records?.length!==batch.length||result.records.some(r=>!r.deleted||!batch.includes(r.id)))throw Error('Archive deletion readback mismatch');
  }
 }
 async function init(){
  if(ready)return;
  if(fs.existsSync(checkpoint)){state=JSON.parse(fs.readFileSync(checkpoint));ready=true;return;}
  const attempts=await list(collections),records=await list(schedules);
  archive('initial-airtable', {collections:attempts,schedules:records});
  const datasets=new Map();for(const r of records){const id=r.fields['dataset-id'];if(!datasets.has(id))datasets.set(id,[]);datasets.get(id).push(JSON.parse(r.fields['row-json']));}
  for(const [id,rows] of datasets)archive('datasets/'+safe(id),rows);
  for(const r of attempts.filter(r=>r.fields.status==='complete')){
   const f=r.fields,key=`${f['show-id']}|${f.date}`;
   if(!state[key]||f.completed>state[key].fields.completed)state[key]=r;
   // Empty datasets have no schedule records.
   if(!datasets.has(f['dataset-id'])){if(f['row-count']!==0)throw Error('Historical dataset missing');archive('datasets/'+safe(f['dataset-id']),[]);}
  }
  for(const r of Object.values(state))require('./store').verifyDataset(r,read('datasets/'+safe(r.fields['dataset-id'])),r.fields['show-id'],r.fields.date);
  save();ready=true;
 }
 async function sync(show,date,dataset,rows){
  const all=await list(schedules);const current=all.filter(r=>{
   if(r.fields['show-id']!==undefined)return Number(r.fields['show-id'])===Number(show)&&r.fields.date===date;
   const row=JSON.parse(r.fields['row-json']);return Number(row.fields.show_id)===Number(show)&&legacy.resolveExistingScheduleShowDate({fields:row.fields})===date;
  });
  const desired=new Map(rows.map(row=>[`${show}|${date}|${row.key}`,row]));if(desired.size!==rows.length)throw Error('Duplicate current schedule key');
  if(all.length-current.length+rows.length>=4900)throw Error('Staging capacity warning: current schedule exceeds 4900 rows; existing data retained');
  const groups=new Map();for(const r of current){const row=JSON.parse(r.fields['row-json']);const key=`${show}|${date}|${row.key}`;if(!groups.has(key))groups.set(key,[]);groups.get(key).push(r);}
  const patches=[],creates=[],keep=new Set();const now=new Date().toISOString();
  for(const [key,row] of desired){const candidates=groups.get(key)||[];const existing=candidates.find(r=>r.fields['stage-key']===key)||candidates.find(r=>r.fields['dataset-id']===dataset)||candidates[0];
   const f=columns(row,show,date,dataset,existing?.fields['row-json']===JSON.stringify(row)&&existing.fields['changed-at']?existing.fields['changed-at']:now);
   if(existing){keep.add(existing.id);const delta=Object.fromEntries(Object.entries(f).filter(([k,v])=>JSON.stringify(existing.fields[k]??null)!==JSON.stringify(v)));if(Object.keys(delta).length)patches.push({id:existing.id,fields:delta});}
   else creates.push({fields:f});
  }
  if(patches.length)checked(await api.airtablePatchRecords(schedules,patches,baseId));
  if(creates.length)checked(await api.airtableCreateRecords(schedules,creates,baseId));
  const saved=await list(schedules);for(const [key,row] of desired){const matches=saved.filter(r=>r.fields['stage-key']===key);if(matches.length!==1)throw Error('Current schedule key readback mismatch');const expected=columns(row,show,date,dataset,matches[0].fields['changed-at']);for(const [k,v] of Object.entries(expected))if(JSON.stringify(matches[0].fields[k]??null)!==JSON.stringify(v))throw Error('Current field readback mismatch '+k);}
  const extras=current.filter(r=>!keep.has(r.id));if(extras.length){archive('removed/'+crypto.randomUUID(),extras);await remove(schedules,extras.map(r=>r.id));}
 }
 return {
  async begin(fields){await init();return {fields:{...fields}};},
  async latest(show,date){await init();return state[`${show}|${date}`]||null;},
  async writeRows(dataset,show,date,rows){archive('datasets/'+safe(dataset),rows);},
  async loadRows(dataset){return read('datasets/'+safe(dataset));},
  async finish(record,fields){
   const merged={...record.fields,...fields},key=`${merged['show-id']}|${merged.date}`;
   archive('attempts/'+safe(merged['collection-id'])+'-'+safe(fields.status),merged);
   if(fields.status==='complete')await sync(merged['show-id'],merged.date,merged['dataset-id'],read('datasets/'+safe(merged['dataset-id'])));
   const prior=(await list(collections)).filter(r=>Number(r.fields['show-id'])===merged['show-id']&&r.fields.date===merged.date);
   const existing=prior.find(r=>r.id===state[key]?.id)||prior[0];
   const payload={...merged};
   if(existing)checked(await api.airtablePatchRecords(collections,[{id:existing.id,fields:payload}],baseId));else checked(await api.airtableCreateRecords(collections,[{fields:payload}],baseId));
   const saved=(await list(collections)).find(r=>r.fields['collection-id']===merged['collection-id']);if(!saved)throw Error('Collection readback missing');
   for(const [k,v] of Object.entries(payload))if(JSON.stringify(saved.fields[k])!==JSON.stringify(v))throw Error('Collection readback mismatch '+k);
   if(fields.status==='complete'){state[key]=saved;save();}
   const extras=prior.filter(r=>r.id!==saved.id);if(extras.length){archive('removed/'+crypto.randomUUID(),extras);await remove(collections,extras.map(r=>r.id));}
   return saved;
  },
  async finalize(show){
   // Only after every new-show day succeeds. Retire old-show Airtable data,
   // preserving exact records in the separate verified archive first.
   for(const table of [schedules,collections]){const old=(await list(table)).filter(r=>Number(r.fields['show-id'])!==Number(show));if(old.length){archive('retired/'+crypto.randomUUID(),old);await remove(table,old.map(r=>r.id));}}
  }
 };
}
module.exports={currentStore,columns,COLUMNS,NUMBERS};
