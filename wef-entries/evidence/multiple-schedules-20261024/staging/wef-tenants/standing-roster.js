"use strict";

const {assertUniqueRoster}=require('./identity-policy');
const LEGACY_BASE = 'apptdhhNzduxm5gjn';
const TABLES = {horses:'ww_horses', riders:'ww_riders', tenant:'ww_tenants'};
const TABLE_IDS = {horses:'tblliyUZ1ZS88Kfvl', riders:'tblI6yrqv0Y4QTvyi'};
const positive = value => Number.isSafeInteger(Number(value)) && Number(value)>0;
const ids = value => String(value??'').split(',').map(v=>v.trim()).filter(Boolean);
const links = value => (value||[]).map(v=>typeof v==='string'?v:v.id);
const text = value => value==null?'':String(value).trim();
const normalized = value => text(value).normalize('NFKC').replace(/\s+/g,' ').toLocaleLowerCase('en-US');
const blank = value => value==null||value==='';

// Only reads retained roster tables. Never calls an SGL endpoint or changes the source.
async function loadStandingRoster(api,tenant,trainers) {
 const tenantId=Number(tenant.fields['tenant-id']);
 if(!positive(tenantId))throw Error('Invalid standing roster tenant ID');
 const tenants=await api.list(TABLES.tenant,LEGACY_BASE,{filterByFormula:`{tenant_id}=${tenantId}`,fields:['tenant_id','tenant_official_name','tenant_accronym','ww_horses','ww_riders']});
 if(tenants.length!==1)throw Error('Standing source tenant missing or ambiguous');
 const sourceTenant=tenants[0];
 const query=(type,allowed)=>{
  const terms=allowed.map(id=>`{pid}=${id}`);
  if(type==='horses')terms.push(`{tenant_id}=${tenantId}`,...allowed.map(id=>`{trainer_id}=${id}`));
  for(const id of links(sourceTenant.fields[TABLES[type]])){
   if(!/^rec[A-Za-z0-9]+$/.test(id))throw Error('Invalid roster source record reference');
   terms.push(`RECORD_ID()='${id}'`);
  }
  return {filterByFormula:`OR(${terms.join(',')})`};
 };
 const active=trainers.filter(t=>t.fields.active===true);
 const all=active.map(t=>Number(t.fields['trainer-id']));
 const main=active.filter(t=>t.fields.role==='main').map(t=>Number(t.fields['trainer-id']));
 if(all.some(id=>!positive(id))||main.length!==1)throw Error('Invalid roster trainer scope');
 const horses=await api.list(TABLES.horses,LEGACY_BASE,{...query('horses',all),fields:['horse_id','horse','show_name','barn_name','trainer_id','pid','tenant_id','inactive','horse_note']});
 const riders=await api.list(TABLES.riders,LEGACY_BASE,{...query('riders',all),fields:['rider_id','rider_name','name','lf_name','properFirst','properLast','pid','inactive']});
 return {tenant:sourceTenant,horses,riders};
}

function planStandingRoster({source,tenant,trainers,horses=[],riders=[],now}) {
 const tenantId=Number(tenant.fields['tenant-id']);
 if(Number(source.tenant?.fields.tenant_id)!==tenantId)throw Error('Standing source tenant mismatch');
 const plan={horses:[],riders:[],missing_ids:[],source_counts:{horses:source.horses.length,riders:source.riders.length}};
 for(const type of ['horses','riders']){
  const singular=type==='horses'?'horse':'rider',key=singular+'-key',idField=singular+'-id',nameField=singular+'-name';
  const old=type==='horses'?horses:riders,byKey=new Map(old.map(r=>[r.fields[key],{...r.fields}]));
  if(byKey.size!==old.length)throw Error('Duplicate standing helper key');
  const seenIds=new Set(),seenRecords=new Set();
  const memberships=new Set(links(source.tenant.fields[TABLES[type]]));
  for(const record of source[type]){
   const f=record.fields;
   const matching=trainers.filter(t=>t.fields.active===true&&[f.pid,f.trainer_id].some(id=>Number(id)===Number(t.fields['trainer-id'])));
   const member=memberships.has(record.id)||Number(f.tenant_id)===tenantId;
   if(!member&&!matching.length)continue;
   if(seenRecords.has(record.id))throw Error('Duplicate roster source record');seenRecords.add(record.id);
   const rawId=f[singular+'_id'],numeric=positive(rawId)?Number(rawId):null;
   if(numeric&&seenIds.has(numeric))throw Error('Duplicate numeric roster identity');if(numeric)seenIds.add(numeric);
   const candidates=old.filter(r=>(r.fields['source-base']===LEGACY_BASE&&r.fields['source-record']===record.id)||(numeric&&Number(r.fields[idField])===numeric));
   if(candidates.length>1)throw Error('Ambiguous numeric/source roster identity; inspect without merging');
   const prior=candidates[0]?.fields||{};
   if(numeric&&positive(prior[idField])&&Number(prior[idField])!==numeric)throw Error('Source roster ID changed for retained identity');
   const sourceName=text(type==='horses'?f.horse:(f.rider_name||f.name));
   if(!sourceName)throw Error('Standing roster name missing');
   const trainerIds=[...new Set([...ids(prior['trainer-ids']),...matching.map(t=>String(t.fields['trainer-id'])),...[f.trainer_id,f.pid].filter(positive).map(String)])].sort((a,b)=>Number(a)-Number(b));
   const row={...prior,[key]:prior[key]||String(numeric||'legacy:'+record.id),...(numeric?{[idField]:numeric}:{}),
    'tenant-id':tenantId,'trainer-ids':trainerIds.join(','),'trainer-id':Number(prior['trainer-id']||f.trainer_id||f.pid||matching[0]?.fields['trainer-id'])||null,
    trainers:[...new Set([...(prior.trainers||[]),...matching.map(t=>t.id)])],
    'trainer-name':trainers.filter(t=>trainerIds.includes(String(t.fields['trainer-id']))).map(t=>t.fields['trainer-name']).filter(Boolean).join(', '),
    'source-base':LEGACY_BASE,'source-table':TABLES[type],'source-record':record.id,
    'source-link':`https://airtable.com/${LEGACY_BASE}/${TABLE_IDS[type]}/${record.id}`,
    'source-name':sourceName,'source-inactive':f.inactive===true,'source-id-missing':!numeric,
    'helper-refreshed':now,'first-seen':prior['first-seen']||now,source:'Retained CWF standing roster; not current-show attendance'};
   // Static helper fields are tenant-owned after first fill. Source refreshes do
   // not overwrite nicknames, aliases, notes, name corrections or disabling.
   const defaults={[nameField]:sourceName,name:sourceName,...(type==='horses'?{'show-name':text(f.show_name||f.horse)}:{}),
    'nick-name':type==='horses'?text(f.barn_name):'',aliases:type==='riders'?text(f.lf_name):'',notes:type==='horses'?text(f.horse_note):''};
   if(type==='riders'){
    delete row['show-name'];
    const comma=text(f.lf_name).split(',');
    defaults['first-name']=text(f.properFirst||(comma.length===2?comma[1]:''));
    defaults['last-name']=text(f.properLast||(comma.length===2?comma[0]:''));
   }
   if(!prior['source-record'])for(const [field,value]of Object.entries(defaults))if(blank(row[field])&&value)row[field]=value;
   if(!candidates.length)row.disabled=f.inactive===true;
   if(!numeric)plan.missing_ids.push({type,key:row[key],source_record:record.id,name:sourceName});
   byKey.set(row[key],row);
  }
  plan[type]=[...byKey.values()];
 }
 for(const type of ['horses','riders'])assertUniqueRoster(type,plan[type]);
 return plan;
}

function lookupHelpers(records,criteria={}) {
 const allowed=new Set(['name','nick-name','show-name','first-name','last-name','trainer-id','tenant-id','horse-id','rider-id','horse-key','rider-key','horse-name','rider-name']);
 for(const key of Object.keys(criteria))if(!allowed.has(key))throw Error('Unsupported roster lookup '+key);
 for(const type of ['horses','riders']){const key=type==='horses'?'horse-key':'rider-key';const subset=records.filter(r=>Object.hasOwn(r.fields,key));if(subset.length)assertUniqueRoster(type,subset.map(r=>r.fields));}
 if(Object.hasOwn(criteria,'show-name')&&records.some(r=>Object.hasOwn(r.fields,'rider-key')))throw Error('Riders do not use show-name');
 return records.filter(record=>Object.entries(criteria).every(([key,wanted])=>{
  const f=record.fields;
  if(key==='trainer-id')return ids(f['trainer-ids']).includes(String(wanted))||String(f[key])===String(wanted);
  if(key.endsWith('-key'))return f[key]===wanted;
  if(key.endsWith('-id'))return positive(wanted)&&Number(f[key])===Number(wanted);
  const values=key==='name'?[f.name,f['horse-name'],f['rider-name'],...(Object.hasOwn(f,'horse-key')?[f['show-name']]:[]),...text(f.aliases).split(/\n|;/)]:[f[key]];
  return values.some(v=>!blank(v)&&normalized(v)===normalized(wanted));
 }));
}
module.exports={LEGACY_BASE,loadStandingRoster,planStandingRoster,lookupHelpers};
