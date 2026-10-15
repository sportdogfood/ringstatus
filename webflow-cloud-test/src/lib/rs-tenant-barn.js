import {prepareManagedCredentials} from './rs-recognition-action.js';
// Compatibility boundary for the existing native Barn/Rider editor.
// Astro validates, writes canonical Airtable records and returns verified data.
import { InputError, saveInputRecord, readInputState, digest } from './rs-inputs.js';
import { claimOnce, completeClaim, requireClaimDatabase } from './rs-recognition-claims.js';
import { createAccessStore } from './rs-inputs-access.js';
import {payloadHash,readTenantSave,runTenantSave} from './rs-tenant-save-operation.js';

export const BASE='app7BXcw801n19QKF';
export const TABLE={barn:'tbled1saw01Thffs1',riders:'tbllUlTlcEMgZiW3h',users:'tblyTBgrgdhT3Tcg7',sources:'tblRswgKjA9dt3PED',events:'tbluecPFIorOytASP',submissions:'tblldMTDEC697hPwE',people:'tblyTBgrgdhT3Tcg7',memberships:'tblFf4aWQYA2RRsWo',roles:'tblRqRbjmqmNfJtIP',roster:'tblwcczj8lFhVcbyt',optins:'tblCX61u6pjSLrP5S'};
export const SOURCE='rs-inputs-native';
const fail=(code,status=409)=>{throw new InputError(code,status)};
const quote=v=>String(v).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const selected=['barn','riders','users'];
const DIRECT_FIELDS={ [TABLE.barn]:['tenant_official_name','tenant_logo_url'],[TABLE.riders]:['name','people'],[TABLE.people]:['entity_uid','recognition_person_uid','name','email','status','record_mode','phone_e164','display-name','profile-img','tags'],[TABLE.memberships]:['Name','Person','Tenant','Roles','Status'],[TABLE.roster]:['Name','Tenant','Rider','Status'],[TABLE.sources]:['mapping_uid','entity_type','entity_uid','source_system','source_entity_type','source_scope','external_id','record_mode'],[TABLE.events]:['event_uid','actor_uid','entity_uid','barn_uid','action','kind','outcome','request_uid','input_hash','result_json','occurred_at','record_mode'] };

export function createTenantRest(env,fetchImpl=fetch){
 if(!env.AIRTABLE_TOKEN||env.RS_INPUTS_WRITE_MODE!=='isolated-trial')fail('tenant_configuration_required',503);
 let last=0;const cache=new Map();
 return async function call(table,{id,formula,fields,method='GET',fresh=false}={}){
  if(fresh)cache.clear();
  if(!Object.values(TABLE).includes(table)||!['GET','POST','PATCH'].includes(method))fail('tenant_write_boundary',403);
  if(method!=='GET'){if(!DIRECT_FIELDS[table]||!fields||Object.keys(fields).some(k=>!DIRECT_FIELDS[table].includes(k))||(method==='PATCH'&&!id))fail('tenant_write_boundary',403);for(const key of cache.keys())if(new URL(key).pathname.split('/')[3]===table)cache.delete(key);}
  const url=new URL(`https://api.airtable.com/v0/${BASE}/${table}${id&&method==='GET'?'/'+id:''}`);
  if(formula)url.searchParams.set('filterByFormula',formula);
  const cacheKey=String(url);if(method==='GET'&&cache.has(cacheKey))return structuredClone(cache.get(cacheKey));
  const rows=[],seen=new Set();
  do{
   const pause=Math.max(0,225-(Date.now()-last));if(pause)await new Promise(r=>setTimeout(r,pause));last=Date.now();
   let body;try{const res=await fetchImpl(url,{method,redirect:'manual',signal:AbortSignal.timeout(15000),headers:{Authorization:`Bearer ${env.AIRTABLE_TOKEN}`,'Content-Type':'application/json'},...(fields?{body:JSON.stringify({records:[{...(id?{id}:{}),fields}]})}:{})});if(!res.ok){const error=new InputError(method==='GET'?'storage_unavailable':'write_outcome_unknown',503);error.noWrite=method!=='GET'&&res.status>=400&&res.status<500&&res.status!==408;throw error;}body=await res.json();}catch(error){if(error.noWrite)throw error;fail(method==='GET'?'storage_unavailable':'write_outcome_unknown',503)}
   if(id&&method==='GET'){if(body.id!==id||!body.fields)fail('invalid_storage_response',502);cache.set(cacheKey,body);return structuredClone(body);}
   if(!Array.isArray(body.records))fail('invalid_storage_response',502);rows.push(...body.records);
   if(!body.offset||method!=='GET')break;if(seen.has(body.offset))fail('invalid_storage_response',502);seen.add(body.offset);url.searchParams.set('offset',body.offset);
  }while(true);if(method==='GET')cache.set(cacheKey,rows);return structuredClone(rows);
 };
}
export function createTenantBarnStore({legacy,call,actor}){
 actor.barnIds ||= [];
 const one=async(table,field,value)=>{const rows=await call(table,{formula:`{${field}} = '${quote(value)}'`});if(rows.length>1)fail('ambiguous_tenant_mapping');return rows[0]||null;};
 const mapping=(kind,id)=>one(TABLE.sources,'mapping_uid',`${SOURCE}|${kind}|${id}`);
 async function access(tenantId){
  const person=await one(TABLE.people,'recognition_person_uid',actor.id);if(!person||!['Active','Test'].includes(person.fields.status))return null;
  const memberships=(await call(TABLE.memberships)).filter(r=>r.fields.Person?.length===1&&r.fields.Person[0]===person.id&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===tenantId);
  if(memberships.length>1)fail('ambiguous_tenant_membership');
  const membership=memberships[0];if(membership?.fields.Status!=='Enabled')return null;
  const catalog=await call(TABLE.roles),roles=catalog.filter(r=>membership.fields.Roles?.includes(r.id));
  if(!roles.length)return null;
  return {person,membership,roles,manage:roles.some(r=>r.fields.rec_name==='owner')};
 }
 async function allowed(tenantId){return !!await access(tenantId);}
 async function manages(tenantId){return (await access(tenantId))?.manage===true;}
 async function snapshot(kind,map){
  const m=map.fields;if(m.source_system!==SOURCE||m.source_entity_type!==kind||!/^rec[A-Za-z0-9]+$/.test(m.external_id||''))fail('invalid_tenant_mapping');
  const events=await call(TABLE.events,{formula:`AND({entity_uid} = '${quote(m.entity_uid)}', {kind} = '${kind}', {outcome} = 'committed')`});
  const snapshots=events.map(r=>{let data;try{data=JSON.parse(r.fields.result_json)}catch{fail('invalid_tenant_event')};return {row:r,data};}).filter(x=>x.data.id===m.entity_uid&&x.data.barnId===m.source_scope);
  snapshots.sort((a,b)=>b.data.revision-a.data.revision);const latest=snapshots[0];
  if(!latest||!Number.isSafeInteger(latest.data.revision)||latest.data.revision<1||!latest.data.ownerUid)fail('tenant_confirmation_missing');
  const target=await call(TABLE[kind],{id:m.external_id});
  const name=target.fields[kind==='barn'?'tenant_official_name':'name'];if(typeof name!=='string'||!name.trim())fail('invalid_tenant_record');
  const data={...latest.data,name,requestUid:latest.row.fields.event_uid,requestHash:latest.row.fields.input_hash};
  if(kind==='barn'&&('logo' in data||target.fields.tenant_logo_url))data.logo=target.fields.tenant_logo_url||'';
  if(kind==='users'){
   data.email=target.fields.email||'';data.recognitionPersonId=target.fields.recognition_person_uid||data.recognitionPersonId||'';
   for(const [key,field] of [['displayName','display-name'],['sms','phone_e164'],['profileImg','profile-img'],['tags','tags']])if(key in data||target.fields[field])data[key]=target.fields[field]||'';
   if('roles' in data||'membershipStatus' in data){const tenantId=(await mapping('barn',m.source_scope))?.fields.external_id;const membership=(await call(TABLE.memberships)).find(r=>r.fields.Person?.includes(target.id)&&r.fields.Tenant?.includes(tenantId));data.roles=(membership?.fields.Roles||[]).sort().join(',');data.membershipStatus=membership?.fields.Status||'Disabled';}
  }
  delete data.pin;return data;
 }
 const api={
  ...legacy,profileFields:true,one,mapping,allowed,manages,access,snapshot,
  async list(kind,barnId=''){
   if(!selected.includes(kind)){const rows=await legacy.list(kind),visible=[];for(const r of rows){const bm=await mapping('barn',r.barnId);if(bm?await manages(bm.fields.external_id):r.ownerUid===actor.id)visible.push(r);}return visible;}
   const inScope=r=>kind!=='users'||!barnId||r.barnId===barnId;
   const original=(await legacy.list(kind)).filter(inScope),maps=(await call(TABLE.sources,{formula:`AND({source_system} = '${SOURCE}', {source_entity_type} = '${kind}')`})).filter(m=>kind!=='users'||!barnId||m.fields.source_scope===barnId);
   const byId=new Map(original.map(r=>[r.id,r]));const seen=new Set();
   for(const map of maps){const id=map.fields.entity_uid;if(seen.has(id))fail('ambiguous_tenant_mapping');seen.add(id);
    const barnMap=kind==='barn'?map:await mapping('barn',map.fields.source_scope);
    // A mapped record never falls back to legacy ownership after disable/revocation.
    byId.delete(id);if(!barnMap||!await allowed(barnMap.fields.external_id))continue;
    if(kind==='riders'){const roster=await call(TABLE.roster);if(!roster.some(r=>r.fields.Status==='Enabled'&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===barnMap.fields.external_id&&r.fields.Rider?.length===1&&r.fields.Rider[0]===map.fields.external_id&&!r.fields.Horse?.length))continue;}
    if(kind==='users'&&!await manages(barnMap.fields.external_id)){const person=await one(TABLE.people,'recognition_person_uid',actor.id);if(map.fields.external_id!==person?.id)continue;}
    if(kind==='riders'&&!await manages(barnMap.fields.external_id))continue;
    if(kind==='barn'&&!actor.barnIds.includes(id))actor.barnIds.push(id);
    if(kind==='users'){const memberships=await call(TABLE.memberships),manage=await manages(barnMap.fields.external_id);if(!memberships.some(r=>(manage||r.fields.Status==='Enabled')&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===barnMap.fields.external_id&&r.fields.Person?.length===1&&r.fields.Person[0]===map.fields.external_id))continue;}
    byId.set(id,await snapshot(kind,map));
   }
   if(kind==='users'){
    // Prepopulated canonical People do not need a prior form-save event to appear.
    const barns=await call(TABLE.sources,{formula:`AND({source_system} = '${SOURCE}', {source_entity_type} = 'barn')`});
    for(const bm of barns){if(barnId&&bm.fields.entity_uid!==barnId)continue;const granted=await access(bm.fields.external_id);if(!granted)continue;
     const memberships=(await call(TABLE.memberships)).filter(m=>m.fields.Tenant?.length===1&&m.fields.Tenant[0]===bm.fields.external_id&&m.fields.Person?.length===1);
     for(const m of memberships){if(memberships.filter(other=>other.fields.Person[0]===m.fields.Person[0]).length!==1)fail('ambiguous_tenant_membership');
      if(!granted.manage&&(m.fields.Person[0]!==granted.person.id||m.fields.Status!=='Enabled'))continue;
      if(maps.some(map=>map.fields.external_id===m.fields.Person[0]&&map.fields.source_scope===bm.fields.entity_uid))continue;
      const p=await call(TABLE.people,{id:m.fields.Person[0]}),f=p.fields;if(!f.entity_uid||!f.name||!['Active','Test'].includes(f.status))continue;
      if(byId.has(f.entity_uid))fail('ambiguous_tenant_mapping');
      byId.set(f.entity_uid,{id:f.entity_uid,barnId:bm.fields.entity_uid,name:f.name,email:f.email||'',displayName:f['display-name']||'',sms:f.phone_e164||'',profileImg:f['profile-img']||'',tags:f.tags||'',roles:(m.fields.Roles||[]).slice().sort().join(','),membershipStatus:m.fields.Status||'Disabled',recognitionPersonId:f.recognition_person_uid||'',revision:0,ownerUid:actor.id});
     }
    }
   }
   const visible=[];for(const r of byId.values()){const bm=await mapping('barn',r.barnId);if(!bm){if(r.ownerUid===actor.id)visible.push(r);continue;}if(await manages(bm.fields.external_id)||kind==='barn'&&await allowed(bm.fields.external_id)||kind==='users'&&r.recognitionPersonId===actor.id&&await allowed(bm.fields.external_id))visible.push(r);}return visible;
  },
  async put(kind,record,options){if(selected.includes(kind))fail('tenant_submission_required');const bm=await mapping('barn',record.barnId);if(!bm||!await manages(bm.fields.external_id))fail('permission_denied',403);return legacy.put(kind,record,options);},
  async event(id){const row=await one(TABLE.events,'event_uid',id);if(!row)return legacy.event(id);let record;try{record=JSON.parse(row.fields.result_json)}catch{fail('invalid_tenant_event')};return {inputHash:row.fields.input_hash,record};},
 };
 return api;
}
export function createTenantBarnBridge({env,legacy,actor,fetchImpl=fetch,call=createTenantRest(env,fetchImpl),clock=()=>new Date().toISOString(),includeState=true}){
 const baseStore=createTenantBarnStore({legacy,call,actor});
 let profileTarget=null,activeOperation=null;
 const store={...baseStore,async list(kind,barnId=''){
  const rows=await baseStore.list(kind,barnId);
  if(kind!=='users'||!profileTarget||barnId&&profileTarget.record.barnId!==barnId||await baseStore.mapping('users',profileTarget.record.id))return rows;
  return [...rows.filter(r=>r.id!==profileTarget.record.id&&!(r.barnId===profileTarget.record.barnId&&r.recognitionPersonId===actor.id)),profileTarget.record];
 }};
 async function prepareProfileLink(barnId){
  if(actor.profile?.personUid!==actor.id||!actor.profile?.name)fail('verified_profile_required',403);
  const barnMap=await store.mapping('barn',barnId);
  if(!barnMap||!await store.allowed(barnMap.fields.external_id))fail('tenant_membership_required',403);
  const person=await store.one(TABLE.people,'recognition_person_uid',actor.id);
  if(!person||!person.fields.entity_uid||!['Active','Test'].includes(person.fields.status))fail('tenant_membership_required',403);
  const maps=(await call(TABLE.sources,{formula:`AND({source_system} = '${SOURCE}', {source_entity_type} = 'users')`})).filter(m=>m.fields.external_id===person.id&&m.fields.source_scope===barnId);
  if(maps.length>1)fail('ambiguous_profile_link');
  const rows=await baseStore.list('users',barnId);
  const matches=rows.filter(r=>r.barnId===barnId&&r.recognitionPersonId===actor.id);
  if(matches.length>1)fail('ambiguous_profile_link');
  const existing=maps.length?await baseStore.snapshot('users',maps[0]):matches[0];
  const id=existing?.id||person.fields.entity_uid;
  const map=await store.mapping('users',id);
  if(map&&(map.fields.external_id!==person.id||map.fields.source_scope!==barnId))fail('ambiguous_profile_link');
  if(rows.some(r=>r.id===id&&r.barnId!==barnId))fail('ambiguous_profile_link');
  const identity=await createAccessStore({env,fetchImpl}).byUid(actor.id);
  const ownAccess=await store.access(barnMap.fields.external_id);
  profileTarget={personId:person.id,record:{...existing,id,barnId,name:person.fields.name,email:person.fields.email||'',displayName:person.fields['display-name']||'',profileImg:person.fields['profile-img']||'',tags:person.fields.tags||'',sms:identity?.fields.primary_phone_e164||person.fields.phone_e164||'',roles:(ownAccess.membership.fields.Roles||[]).slice().sort().join(','),membershipStatus:ownAccess.membership.fields.Status,recognitionPersonId:actor.id,revision:existing?.revision||0,ownerUid:actor.id}};
 }
 const key=()=>{const secret=env.RS_INPUTS_SESSION_SECRET;if(!/^[a-fA-F0-9]{64}$/.test(secret||''))fail('access_secret_missing',503);return crypto.subtle.importKey('raw',Uint8Array.from(secret.match(/../g),x=>parseInt(x,16)),{name:'HMAC',hash:'SHA-256'},false,['sign','verify']);};
 const proof=e=>new TextEncoder().encode('tenant-barn-v1|'+JSON.stringify(e));
 async function submission(id){return store.one(TABLE.submissions,'Submission ID',id);}
 async function envelope(row){let parsed;try{parsed=JSON.parse(row.fields['Submitted changes'])}catch{fail('invalid_submission',403)};const {signature:sig,...e}=parsed;
  if(e.version!==1||e.kind!=='barn-rider'||!selected.includes(e.entityKind)||!/^\w[\w-]{7,127}$/.test(e.payload?.requestId||'')||!/^([a-f0-9]{64})$/.test(sig||'')||!await crypto.subtle.verify('HMAC',await key(),Uint8Array.from(sig.match(/../g),v=>parseInt(v,16)),proof(e)))fail('invalid_submission',403);
  if(row.fields['Submission ID']!==e.event.eventId||row.fields['Actor ID']!==e.actor.id||row.fields['Target table ID']!==TABLE[e.entityKind]||(row.fields['Processing status']!=='Applied'&&(row.fields['Target record ID']||'')!==(e.targetId||'')))fail('submission_scope_mismatch',403);
  return {e,sig};
 }
 async function currentActor(e){
  const person=await createAccessStore({env,fetchImpl}).byUid(e.actor.id);
  if(!person||!['invited','approved'].includes(person.fields.input_access)||!['active','test'].includes(String(person.fields.status).toLowerCase()))fail('permission_denied',403);
 }
 async function confirmed(row,e){
  if(row.fields['Processing status']!=='Applied')return null;
  const map=await store.mapping(e.entityKind,e.record.id);if(!map||row.fields['Target record ID']!==map.fields.external_id)fail('tenant_confirmation_missing');
  const event=await store.one(TABLE.events,'event_uid',e.event.eventId);let record;try{record=JSON.parse(event?.fields.result_json)}catch{fail('tenant_confirmation_missing')};
  if(event.fields.input_hash!==e.event.inputHash||!same(record,e.record))fail('tenant_confirmation_mismatch');
  await requireRecordAccess(e.entityKind,record);
  await store.snapshot(e.entityKind,map);
  const barn=await store.mapping('barn',e.record.barnId);if(!barn||!await store.allowed(barn.fields.external_id))fail('permission_denied',403);
  const {ownerUid,requestUid,requestHash,...publicRecord}=record;
  return {ok:true,record:publicRecord,state:await readInputState(store,actor,e.record.barnId)};
 }
 const equalField=(actual,expected)=>same(actual??(Array.isArray(expected)?[]:expected===''?'':null),expected??null);
 const publicRecord=({ownerUid,requestUid,requestHash,pin,...value})=>value;
 async function requireRecordAccess(kind,record){
  const bm=await store.mapping('barn',record.barnId);const access=bm?await store.access(bm.fields.external_id):null;
  if(!access)fail('permission_denied',403);
  if(!access.manage){const target=kind==='users'?await store.mapping('users',record.id):null;if(!target||target.fields.external_id!==access.person.id)fail('permission_denied',403);}
 }
 async function directResult(record,{verifyCurrent=false,kind=record.id===record.barnId?'barn':'riders'}={}){
  await requireRecordAccess(kind,record);
  const barn=await store.mapping('barn',record.barnId);
  if(!barn||!await store.allowed(barn.fields.external_id))fail('permission_denied',403);
  if(!includeState){
   const map=await store.mapping(kind,record.id);if(!map)fail('tenant_confirmation_missing');
   const latest=await store.snapshot(kind,map);
   if(kind==='riders'){
    const roster=await call(TABLE.roster);
    if(!roster.some(r=>r.fields.Status==='Enabled'&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===barn.fields.external_id&&r.fields.Rider?.length===1&&r.fields.Rider[0]===map.fields.external_id&&!r.fields.Horse?.length))fail('tenant_confirmation_missing');
   }
   if(kind==='users'){const memberships=await call(TABLE.memberships);if(!memberships.some(m=>m.fields.Status===(record.membershipStatus||'Enabled')&&m.fields.Person?.length===1&&m.fields.Person[0]===map.fields.external_id&&m.fields.Tenant?.length===1&&m.fields.Tenant[0]===barn.fields.external_id))fail('tenant_confirmation_missing');}
   if(verifyCurrent&&!same(publicRecord(latest),publicRecord(record)))fail('tenant_confirmation_missing');
   return {ok:true,record:publicRecord(latest)};
  }
  const state=await readInputState(store,actor,record.barnId);
  if(verifyCurrent){const rows=kind==='barn'?state.barns:state[kind];if(!rows.some(r=>same(r,publicRecord(record))))fail('tenant_confirmation_missing');}
  return {ok:true,record:publicRecord(record),state};
 }
 async function write(table,fields,id){
  if(activeOperation){
   const identity=id||fields.mapping_uid||fields.event_uid||fields.entity_uid||JSON.stringify([fields.Person,fields.Tenant,fields.Rider]);
   return activeOperation.step(`${table}:${identity}`,async confirmedId=>{
    let row;if(id||confirmedId)row=await call(table,{id:id||confirmedId,fresh:true});
    else{
     const key=['mapping_uid','event_uid','entity_uid'].find(k=>fields[k]);
     if(!key&&!fields.Person&&!fields.Tenant&&!fields.Rider)return null;
     const rows=await call(table,{fresh:true,...(key?{formula:`{${key}} = '${quote(fields[key])}'`}:{})});
     const matches=key?rows:rows.filter(r=>['Person','Tenant','Rider'].every(k=>!fields[k]||same(r.fields[k],fields[k])));
     if(matches.length>1)fail('ambiguous_tenant_mapping');row=matches[0];
    }
    return row&&Object.entries(fields).every(([k,v])=>equalField(row.fields[k],v))?row:null;
   },async()=>{const rows=await call(table,{method:id?'PATCH':'POST',...(id?{id}:{}),fields});if(rows.length!==1)fail('write_outcome_unknown',503);return rows[0];});
  }
  const rows=await call(table,{method:id?'PATCH':'POST',...(id?{id}:{}),fields});
  if(rows.length!==1||!/^rec[A-Za-z0-9]+$/.test(rows[0]?.id||''))fail('write_outcome_unknown',503);
  const row=await call(table,{id:rows[0].id});
  for(const [k,v] of Object.entries(fields))if(!equalField(row.fields[k],v))fail('target_readback_mismatch');
  return row;
 }
 async function member(tenantId,allowCreate){
  const owners=(await call(TABLE.roles)).filter(r=>r.fields.rec_name==='owner');if(owners.length!==1)fail('tenant_owner_role_required');
  let person=await store.one(TABLE.people,'recognition_person_uid',actor.id);
  if(!person&&allowCreate)person=await write(TABLE.people,{entity_uid:`recognize_${actor.id}`,recognition_person_uid:actor.id,name:actor.profile.name,email:actor.profile.email||'',status:'Active',record_mode:'Test'});
  if(!person)fail('tenant_membership_required',403);
  const rows=(await call(TABLE.memberships)).filter(r=>r.fields.Person?.includes(person.id)&&r.fields.Tenant?.includes(tenantId));if(rows.length>1)fail('ambiguous_tenant_mapping');
  if(rows.length){const m=rows[0];if(m.fields.Person.length!==1||m.fields.Tenant.length!==1||m.fields.Status!=='Enabled'||!m.fields.Roles?.includes(owners[0].id))fail('tenant_membership_required',403);return m;}
  if(!allowCreate)fail('tenant_membership_required',403);
  return write(TABLE.memberships,{Name:`${actor.profile.name} · owner`,Person:[person.id],Tenant:[tenantId],Roles:[owners[0].id],Status:'Enabled'});
 }
 async function preflightDirect(e,{fresh=false}={}){
  await call(TABLE.roles,{fresh});
  const r=e.record,kind=e.entityKind;
  const map=await store.mapping(kind,r.id),canonical=kind==='users'&&!map?await store.one(TABLE.people,'entity_uid',r.id):null;if((map?.fields.external_id||e.profilePersonId||canonical?.id||'')!==e.targetId)fail('mapping_changed');
  if(e.profilePersonId){const person=await store.one(TABLE.people,'recognition_person_uid',actor.id);if(person?.id!==e.profilePersonId||r.recognitionPersonId!==actor.id)fail('profile_identity_changed',403);}
  const now=(await store.list(kind,r.barnId)).find(x=>x.id===r.id)||null;if(!same(now,e.previous))fail('record_changed');
  let tenantId=kind==='barn'?e.targetId:(await store.mapping('barn',r.barnId))?.fields.external_id;
  if(kind!=='barn'&&!tenantId)fail('save_barn_first');
  const access=tenantId?await store.access(tenantId):null;
  if(tenantId&&(!access||(!access.manage&&(kind!=='users'||e.targetId!==access.person.id))))fail('permission_denied',403);
  const nameField=kind==='barn'?'tenant_official_name':'name';
  if(e.targetId){const old=await call(TABLE[kind],{id:e.targetId});if(old.fields[nameField]!==e.previous?.name)fail('record_changed');}
  else if((await call(TABLE[kind])).some(x=>[r.name,e.previous?.name].filter(Boolean).some(n=>String(x.fields[nameField]||'').trim().toLowerCase()===n.trim().toLowerCase())))fail('existing_record_mapping_required');
  let roster=null;
  if(kind==='riders'&&e.targetId){const rows=(await call(TABLE.roster)).filter(x=>x.fields.Tenant?.includes(tenantId)&&x.fields.Rider?.includes(e.targetId));if(rows.length!==1)fail('roster_not_enabled');roster=rows[0];if(roster.fields.Status!=='Enabled'||roster.fields.Horse?.length||roster.fields.Tenant.length!==1||roster.fields.Rider.length!==1)fail('roster_not_enabled');}
  let userMembership=null;
  if(kind==='users'&&e.targetId){const rows=(await call(TABLE.memberships)).filter(x=>x.fields.Tenant?.includes(tenantId)&&x.fields.Person?.includes(e.targetId));if(rows.length!==1||(!access?.manage&&rows[0].fields.Status!=='Enabled')||rows[0].fields.Tenant.length!==1||rows[0].fields.Person.length!==1)fail('tenant_membership_required',403);userMembership=rows[0];}
  const fields={[nameField]:r.name};
  let credentials=null,roleIds=null;
  if(kind==='users'){
   fields.email=r.email||'';if(e.profilePersonId)fields.recognition_person_uid=actor.id;if(!e.targetId)Object.assign(fields,{entity_uid:r.id,status:'Active',record_mode:'Test'});
   for(const [key,field]of [['displayName','display-name'],['sms','phone_e164'],['profileImg','profile-img'],['tags','tags']])if(key in r)fields[field]=r[key];
   if(!r.roles&&!userMembership){const defaults=(await call(TABLE.roles)).filter(x=>x.fields.rec_name==='user');if(defaults.length!==1)fail('tenant_user_role_required');r.roles=defaults[0].id;}
   r.membershipStatus=r.membershipStatus||userMembership?.fields.Status||'Enabled';
   if(!access?.manage&&r.membershipStatus!==(userMembership?.fields.Status||'Enabled'))fail('permission_denied',403);
   if(e.targetId===access?.person.id&&access.manage&&r.membershipStatus!=='Enabled')fail('owner_membership_required',403);
   if('roles' in r){roleIds=r.roles?r.roles.split(','):[];const catalog=await call(TABLE.roles);
    if(roleIds.some(id=>!catalog.some(x=>x.id===id&&!['admin','dev','developer'].includes(x.fields.rec_name))))fail('invalid_role',400);
    const existing=(userMembership?.fields.Roles||[]).slice().sort();
    if(!access?.manage&&!same(roleIds,existing))fail('permission_denied',403);
    if(e.targetId===access?.person.id&&access.manage&&!roleIds.some(id=>catalog.find(x=>x.id===id)?.fields.rec_name==='owner'))fail('owner_role_required',403);
   }
   if(r.pin&&!r.sms)fail('phone_required',400);
   const credentialChange=!e.previous?.recognitionPersonId||r.name!==e.previous.name||(r.email||'')!==(e.previous.email||'')||r.sms!==e.previous.sms||!!r.pin;
   if(r.sms&&credentialChange){
    const personUid=r.recognitionPersonId||`tenant_${r.id}`;
    credentials=await prepareManagedCredentials({env,fetchImpl,personUid,name:r.name,email:r.email,sms:r.sms,pin:r.pin});
    fields.recognition_person_uid=personUid;fields.phone_e164=credentials.phone;
   }
  }
  if(kind==='barn'&&'logo' in r)fields.tenant_logo_url=r.logo;
  if(kind==='riders'){
   fields.people=[];
   if(r.userId){const rows=(await call(TABLE.people)).filter(x=>x.fields.entity_uid===r.userId||(e.linkedUser?.recognitionPersonId&&x.fields.recognition_person_uid===e.linkedUser.recognitionPersonId));if(rows.length!==1)fail('person_mapping_required');fields.people=[rows[0].id];}
  }
  if(kind==='barn'&&!e.targetId){
   const owners=(await call(TABLE.roles)).filter(x=>x.fields.rec_name==='owner');if(owners.length!==1)fail('tenant_owner_role_required');
   await store.one(TABLE.people,'recognition_person_uid',actor.id);
  }
  await currentActor(e);
  return {map,tenantId,roster,userMembership,fields,credentials,roleIds};
 }
 async function applyDirect(e,prepared){
  // Recheck race-sensitive permissions, mapping and target after reservation.
  const {map,roster,userMembership,fields,credentials,roleIds,tenantId:existingTenant}=prepared||await preflightDirect(e);
  const r=e.record,kind=e.entityKind;let tenantId=existingTenant;
  if(credentials){
   const phoneClaim=await claimOnce(env,'tenant-recognition-phone',credentials.phone);
   if(!phoneClaim.won&&phoneClaim.result?.personUid!==credentials.personUid)fail('phone_already_registered',409);
   if(phoneClaim.won)await completeClaim(env,phoneClaim,{personUid:credentials.personUid});
   r.recognitionPersonId=await credentials.apply(activeOperation);r.sms=credentials.phone;
  }
  delete r.pin;
  const target=await write(TABLE[kind],fields,e.targetId||undefined);
  if(kind==='barn')tenantId=target.id;
  if(kind==='barn'&&!e.targetId)await member(tenantId,true);
  if(kind==='riders'&&!roster)await write(TABLE.roster,{Name:r.name,Tenant:[tenantId],Rider:[target.id],Status:'Enabled'});
  if(kind==='users'){if(!userMembership)await write(TABLE.memberships,{Name:r.name,Person:[target.id],Tenant:[tenantId],Status:r.membershipStatus,...(roleIds?{Roles:roleIds}:{})});else if(userMembership.fields.Status!==r.membershipStatus||roleIds&&!same(roleIds,(userMembership.fields.Roles||[]).slice().sort()))await write(TABLE.memberships,{Status:r.membershipStatus,...(roleIds?{Roles:roleIds}:{})},userMembership.id);}
  if(!map)await write(TABLE.sources,{mapping_uid:`${SOURCE}|${kind}|${r.id}`,entity_type:kind==='barn'?'Barn':kind==='users'?'Person':'Rider',entity_uid:r.id,source_system:SOURCE,source_entity_type:kind,source_scope:r.barnId,external_id:target.id,record_mode:'Test'});
  await write(TABLE.events,{event_uid:e.event.eventId,actor_uid:actor.id,entity_uid:r.id,barn_uid:r.barnId,action:e.event.action,kind,outcome:'committed',request_uid:e.payload.requestId,input_hash:e.event.inputHash,result_json:JSON.stringify(r),occurred_at:e.acceptedAt,record_mode:'Test'});
  // Verify the same target again after related records/audit, never a cached value.
  const verified=await call(TABLE[kind],{id:target.id,fresh:true});for(const [k,v]of Object.entries(fields))if(!equalField(verified.fields[k],v))fail('target_readback_mismatch');
  await currentActor(e);
  // WEC action-result pattern: return the confirmed edit, not a rebuilt snapshot.
  // The fresh target read above invalidated the cache; access is checked again.
  await requireRecordAccess(kind,r);
  if(kind==='users'){
   const memberships=await call(TABLE.memberships);
   const matches=memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===target.id&&m.fields.Tenant?.length===1&&m.fields.Tenant[0]===tenantId);
   if(matches.length!==1||matches[0].fields.Status!==r.membershipStatus||!same((matches[0].fields.Roles||[]).slice().sort().join(','),r.roles||''))fail('tenant_confirmation_missing');
  }
  if(kind==='riders'){
   const roster=await call(TABLE.roster);
   if(!roster.some(m=>m.fields.Status==='Enabled'&&m.fields.Rider?.length===1&&m.fields.Rider[0]===target.id&&m.fields.Tenant?.length===1&&m.fields.Tenant[0]===tenantId&&!m.fields.Horse?.length))fail('tenant_confirmation_missing');
  }
  return {ok:true,record:publicRecord(r),...(includeState?{state:await readInputState(store,actor,r.barnId)}:{})};
 }
 return {store,
  async state(barnId=''){
   const state=await readInputState(store,actor,barnId);
   const map=state.selectedBarn?await store.mapping('barn',state.selectedBarn):null;
   state.canManage=!!map&&await store.manages(map.fields.external_id);
   state.roleOptions=(await call(TABLE.roles)).filter(r=>!['admin','dev','developer'].includes(r.fields.rec_name)).map(r=>({id:r.id,name:r.fields.rec_name}));
   for(const user of state.users){const m=await store.mapping('users',user.id);if(m){const membership=(await call(TABLE.memberships)).find(r=>r.fields.Person?.includes(m.fields.external_id)&&r.fields.Tenant?.includes(map?.fields.external_id));user.roles=(membership?.fields.Roles||[]).slice().sort().join(',');user.membershipStatus=membership?.fields.Status||'Disabled';}}
   return state;
  },
  async optins({barnId,userId}){
   const bm=await store.mapping('barn',barnId),um=await store.mapping('users',userId);
   if(!bm||um&&um.fields.source_scope!==barnId||!await store.allowed(bm.fields.external_id))fail('permission_denied',403);
   const person=um?await call(TABLE.people,{id:um.fields.external_id}):await store.one(TABLE.people,'entity_uid',userId);
   if(!person)fail('permission_denied',403);
   const memberships=(await call(TABLE.memberships)).filter(r=>r.fields.Person?.length===1&&r.fields.Person[0]===person.id&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===bm.fields.external_id);
   if(memberships.length!==1)fail('permission_denied',403);
   const membership=memberships[0];
   if(!await store.manages(bm.fields.external_id)&&(membership.fields.Status!=='Enabled'||person.fields.recognition_person_uid!==actor.id))fail('permission_denied',403);
   const rows=(await call(TABLE.optins)).filter(r=>r.fields.Membership?.length===1&&r.fields.Membership[0]===membership.id||!r.fields.Membership?.length&&((r.fields.barn_uid===barnId&&r.fields.user_uid===userId)||(r.fields.target_type==='Person'&&!!person.fields.recognition_person_uid&&r.fields.target_uid===person.fields.recognition_person_uid&&r.fields.owner_uid===person.fields.recognition_person_uid)));
   return {ok:true,userId,name:person.fields['display-name']||person.fields.name,subscriptions:rows.map(r=>({id:r.fields.entity_uid,status:r.fields.status,consentState:r.fields.consent_state,phone:r.fields.phone_e164||'',preferences:r.fields.preferences_json?JSON.parse(r.fields.preferences_json):null})),manageUrl:person.fields.recognition_person_uid===actor.id?'/rs-barn-onboarding-alerts':null};
  },
  async intake(payload,{profileLink=false}={}){
   if(profileLink)payload={...payload,kind:'users'};
   if(!selected.includes(payload?.kind))fail('invalid_kind',400);
   const id=await digest(`tenants-barn-v1|${actor.id}|${payload.requestId}`),operationState=await readTenantSave(env,id),fingerprint=await payloadHash(payload);
   const savedOperation=operationState.row?.result_json?JSON.parse(operationState.row.result_json):null;
   if(savedOperation?.payloadHash){
    if(savedOperation.actorId!==actor.id||savedOperation.payloadHash!==fingerprint)fail('request_id_reused');
    if(operationState.row.state==='complete')return directResult(savedOperation.record,{kind:payload.kind});
    const e=structuredClone(savedOperation.envelope);if(payload.draft?.pin)e.record.pin=payload.draft.pin;
    return runTenantSave({env,...operationState,prior:operationState.row,work:async operation=>{
     activeOperation=operation;
     const barn=await store.mapping('barn',e.record.barnId),access=barn&&await store.access(barn.fields.external_id);
     if(!barn&&e.entityKind==='barn'&&!e.targetId)fail('write_outcome_unknown',503);
     if(!access||!access.manage&&(e.entityKind!=='users'||access.person.id!==e.targetId))fail('permission_denied',403);
     await currentActor({actor});
     const prepared=structuredClone(savedOperation.prepared);
     if(e.entityKind==='users'&&!access.manage){
      const memberships=(await call(TABLE.memberships,{fresh:true})).filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===e.targetId&&m.fields.Tenant?.length===1&&m.fields.Tenant[0]===barn.fields.external_id);
      const current=memberships.length===1?memberships[0]:null;
      if(!current||current.fields.Status!=='Enabled'||e.record.membershipStatus!==current.fields.Status||prepared.roleIds&&!same(prepared.roleIds,(current.fields.Roles||[]).slice().sort()))fail('permission_denied',403);
     }
     if(prepared.credentials)prepared.credentials=await prepareManagedCredentials({env,fetchImpl,personUid:prepared.credentials.personUid,name:e.record.name,email:e.record.email,sms:e.record.sms,pin:e.record.pin});
     return applyDirect(e,prepared);
    }});
   }
   if(profileLink){await currentActor({actor});await prepareProfileLink(payload.barnId);}
   const eventId=await digest(`tenants-barn-v1|${actor.id}|${payload.requestId}`),prior=await submission(eventId);
   if(prior){const {e}=await envelope(prior);if(e.actor.id!==actor.id||!same(e.payload,payload))fail('request_id_reused');const done=await confirmed(prior,e);if(done)return done;fail(prior.fields['Processing status']==='Pending'?'tenant_save_pending':'tenant_save_needs_review',409);}
   await currentActor({actor});
   // Completed receipts are read without acquiring a new claim or repeating effects.
   const receipt=await store.one(TABLE.events,'event_uid',eventId);
   if(receipt){
    let saved;try{saved=JSON.parse(receipt.fields.result_json)}catch{fail('invalid_tenant_event')}
    await requireRecordAccess(payload.kind,saved);
    // Reuse common normalized request validation/fingerprint and authorization.
    const replay=await saveInputRecord({store:{...store,requestNamespace:'tenants-barn-v1'},actor,payload,profileLink});
    const claim=await claimOnce(env,'tenant-barn-direct-operation',eventId);
    if(!claim.result||claim.result.actorId!==actor.id||claim.result.inputHash!==receipt.fields.input_hash||!same(claim.result.record,saved))fail('write_outcome_unknown',503);
    return {...replay,record:publicRecord(saved)};
   }
   let record,event,previous;
   const staging={...store,requestNamespace:'tenants-barn-v1',
    async list(kind,barnId=''){const rows=await store.list(kind,barnId);return record&&kind===payload.kind?[...rows.filter(r=>r.id!==record.id),record]:rows;},
    async put(kind,value){previous=(await store.list(kind,value.barnId)).find(r=>r.id===value.id)||null;record=value;},
    async appendEvent(value){event=value;}
   };
   await saveInputRecord({store:staging,actor,payload,profileLink,includeState:false});if(!record||!event)fail('tenant_confirmation_missing');
   const map=await store.mapping(payload.kind,record.id),canonical=payload.kind==='users'&&!map?await store.one(TABLE.people,'entity_uid',record.id):null,targetId=map?.fields.external_id||profileTarget?.personId||canonical?.id||'';
   // First-copy is restricted to a previously owned isolated-trial record or
   // an explicit create through this authenticated editor; never match by name.
   if(previous&&previous.ownerUid!==actor.id&&!(payload.kind==='users'&&previous.recognitionPersonId===actor.id)&&!await store.manages((await store.mapping('barn',record.barnId))?.fields.external_id))fail('permission_denied',403);
   const linkedUser=record.userId?(await store.list('users',record.barnId)).find(r=>r.id===record.userId&&r.barnId===record.barnId):null;
   const e={linkedUser,profilePersonId:profileLink?profileTarget.personId:null,version:1,kind:'barn-rider',entityKind:payload.kind,actor,payload,record,previous,event:{...event,record},targetId,acceptedAt:clock()};
   await currentActor(e);
   const operationKey=await digest(JSON.stringify(['tenant-barn-direct-operation',eventId]));
   let existingOperation;
   try{existingOperation=await requireClaimDatabase(env).prepare('SELECT state, result_json FROM recognize_claims WHERE claim_key = ?').bind(operationKey).first();}catch{fail('recognition_claim_unavailable',503)}
   if(existingOperation){
    if(existingOperation.state!=='complete')fail('write_outcome_unknown',503);
    let saved;try{saved=JSON.parse(existingOperation.result_json)}catch{fail('recognition_claim_unavailable',503)}
    if(saved.actorId!==actor.id||saved.inputHash!==event.inputHash)fail('request_id_reused');
    return directResult(saved.record,{kind:payload.kind});
   }
   // Validation failures with no effects must not consume an operation/revision.
   const prepared=await preflightDirect(e);
   const savedEnvelope=structuredClone({...e,payload:{...payload,draft:{...payload.draft,pin:undefined}},record:{...record,pin:undefined},event:{...e.event,record:{...record,pin:undefined}}});
   return runTenantSave({env,key:operationState.key,prior:null,context:{actorId:actor.id,payloadHash:fingerprint,envelope:savedEnvelope,prepared:JSON.parse(JSON.stringify(prepared))},work:async operation=>{
    activeOperation=operation;
    await operation.reserveRevision(`${payload.kind}|${record.id}|${previous?.revision||0}`);
    // The second preflight remains under ownership; zero-effect failures release it.
    const currentPrepared=await preflightDirect(e,{fresh:true});
    return applyDirect(e,currentPrepared);
   }});
  },
  async claim(rowId,sig){
   if(!/^rec[A-Za-z0-9]+$/.test(rowId||''))fail('invalid_submission',400);
   const row=await call(TABLE.submissions,{id:rowId}),{e,sig:expected}=await envelope(row);if(sig!==expected)fail('invalid_submission',403);
   if(row.fields['Processing status']==='Applied')return {ok:true,state:'Applied'};
   if(row.fields['Processing status']!=='Pending')fail('tenant_save_needs_review');
   const submissionClaim=await claimOnce(env,'tenant-barn-submission',rowId);if(!submissionClaim.won)fail('tenant_save_needs_review');
   try{
   await currentActor(e);
   const current=(await store.list(e.entityKind,e.record.barnId)).find(r=>r.id===e.record.id)||null;
   if(!same(current,e.previous))fail('record_changed');
   if(e.entityKind==='riders'){const barns=await store.list('barn');if(!barns.some(r=>r.id===e.record.barnId&&r.ownerUid===e.actor.id))fail('permission_denied',403);}
   const claim=await claimOnce(env,'tenant-barn-entity-revision',`${e.entityKind}|${e.record.id}|${e.previous?.revision||0}`);
   if(!claim.won)fail('tenant_save_needs_review');
   return {ok:true,state:'Claimed',envelope:e};
   }catch(error){error.processingClaimed=true;throw error;}
  },
  async complete(rowId,sig){const row=await call(TABLE.submissions,{id:rowId}),{e,sig:expected}=await envelope(row);if(sig!==expected)fail('invalid_submission',403);await currentActor(e);const result=await confirmed(row,e);if(!result)fail('tenant_confirmation_missing');return result;}
 };
}
