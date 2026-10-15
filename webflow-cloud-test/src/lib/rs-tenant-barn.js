// Compatibility boundary for the existing native Barn/Rider editor.
// Astro validates, writes canonical Airtable records and returns verified data.
import { InputError, saveInputRecord, readInputState, digest } from './rs-inputs.js';
import { claimOnce, completeClaim, requireClaimDatabase } from './rs-recognition-claims.js';
import { createAccessStore } from './rs-inputs-access.js';

export const BASE='app7BXcw801n19QKF';
export const TABLE={barn:'tbled1saw01Thffs1',riders:'tbllUlTlcEMgZiW3h',sources:'tblRswgKjA9dt3PED',events:'tbluecPFIorOytASP',submissions:'tblldMTDEC697hPwE',people:'tblyTBgrgdhT3Tcg7',memberships:'tblFf4aWQYA2RRsWo',roles:'tblRqRbjmqmNfJtIP',roster:'tblwcczj8lFhVcbyt'};
export const SOURCE='rs-inputs-native';
const fail=(code,status=409)=>{throw new InputError(code,status)};
const quote=v=>String(v).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const selected=['barn','riders'];
const DIRECT_FIELDS={ [TABLE.barn]:['tenant_official_name'],[TABLE.riders]:['name','people'],[TABLE.people]:['entity_uid','recognition_person_uid','name','email','status','record_mode'],[TABLE.memberships]:['Name','Person','Tenant','Roles','Status'],[TABLE.roster]:['Name','Tenant','Rider','Status'],[TABLE.sources]:['mapping_uid','entity_type','entity_uid','source_system','source_entity_type','source_scope','external_id','record_mode'],[TABLE.events]:['event_uid','actor_uid','entity_uid','barn_uid','action','kind','outcome','request_uid','input_hash','result_json','occurred_at','record_mode'] };

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
   let body;try{const res=await fetchImpl(url,{method,redirect:'manual',signal:AbortSignal.timeout(15000),headers:{Authorization:`Bearer ${env.AIRTABLE_TOKEN}`,'Content-Type':'application/json'},...(fields?{body:JSON.stringify({records:[{...(id?{id}:{}),fields}]})}:{})});if(!res.ok)throw Error();body=await res.json();}catch{fail(method==='GET'?'storage_unavailable':'write_outcome_unknown',503)}
   if(id&&method==='GET'){if(body.id!==id||!body.fields)fail('invalid_storage_response',502);cache.set(cacheKey,body);return structuredClone(body);}
   if(!Array.isArray(body.records))fail('invalid_storage_response',502);rows.push(...body.records);
   if(!body.offset||method!=='GET')break;if(seen.has(body.offset))fail('invalid_storage_response',502);seen.add(body.offset);url.searchParams.set('offset',body.offset);
  }while(true);if(method==='GET')cache.set(cacheKey,rows);return structuredClone(rows);
 };
}
export function createTenantBarnStore({legacy,call,actor}){
 const one=async(table,field,value)=>{const rows=await call(table,{formula:`{${field}} = '${quote(value)}'`});if(rows.length>1)fail('ambiguous_tenant_mapping');return rows[0]||null;};
 const mapping=(kind,id)=>one(TABLE.sources,'mapping_uid',`${SOURCE}|${kind}|${id}`);
 async function allowed(tenantId){
  const person=await one(TABLE.people,'recognition_person_uid',actor.id);if(!person)return false;
  const memberships=(await call(TABLE.memberships)).filter(r=>r.fields.Person?.length===1&&r.fields.Person[0]===person.id&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===tenantId);
  if(memberships.length>1)fail('ambiguous_tenant_membership');
  const membership=memberships[0];
  if(membership?.fields.Status!=='Enabled')return false;
  // Barn/Rider editing is the owner's existing permission. Descriptive people
  // roles, trainer labels, or client-supplied role names never grant it.
  const owners=(await call(TABLE.roles)).filter(r=>r.fields.rec_name==='owner');
  if(owners.length!==1)fail('tenant_owner_role_required');
  return membership.fields.Roles?.includes(owners[0].id)===true;
 }
 async function snapshot(kind,map){
  const m=map.fields;if(m.source_system!==SOURCE||m.source_entity_type!==kind||!/^rec[A-Za-z0-9]+$/.test(m.external_id||''))fail('invalid_tenant_mapping');
  const events=await call(TABLE.events,{formula:`AND({entity_uid} = '${quote(m.entity_uid)}', {kind} = '${kind}', {outcome} = 'committed')`});
  const snapshots=events.map(r=>{let data;try{data=JSON.parse(r.fields.result_json)}catch{fail('invalid_tenant_event')};return {row:r,data};}).filter(x=>x.data.id===m.entity_uid&&x.data.barnId===m.source_scope);
  snapshots.sort((a,b)=>b.data.revision-a.data.revision);const latest=snapshots[0];
  if(!latest||!Number.isSafeInteger(latest.data.revision)||latest.data.revision<1||!latest.data.ownerUid)fail('tenant_confirmation_missing');
  const target=await call(TABLE[kind],{id:m.external_id});
  const name=target.fields[kind==='barn'?'tenant_official_name':'name'];if(typeof name!=='string'||!name.trim())fail('invalid_tenant_record');
  return {...latest.data,name,requestUid:latest.row.fields.event_uid,requestHash:latest.row.fields.input_hash};
 }
 const api={
  ...legacy,one,mapping,allowed,snapshot,
  async list(kind){
   if(!selected.includes(kind))return legacy.list(kind);
   const original=await legacy.list(kind),maps=await call(TABLE.sources,{formula:`AND({source_system} = '${SOURCE}', {source_entity_type} = '${kind}')`});
   const byId=new Map(original.map(r=>[r.id,r]));const seen=new Set();
   for(const map of maps){const id=map.fields.entity_uid;if(seen.has(id))fail('ambiguous_tenant_mapping');seen.add(id);
    const barnMap=kind==='barn'?map:await mapping('barn',map.fields.source_scope);
    // A mapped record never falls back to legacy ownership after disable/revocation.
    byId.delete(id);if(!barnMap||!await allowed(barnMap.fields.external_id))continue;
    if(kind==='riders'){const roster=await call(TABLE.roster);if(!roster.some(r=>r.fields.Status==='Enabled'&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===barnMap.fields.external_id&&r.fields.Rider?.length===1&&r.fields.Rider[0]===map.fields.external_id&&!r.fields.Horse?.length))continue;}
    byId.set(id,await snapshot(kind,map));
   }return [...byId.values()];
  },
  async put(kind,record,options){if(selected.includes(kind))fail('tenant_submission_required');return legacy.put(kind,record,options);},
  async event(id){const row=await one(TABLE.events,'event_uid',id);if(!row)return legacy.event(id);let record;try{record=JSON.parse(row.fields.result_json)}catch{fail('invalid_tenant_event')};return {inputHash:row.fields.input_hash,record};},
 };
 return api;
}
export function createTenantBarnBridge({env,legacy,actor,fetchImpl=fetch,call=createTenantRest(env,fetchImpl),clock=()=>new Date().toISOString(),includeState=true}){
 const store=createTenantBarnStore({legacy,call,actor});
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
  await store.snapshot(e.entityKind,map);
  const barn=await store.mapping('barn',e.record.barnId);if(!barn||!await store.allowed(barn.fields.external_id))fail('permission_denied',403);
  const {ownerUid,requestUid,requestHash,...publicRecord}=record;
  return {ok:true,record:publicRecord,state:await readInputState(store,actor,e.record.barnId)};
 }
 const equalField=(actual,expected)=>same(actual??(Array.isArray(expected)?[]:expected===''?'':null),expected??null);
 const publicRecord=({ownerUid,requestUid,requestHash,...value})=>value;
 async function directResult(record,{verifyCurrent=false}={}){
  const barn=await store.mapping('barn',record.barnId);
  if(!barn||!await store.allowed(barn.fields.external_id))fail('permission_denied',403);
  if(!includeState){
   const kind=record.id===record.barnId?'barn':'riders';
   const map=await store.mapping(kind,record.id);if(!map)fail('tenant_confirmation_missing');
   const latest=await store.snapshot(kind,map);
   if(kind==='riders'){
    const roster=await call(TABLE.roster);
    if(!roster.some(r=>r.fields.Status==='Enabled'&&r.fields.Tenant?.length===1&&r.fields.Tenant[0]===barn.fields.external_id&&r.fields.Rider?.length===1&&r.fields.Rider[0]===map.fields.external_id&&!r.fields.Horse?.length))fail('tenant_confirmation_missing');
   }
   if(verifyCurrent&&!same(publicRecord(latest),publicRecord(record)))fail('tenant_confirmation_missing');
   return {ok:true,record:publicRecord(latest)};
  }
  const state=await readInputState(store,actor,record.barnId);
  if(verifyCurrent){const rows=record.id===record.barnId?state.barns:state.riders;if(!rows.some(r=>same(r,publicRecord(record))))fail('tenant_confirmation_missing');}
  return {ok:true,record:publicRecord(record),state};
 }
 async function write(table,fields,id){
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
 async function preflightDirect(e){
  await call(TABLE.roles,{fresh:true});
  const r=e.record,kind=e.entityKind;
  const map=await store.mapping(kind,r.id);if((map?.fields.external_id||'')!==e.targetId)fail('mapping_changed');
  const now=(await store.list(kind)).find(x=>x.id===r.id)||null;if(!same(now,e.previous))fail('record_changed');
  let tenantId=kind==='barn'?e.targetId:(await store.mapping('barn',r.barnId))?.fields.external_id;
  if(kind==='riders'&&!tenantId)fail('save_barn_first');
  if(tenantId)await member(tenantId,false);
  const nameField=kind==='barn'?'tenant_official_name':'name';
  if(e.targetId){const old=await call(TABLE[kind],{id:e.targetId});if(old.fields[nameField]!==e.previous?.name)fail('record_changed');}
  else if((await call(TABLE[kind])).some(x=>[r.name,e.previous?.name].filter(Boolean).some(n=>String(x.fields[nameField]||'').trim().toLowerCase()===n.trim().toLowerCase())))fail('existing_record_mapping_required');
  let roster=null;
  if(kind==='riders'&&e.targetId){const rows=(await call(TABLE.roster)).filter(x=>x.fields.Tenant?.includes(tenantId)&&x.fields.Rider?.includes(e.targetId));if(rows.length!==1)fail('roster_not_enabled');roster=rows[0];if(roster.fields.Status!=='Enabled'||roster.fields.Horse?.length||roster.fields.Tenant.length!==1||roster.fields.Rider.length!==1)fail('roster_not_enabled');}
  const fields={[nameField]:r.name};
  if(kind==='riders'){
   fields.people=[];
   if(r.userId){const rows=(await call(TABLE.people)).filter(x=>x.fields.entity_uid===r.userId||(e.linkedUser?.recognitionPersonId&&x.fields.recognition_person_uid===e.linkedUser.recognitionPersonId));if(rows.length!==1)fail('person_mapping_required');fields.people=[rows[0].id];}
  }
  if(kind==='barn'&&!e.targetId){
   const owners=(await call(TABLE.roles)).filter(x=>x.fields.rec_name==='owner');if(owners.length!==1)fail('tenant_owner_role_required');
   await store.one(TABLE.people,'recognition_person_uid',actor.id);
  }
  await currentActor(e);
  return {map,tenantId,roster,fields};
 }
 async function applyDirect(e){
  // Recheck race-sensitive permissions, mapping and target after reservation.
  const {map,roster,fields,tenantId:existingTenant}=await preflightDirect(e);
  const r=e.record,kind=e.entityKind;let tenantId=existingTenant;
  const target=await write(TABLE[kind],fields,e.targetId||undefined);
  if(kind==='barn')tenantId=target.id;
  await member(tenantId,kind==='barn'&&!e.targetId);
  if(kind==='riders'&&!roster)await write(TABLE.roster,{Name:r.name,Tenant:[tenantId],Rider:[target.id],Status:'Enabled'});
  if(!map)await write(TABLE.sources,{mapping_uid:`${SOURCE}|${kind}|${r.id}`,entity_type:kind==='barn'?'Barn':'Rider',entity_uid:r.id,source_system:SOURCE,source_entity_type:kind,source_scope:r.barnId,external_id:target.id,record_mode:'Test'});
  await write(TABLE.events,{event_uid:e.event.eventId,actor_uid:actor.id,entity_uid:r.id,barn_uid:r.barnId,action:e.event.action,kind,outcome:'committed',request_uid:e.payload.requestId,input_hash:e.event.inputHash,result_json:JSON.stringify(r),occurred_at:e.acceptedAt,record_mode:'Test'});
  // Verify the same target again after related records/audit, never a cached value.
  const verified=await call(TABLE[kind],{id:target.id,fresh:true});for(const [k,v]of Object.entries(fields))if(!equalField(verified.fields[k],v))fail('target_readback_mismatch');
  await currentActor(e);
  return directResult(r,{verifyCurrent:true});
 }
 return {store,
  async intake(payload){
   if(!selected.includes(payload?.kind))fail('invalid_kind',400);
   const eventId=await digest(`tenants-barn-v1|${actor.id}|${payload.requestId}`),prior=await submission(eventId);
   if(prior){const {e}=await envelope(prior);if(e.actor.id!==actor.id||!same(e.payload,payload))fail('request_id_reused');const done=await confirmed(prior,e);if(done)return done;fail(prior.fields['Processing status']==='Pending'?'tenant_save_pending':'tenant_save_needs_review',409);}
   await currentActor({actor});
   // Completed receipts are read without acquiring a new claim or repeating effects.
   const receipt=await store.one(TABLE.events,'event_uid',eventId);
   if(receipt){
    let saved;try{saved=JSON.parse(receipt.fields.result_json)}catch{fail('invalid_tenant_event')}
    // Reuse common normalized request validation/fingerprint and authorization.
    const replay=await saveInputRecord({store:{...store,requestNamespace:'tenants-barn-v1'},actor,payload});
    const claim=await claimOnce(env,'tenant-barn-direct-operation',eventId);
    if(!claim.result||claim.result.actorId!==actor.id||claim.result.inputHash!==receipt.fields.input_hash||!same(claim.result.record,saved))fail('write_outcome_unknown',503);
    return {...replay,record:publicRecord(saved)};
   }
   let record,event,previous;
   const staging={...store,requestNamespace:'tenants-barn-v1',
    async list(kind){const rows=await store.list(kind);return record&&kind===payload.kind?[...rows.filter(r=>r.id!==record.id),record]:rows;},
    async put(kind,value){previous=(await store.list(kind)).find(r=>r.id===value.id)||null;record=value;},
    async appendEvent(value){event=value;}
   };
   await saveInputRecord({store:staging,actor,payload,includeState:false});if(!record||!event)fail('tenant_confirmation_missing');
   const map=await store.mapping(payload.kind,record.id),targetId=map?.fields.external_id||'';
   // First-copy is restricted to a previously owned isolated-trial record or
   // an explicit create through this authenticated editor; never match by name.
   if(previous&&previous.ownerUid!==actor.id)fail('permission_denied',403);
   const linkedUser=record.userId?(await store.list('users')).find(r=>r.id===record.userId&&r.barnId===record.barnId):null;
   const e={linkedUser,version:1,kind:'barn-rider',entityKind:payload.kind,actor,payload,record,previous,event:{...event,record},targetId,acceptedAt:clock()};
   await currentActor(e);
   const operationKey=await digest(JSON.stringify(['tenant-barn-direct-operation',eventId]));
   let existingOperation;
   try{existingOperation=await requireClaimDatabase(env).prepare('SELECT state, result_json FROM recognize_claims WHERE claim_key = ?').bind(operationKey).first();}catch{fail('recognition_claim_unavailable',503)}
   if(existingOperation){
    if(existingOperation.state!=='complete')fail('write_outcome_unknown',503);
    let saved;try{saved=JSON.parse(existingOperation.result_json)}catch{fail('recognition_claim_unavailable',503)}
    if(saved.actorId!==actor.id||saved.inputHash!==event.inputHash)fail('request_id_reused');
    return directResult(saved.record);
   }
   // Validation failures with no effects must not consume an operation/revision.
   await preflightDirect(e);
   const operation=await claimOnce(env,'tenant-barn-direct-operation',eventId);
   if(!operation.won){
    if(!operation.result)fail('write_outcome_unknown',503);
    if(operation.result.actorId!==actor.id||operation.result.inputHash!==event.inputHash)fail('request_id_reused');
    return directResult(operation.result.record);
   }
   const revision=await claimOnce(env,'tenant-barn-entity-revision',`${payload.kind}|${record.id}|${previous?.revision||0}`);
   if(!revision.won)fail('record_changed');
   const result=await applyDirect(e);
   // Store the receipt only after all canonical effects and audit are read back.
   await completeClaim(env,operation,{actorId:actor.id,inputHash:event.inputHash,record});
   return result;
  },
  async claim(rowId,sig){
   if(!/^rec[A-Za-z0-9]+$/.test(rowId||''))fail('invalid_submission',400);
   const row=await call(TABLE.submissions,{id:rowId}),{e,sig:expected}=await envelope(row);if(sig!==expected)fail('invalid_submission',403);
   if(row.fields['Processing status']==='Applied')return {ok:true,state:'Applied'};
   if(row.fields['Processing status']!=='Pending')fail('tenant_save_needs_review');
   const submissionClaim=await claimOnce(env,'tenant-barn-submission',rowId);if(!submissionClaim.won)fail('tenant_save_needs_review');
   try{
   await currentActor(e);
   const current=(await store.list(e.entityKind)).find(r=>r.id===e.record.id)||null;
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
