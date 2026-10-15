// Compatibility boundary for the existing native Barn/Rider editor.
// Astro validates and logs; the Airtable automation applies canonical records.
import { InputError, saveInputRecord, readInputState, digest } from './rs-inputs.js';
import { claimOnce, completeClaim } from './rs-recognition-claims.js';
import { createAccessStore } from './rs-inputs-access.js';

export const BASE='app7BXcw801n19QKF';
export const TABLE={barn:'tbled1saw01Thffs1',riders:'tbllUlTlcEMgZiW3h',sources:'tblRswgKjA9dt3PED',events:'tbluecPFIorOytASP',submissions:'tblldMTDEC697hPwE',people:'tblyTBgrgdhT3Tcg7',memberships:'tblFf4aWQYA2RRsWo',roles:'tblRqRbjmqmNfJtIP',roster:'tblwcczj8lFhVcbyt'};
export const SOURCE='rs-inputs-native';
const fail=(code,status=409)=>{throw new InputError(code,status)};
const quote=v=>String(v).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const selected=['barn','riders'];
export function createTenantRest(env,fetchImpl=fetch){
 if(!env.AIRTABLE_TOKEN||env.RS_INPUTS_WRITE_MODE!=='isolated-trial')fail('tenant_configuration_required',503);
 let last=0;const cache=new Map();
 return async function call(table,{id,formula,fields,method='GET'}={}){
  if(!Object.values(TABLE).includes(table)||(method!=='GET'&&table!==TABLE.submissions))fail('tenant_write_boundary',403);
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
export function createTenantBarnBridge({env,legacy,actor,fetchImpl=fetch,call=createTenantRest(env,fetchImpl),clock=()=>new Date().toISOString()}){
 const store=createTenantBarnStore({legacy,call,actor});
 const key=()=>{const secret=env.RS_INPUTS_SESSION_SECRET;if(!/^[a-fA-F0-9]{64}$/.test(secret||''))fail('access_secret_missing',503);return crypto.subtle.importKey('raw',Uint8Array.from(secret.match(/../g),x=>parseInt(x,16)),{name:'HMAC',hash:'SHA-256'},false,['sign','verify']);};
 const proof=e=>new TextEncoder().encode('tenant-barn-v1|'+JSON.stringify(e));
 const signature=async e=>[...new Uint8Array(await crypto.subtle.sign('HMAC',await key(),proof(e)))].map(v=>v.toString(16).padStart(2,'0')).join('');
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
 return {store,
  async intake(payload){
   if(!selected.includes(payload?.kind))fail('invalid_kind',400);
   const eventId=await digest(`tenants-barn-v1|${actor.id}|${payload.requestId}`),prior=await submission(eventId);
   if(prior){const {e}=await envelope(prior);if(e.actor.id!==actor.id||!same(e.payload,payload))fail('request_id_reused');const done=await confirmed(prior,e);if(done)return done;fail(prior.fields['Processing status']==='Pending'?'tenant_save_pending':'tenant_save_needs_review',409);}
   let record,event,previous;
   const staging={...store,requestNamespace:'tenants-barn-v1',
    async list(kind){const rows=await store.list(kind);return record&&kind===payload.kind?[...rows.filter(r=>r.id!==record.id),record]:rows;},
    async put(kind,value){previous=(await store.list(kind)).find(r=>r.id===value.id)||null;record=value;},
    async appendEvent(value){event=value;}
   };
   await saveInputRecord({store:staging,actor,payload});if(!record||!event)fail('tenant_confirmation_missing');
   const map=await store.mapping(payload.kind,record.id),targetId=map?.fields.external_id||'';
   // First-copy is restricted to a previously owned isolated-trial record or
   // an explicit create through this authenticated editor; never match by name.
   if(previous&&previous.ownerUid!==actor.id)fail('permission_denied',403);
   const linkedUser=record.userId?(await store.list('users')).find(r=>r.id===record.userId&&r.barnId===record.barnId):null;
   const e={linkedUser,version:1,kind:'barn-rider',entityKind:payload.kind,actor,payload,record,previous,event:{...event,record},targetId,acceptedAt:clock()};
   const admission=await claimOnce(env,'tenant-barn-intake',eventId);if(!admission.won)fail('tenant_save_pending');
   const rows=await call(TABLE.submissions,{method:'POST',fields:{'Submission ID':eventId,'Submitted at':e.acceptedAt,'Actor ID':actor.id,'Target table ID':TABLE[payload.kind],'Target record ID':targetId,Action:targetId?'Update':'Create','Submitted changes':JSON.stringify({...e,signature:await signature(e)}),'Processing status':'Pending'}});
   if(rows.length!==1)fail('write_outcome_unknown',503);await completeClaim(env,admission,{submissionId:rows[0].id});
   fail('tenant_save_pending',409);
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
