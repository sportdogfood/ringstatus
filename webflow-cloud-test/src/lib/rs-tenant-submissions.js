import {InputError,digest} from './rs-inputs.js';
import {validatePreferences} from './rs-input-preferences.js';
import {createTenantSubmissionStore,TENANTS_BASE,TABLES} from './rs-tenant-submission-store.js';
const fail=(code,status=409)=>{throw new InputError(code,status)};
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const uid=v=>typeof v==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(v);
const canonical=v=>JSON.stringify(v&&typeof v==='object'?Array.isArray(v)?v.map(x=>JSON.parse(canonical(x))):Object.fromEntries(Object.keys(v).sort().map(k=>[k,JSON.parse(canonical(v[k]))])):v);
export function createTenantSubmissionBridge({env,definitions,store=createTenantSubmissionStore({env}),clock=()=>new Date().toISOString()}){
 if(env.RS_INPUTS_TENANT_SUBMISSIONS_MODE!=='isolated-trial'||env.RS_INPUTS_WRITE_MODE!=='isolated-trial')fail('tenant_submission_mode_required',503);
 const db=env.RS_RECOGNITION_CONTROL_DB;if(!db?.prepare)fail('subscription_control_database_required',503);
 if(!/^[a-fA-F0-9]{64}$/.test(env.RS_INPUTS_SESSION_SECRET||''))fail('access_secret_missing',503);
 const bytes=Uint8Array.from(env.RS_INPUTS_SESSION_SECRET.match(/../g),x=>parseInt(x,16));
 const key=crypto.subtle.importKey('raw',bytes,{name:'HMAC',hash:'SHA-256'},false,['sign','verify']);
 const unsigned=e=>({...e,admission:{version:e.admission.version,token:e.admission.token}});
 const proof=e=>new TextEncoder().encode('rs-tenant-admission-v1|'+TENANTS_BASE+'|'+TABLES.sub+'|'+e.eventId+'|'+canonical(unsigned(e)));
 const completionProof=(e,result,claimToken)=>new TextEncoder().encode('rs-tenant-completion-v1|'+TENANTS_BASE+'|'+TABLES.sub+'|'+e.eventId+'|'+canonical({envelope:unsigned(e),result,claimToken}));
 const hex=bytes=>Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');
 const receipt=async(e,result,claimToken)=>'rs-tenant-completed-v1:'+JSON.stringify({claimToken,signature:hex(await crypto.subtle.sign('HMAC',await key,completionProof(e,result,claimToken)))});
 const sign=async e=>Array.from(new Uint8Array(await crypto.subtle.sign('HMAC',await key,proof(e))),b=>b.toString(16).padStart(2,'0')).join('');
 const rowFor=actor=>db.prepare('SELECT * FROM subscription_write_reservations WHERE actor_uid=?').bind(actor).first();
 const authorize=actor=>{if(!uid(actor?.id)||actor.profile?.personUid!==actor.id||!actor.permissions?.includes('inputs:read')||!actor.permissions?.includes('inputs:write'))fail('permission_denied',403)};
 async function targetFor(actor){const id='rs_'+(await digest('sms-preferences|'+actor)).slice(0,32),row=await store.target(id),f=row?.fields;
  if(!row||!/^rec[A-Za-z0-9]+$/.test(row.id)||f.entity_uid!==id||f.owner_uid!==actor||f.target_uid!==actor||f.target_type!=='Person'||f.record_mode!=='Test'||!['Draft','Revoked','Paused'].includes(f.status)||['barn_uid','user_uid','engine_scope','alert_types'].some(k=>f[k])||['Membership','Managed by','Selected favorites'].some(k=>f[k]?.length)||!Number.isSafeInteger(f.revision)||f.revision<1)fail('preference_scope_mismatch');return row;
 }
 async function verify(capability){
  if(!capability||Object.keys(capability).some(k=>!['eventId','version','token','signature'].includes(k))||!/^[a-f0-9]{64}$/.test(capability.eventId||'')||capability.version!==1||!/^[a-f0-9-]{36}$/.test(capability.token||'')||!/^[a-f0-9]{64}$/.test(capability.signature||''))fail('invalid_admission',403);
  const row=await store.submission(capability.eventId);if(!row)fail('submission_not_found',404);
  let e;try{e=JSON.parse(row.fields['Submitted changes']);}catch{fail('invalid_admission',403)}
  if(!e?.admission||!same(e.admission,{version:capability.version,token:capability.token,signature:capability.signature})||e.eventId!==capability.eventId||!await crypto.subtle.verify('HMAC',await key,Uint8Array.from(capability.signature.match(/../g),x=>parseInt(x,16)),proof(e)))fail('invalid_admission',403);
  if(e.version!==1||e.kind!=='personal-preferences'||e.targetTableId!==TABLES.target||row.fields['Submission ID']!==e.eventId||row.fields['Actor ID']!==e.actorId||row.fields['Target table ID']!==e.targetTableId||row.fields['Target record ID']!==e.targetRecordId||row.fields.Action!=='Update'||row.fields.Membership?.length)fail('submission_scope_mismatch');
  return {row,e};
 }
 async function exactResult(e){const event=await store.event(e.eventId);if(!event)return null;const f=event.fields;let result;try{result=JSON.parse(f.result_json)}catch{fail('operation_result_unavailable')}
  const entity='rs_'+(await digest('sms-preferences|'+e.actorId)).slice(0,32);
  if(f.event_uid!==e.eventId||f.input_hash!==e.inputHash||f.request_uid!==e.requestId||f.actor_uid!==e.actorId||f.entity_uid!==entity||f.kind!=='subscriptions'||f.outcome!=='committed'||f.record_mode!=='Test'||f.barn_uid||result.id!==entity||result.ownerUid!==e.actorId||result.targetUid!==e.actorId||result.targetType!=='Person'||result.requestId!==e.requestId||result.actorId!==e.actorId||result.revision!==e.expectedRevision+1||!same(validatePreferences(result.preferences,definitions),e.preferences)||result.status!==(e.preferences.enabled?'Draft':'Revoked')||result.consentState!==(e.preferences.enabled?'Granted':'Revoked')||result.consentAt!==e.acceptedAt||result.consentSource!==e.source||result.noticeVersion!==e.noticeVersion||(result.revokedAt??null)!==(e.preferences.enabled?null:e.acceptedAt)||f.action!==(e.preferences.enabled?'consent_granted':'consent_revoked')||f.occurred_at!==e.acceptedAt)fail('operation_result_unavailable');
  return {ok:true,state:'Applied',requestId:e.requestId,actorId:e.actorId,eventId:e.eventId,preferences:result.preferences,revision:result.revision};
 }
 async function verifyEffects(e){const result=await exactResult(e);if(!result)fail('operation_result_unavailable');const target=await targetFor(e.actorId),f=target.fields;
  const expected={revision:result.revision,request_uid:e.eventId,request_hash:e.inputHash,preferences_json:JSON.stringify(e.preferences),phone_e164:e.preferences.phone,status:e.preferences.enabled?'Draft':'Revoked',consent_state:e.preferences.enabled?'Granted':'Revoked',consent_at:e.acceptedAt,consent_source:e.source,notice_version:e.noticeVersion,revoked_at:e.preferences.enabled?null:e.acceptedAt};
  if(Object.entries(expected).some(([k,v])=>k==='phone_e164'&&v===''?(f[k]??'')!=='':(f[k]??null)!==(v??null)))fail('target_readback_mismatch');
  const options=await store.options(e.targetRecordId);if(options.length!==18||new Set(options.map(r=>r.fields.Option)).size!==18||options.some(r=>{const p=e.preferences.variables[r.fields.Option];return !p||r.fields['Opt-in']?.length!==1||r.fields['Opt-in'][0]!==e.targetRecordId||r.fields.Choice!==(p.enabled?'On':'Off')||(r.fields.Value||'')!==p.value}))fail('options_readback_mismatch');return result;
 }
 async function verifiedReceipt(row,e){
  if(row.fields['Processing status']!=='Applied')return null;const result=await exactResult(e);if(!result)fail('operation_result_unavailable');
  const note=row.fields['Error / review note'];let receipt;try{if(!note?.startsWith('rs-tenant-completed-v1:'))throw Error();receipt=JSON.parse(note.slice('rs-tenant-completed-v1:'.length));}catch{fail('operation_result_unavailable')}
  if(typeof receipt.claimToken!=='string'||!/^[a-f0-9]{64}$/.test(receipt.signature||'')||!await crypto.subtle.verify('HMAC',await key,Uint8Array.from(receipt.signature.match(/../g),x=>parseInt(x,16)),completionProof(e,result,receipt.claimToken)))fail('operation_result_unavailable');return {result,claimToken:receipt.claimToken};
 }
 async function terminal(row,e){const confirmed=await verifiedReceipt(row,e);if(!confirmed)return null;const reserved=await rowFor(e.actorId);if(!reserved)fail('operation_result_unavailable');if(reserved.request_uid===e.eventId&&reserved.request_hash===e.inputHash&&reserved.worker_token===confirmed.claimToken){if(reserved.state==='working')return null;if(reserved.state!=='idle')fail('operation_result_unavailable');}return confirmed.result;}
 const pending=e=>({ok:true,state:'Pending',requestId:e.requestId,actorId:e.actorId,eventId:e.eventId});
 return {
  async read({actor}){authorize(actor);const row=await targetFor(actor.id);return {ok:true,preferences:validatePreferences(JSON.parse(row.fields.preferences_json),definitions),revision:row.fields.revision}},
  async intake({actor,payload}){
   authorize(actor);if(!payload||Object.keys(payload).some(k=>!['action','requestId','expectedRevision','preferences'].includes(k))||payload.action!=='preferences'||!/^\w[\w-]{7,127}$/.test(payload.requestId||'')||!Number.isSafeInteger(payload.expectedRevision)||payload.expectedRevision<1)fail('invalid_payload',400);
   const preferences=validatePreferences(payload.preferences,definitions),eventId=await digest('subscriptions|'+actor.id+'|'+payload.requestId),inputHash=await digest(JSON.stringify({preferences,expectedRevision:payload.expectedRevision}));
   const existing=await store.submission(eventId);if(existing){const e=JSON.parse(existing.fields['Submitted changes']);if(e.actorId!==actor.id||e.inputHash!==inputHash)fail('request_id_reused');await verify({eventId,...e.admission});return await terminal(existing,e)||pending(e)}
   const target=await targetFor(actor.id);const options=await store.options(target.id);if(options.length!==18||new Set(options.map(r=>r.fields.Option)).size!==18||options.some(r=>!definitions.some(d=>d.key===r.fields.Option)||r.fields['Opt-in']?.length!==1||r.fields['Opt-in'][0]!==target.id))fail('exact_existing_options_required');if(target.fields.revision!==payload.expectedRevision)fail('record_changed');
   const prior=await store.event(target.fields.request_uid);let previous;try{previous=JSON.parse(prior?.fields?.result_json)}catch{}if(!prior||prior.fields.input_hash!==target.fields.request_hash||previous?.id!==target.fields.entity_uid||previous.ownerUid!==actor.id||previous.targetUid!==actor.id||previous.targetType!=='Person'||previous.revision!==target.fields.revision)fail('prior_audit_pending');
   const previousReservation=await rowFor(actor.id);if(previousReservation&&previousReservation.state!=='idle'){if(previousReservation.request_uid===eventId&&previousReservation.request_hash!==inputHash)fail('request_id_reused');if(previousReservation.request_uid===eventId)fail('write_outcome_unknown',503);fail('subscription_request_in_progress');}if(previousReservation?.state==='idle'){const priorSubmission=await store.submission(previousReservation.request_uid);if(priorSubmission){const oldEnvelope=JSON.parse(priorSubmission.fields['Submitted changes']);if(!await terminal(priorSubmission,oldEnvelope))fail('previous_completion_pending');}}
   const token=crypto.randomUUID(),acceptedAt=clock();if(!Number.isFinite(Date.parse(acceptedAt)))fail('invalid_server_time',503);
   const e={version:1,kind:'personal-preferences',actorId:actor.id,targetTableId:TABLES.target,targetRecordId:target.id,requestId:payload.requestId,eventId,inputHash,expectedRevision:payload.expectedRevision,acceptedAt,noticeVersion:'sms-preferences-v1',source:'/rs-barn-onboarding-alerts:explicit-save',preferences,admission:{version:1,token}};e.admission.signature=await sign(e);
   const inserted=previousReservation
    ?await db.prepare("UPDATE subscription_write_reservations SET request_uid=?,request_hash=?,worker_token=?,state='working',updated_at=CURRENT_TIMESTAMP WHERE actor_uid=? AND request_uid=? AND request_hash=? AND worker_token=? AND state='idle'").bind(eventId,inputHash,token,actor.id,previousReservation.request_uid,previousReservation.request_hash,previousReservation.worker_token).run()
    :await db.prepare("INSERT INTO subscription_write_reservations (actor_uid,request_uid,request_hash,worker_token,state) VALUES (?,?,?,?,'working') ON CONFLICT(actor_uid) DO NOTHING").bind(actor.id,eventId,inputHash,token).run();
   if(inserted.meta.changes!==1){const row=await rowFor(actor.id);if(row?.request_uid===eventId&&row.request_hash!==inputHash)fail('request_id_reused');if(row?.request_uid===eventId)fail('write_outcome_unknown',503);fail('subscription_request_in_progress');}
   // Admission stays reserved even if this one append returns an unknown outcome.
   const saved=await store.create(e);if(saved.fields['Submitted changes']!==JSON.stringify(e))fail('write_outcome_unknown',503);return pending(e);
  },
  async status({actor,requestId}){authorize(actor);if(!/^\w[\w-]{7,127}$/.test(requestId||''))fail('invalid_request_id',400);const eventId=await digest('subscriptions|'+actor.id+'|'+requestId),row=await store.submission(eventId);if(!row)fail('submission_not_found',404);const e=JSON.parse(row.fields['Submitted changes']);await verify({eventId,...e.admission});if(e.actorId!==actor.id)fail('permission_denied',403);const result=await terminal(row,e);if(result)return result;if(!['Pending','Applied'].includes(row.fields['Processing status']))fail(row.fields['Error / review note']||'submission_needs_review');return pending(e)},
  async claim({capability}){const {row,e}=await verify(capability);const replay=await exactResult(e);if(replay){const reserved=await rowFor(e.actorId);if(row.fields['Processing status']==='Applied'){const saved=await terminal(row,e);if(saved)return saved;}if(reserved?.request_uid!==e.eventId||reserved.request_hash!==e.inputHash||!['working','idle'].includes(reserved.state)||reserved.worker_token===e.admission.token)fail('invalid_completion_claim',403);return {ok:true,state:'Replayed',claimToken:reserved.worker_token,result:replay};}if(row.fields['Processing status']!=='Pending')fail('submission_needs_review');const claimToken=crypto.randomUUID();
   const won=await db.prepare("UPDATE subscription_write_reservations SET state='working',worker_token=?,updated_at=CURRENT_TIMESTAMP WHERE actor_uid=? AND request_uid=? AND request_hash=? AND worker_token=? AND state='working'").bind(claimToken,e.actorId,e.eventId,e.inputHash,e.admission.token).run();if(won.meta.changes!==1)fail('submission_claim_in_progress');return {ok:true,state:'Claimed',claimToken};
  },
  async complete({capability,claimToken}){const {row,e}=await verify(capability);const completed=await terminal(row,e);if(completed)return completed;
   const reservation=await rowFor(e.actorId);if(!reservation||reservation.request_uid!==e.eventId||reservation.request_hash!==e.inputHash||!['working','idle'].includes(reservation.state)||reservation.worker_token!==claimToken||claimToken===e.admission.token)fail('invalid_completion_claim',403);const result=await verifyEffects(e);
   // Keep shared ownership held while storing and reading back the verified-effects receipt.
   if(!await verifiedReceipt(row,e))await store.setStatus(row.id,'Applied',await receipt(e,result,claimToken));
   const stored=await store.submission(e.eventId),proof=stored&&await verifiedReceipt(stored,e);if(!proof||proof.claimToken!==claimToken)fail('write_outcome_unknown',503);
   if(reservation.state==='working'){const released=await db.prepare("UPDATE subscription_write_reservations SET state='idle',updated_at=CURRENT_TIMESTAMP WHERE actor_uid=? AND request_uid=? AND request_hash=? AND worker_token=? AND state='working'").bind(e.actorId,e.eventId,e.inputHash,claimToken).run();if(released.meta.changes!==1)fail('subscription_control_unavailable',503);}
   const finalized=await terminal(stored,e);if(!finalized)fail('write_outcome_unknown',503);return finalized;
  }
 };
}
