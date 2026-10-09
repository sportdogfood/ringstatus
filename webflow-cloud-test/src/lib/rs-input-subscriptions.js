import { InputError, digest } from './rs-inputs.js';
import { createAirtableInputStore } from './rs-inputs-airtable.js';
import { handleAccessRoute } from './rs-inputs-access.js';
import { createSubscriptionStore, BASE_ID } from './rs-input-subscription-store.js';
import { withSubscriptionReservation } from './rs-input-subscription-control.js';
const fail=(code,status=400)=>{throw new InputError(code,status)};
const uid=v=>typeof v==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(v);
const publicRecord=({owner_uid,request_uid,request_hash,...record})=>record;
async function bodyOf(request){
 if(!request.headers.get('Content-Type')?.toLowerCase().startsWith('application/json'))fail('json_required',415);
 const reader=request.body?.getReader();if(!reader)fail('invalid_payload');
 const chunks=[];let length=0;
 while(true){const {done,value}=await reader.read();if(done)break;length+=value.length;if(length>16384){await reader.cancel();fail('request_too_large',413);}chunks.push(value);}
 const bytes=new Uint8Array(length);let offset=0;for(const value of chunks){bytes.set(value,offset);offset+=value.length;}
 try{const value=JSON.parse(new TextDecoder().decode(bytes));if(!value||typeof value!=='object'||Array.isArray(value))throw Error();return value;}catch{fail('invalid_json');}
}
export function createSubscriptionHandler({env,fetchImpl=fetch,definitions=[],approvedAlertIds,consentFlow,minimumIntervalMs=225,clock=()=>new Date().toISOString(),log=()=>{}}){
 return request=>handleAccessRoute(request,env,fetchImpl,async actor=>{
  const traceId=crypto.randomUUID();
  const respond=(data,status=200)=>{
   try{log({event:'rs_subscription_request',traceId,actorId:actor.id,status,code:data.error||'ok'});}catch{}
   return Response.json(data,{status,headers:{'Cache-Control':'no-store','Vary':'Cookie','X-Content-Type-Options':'nosniff','X-Request-Id':traceId}});
  };
  try{
   if(env.RS_INPUTS_BASE_ID!==BASE_ID)fail('subscription_destination_required',503);
   if(!actor?.profile?.personUid||actor.id!==actor.profile.personUid||!actor.permissions?.includes('inputs:read'))fail('permission_denied',403);
   const identities=createAirtableInputStore({env,fetchImpl,minimumIntervalMs});
   const store=createSubscriptionStore({env,fetchImpl,minimumIntervalMs});
   const resolve=async barnId=>{
    if(!uid(barnId))fail('barn_required');
    const barn=(await identities.list('barn')).find(b=>b.id===barnId&&b.barnId===barnId&&(b.ownerUid===actor.id||actor.barnIds?.includes(b.id)));
    if(!barn)fail('barn_not_found',404);
    const matches=(await identities.list('users')).filter(u=>u.barnId===barn.id&&u.recognitionPersonId===actor.profile.personUid);
    if(matches.length!==1)fail('canonical_user_link_required',409);
    return {barn,user:matches[0]};
   };
   const authorize=(current,barn,user)=>{
    if(!current||current.barn_uid!==barn.id||current.user_uid!==user.id||current.owner_uid!==actor.id)fail('subscription_not_found',404);
    if(current.record_mode!=='Test')fail('test_record_required',409);
   };
   if(request.method==='GET'){
    const {barn,user}=await resolve(new URL(request.url).searchParams.get('barn_id'));
    const records=(await store.list(barn.id,user.id)).filter(r=>r.owner_uid===actor.id&&r.record_mode==='Test');
    return respond({ok:true,subscriptions:records.map(publicRecord)});
   }
   if(request.method!=='POST')fail('method_not_allowed',405);
   if(request.headers.get('Origin')!==new URL(request.url).origin)fail('origin_denied',403);
   if(!actor.permissions.includes('inputs:write'))fail('permission_denied',403);
   const payload=await bodyOf(request);
   const allowed={revoke:['action','barnId','subscriptionId','expectedRevision','requestId'],update:['action','barnId','subscriptionId','expectedRevision','requestId','alertKeys','alertTypes'],grant:['action','barnId','subscriptionId','expectedRevision','requestId','engineScope','phone','alertKeys']};
   if(typeof payload.action!=='string'||!Object.hasOwn(allowed,payload.action)||Object.keys(payload).some(k=>!allowed[payload.action].includes(k)))fail('invalid_payload');
   if(!/^[\w-]{8,128}$/.test(payload.requestId||''))fail('invalid_request_id');
   if(payload.subscriptionId!==undefined&&!uid(payload.subscriptionId))fail('invalid_subscription_id');
   if(payload.action!=='grant'&&!payload.subscriptionId)fail('subscription_required');
   const {barn,user}=await resolve(payload.barnId);
   const eventId=await digest('subscriptions|'+actor.id+'|'+payload.requestId);
   const normalized={action:payload.action,barnId:barn.id,userId:user.id,subscriptionId:payload.subscriptionId||null,expectedRevision:payload.expectedRevision??null,engineScope:payload.engineScope??null,phone:payload.phone??null,alertKeys:payload.alertKeys??null,alertTypes:payload.alertTypes??null};
   const fingerprint=await digest(JSON.stringify(normalized));
   // Actor scope covers both per-subscription updates and the actor-wide audit request key.
   // A barn-only lock permits the same request ID to race across two owned barns.
   return await withSubscriptionReservation(env,{actorId:actor.id,eventId,fingerprint},async guard=>{
    // Revalidate authorization after queue wait.
    const fresh=await resolve(barn.id);
    if(fresh.user.id!==user.id)fail('canonical_user_link_changed',409);
    const id=payload.subscriptionId||'rs_'+(await digest(eventId+'|subscription')).slice(0,32);
    let current=await store.byId(id);
    if(current)authorize(current,barn,user);
    else if(payload.subscriptionId)fail('subscription_not_found',404);
    const replay=await store.event(eventId);
    if(replay){
     if(replay.inputHash!==fingerprint)fail('request_id_reused',409);
     if(replay.record.id!==id||replay.record.barnId!==barn.id||replay.record.userId!==user.id||replay.record.ownerUid!==actor.id)fail('invalid_audit_record',502);
     if(!current)fail('subscription_not_found',404);
     return respond({ok:true,subscription:publicRecord(current),replayed:true});
    }
    const audit=async record=>{
     guard.markWriteAttempted();
     const occurredAt=clock();if(!Number.isFinite(Date.parse(occurredAt)))fail('invalid_server_time',503);
     const safe={id:record.entity_uid,barnId:record.barn_uid,userId:record.user_uid,ownerUid:record.owner_uid,revision:record.revision,status:record.status,consentState:record.consent_state,consentAt:record.consent_at||null,consentSource:record.consent_source||null,noticeVersion:record.notice_version||null,revokedAt:record.revoked_at||null};
     try{await store.appendEvent({eventId,actorId:actor.id,barnId:barn.id,requestId:payload.requestId,inputHash:fingerprint,record:safe,occurredAt,action:payload.action==='grant'?'consent_granted':payload.action==='revoke'?'consent_revoked':'preferences_updated'});}catch{fail('write_outcome_unknown',503);}
    };
    if(current?.request_uid===eventId){
     if(current.request_hash!==fingerprint)fail('request_id_reused',409);
     await audit(current);
     return respond({ok:true,subscription:publicRecord(current),auditRepaired:true});
    }
    if(guard.recoveryOnly)fail('subscription_reconciliation_required',503);
    if(current&&/^[a-f0-9]{64}$/.test(current.request_uid)){
     const prior=await store.event(current.request_uid);
     if(!prior)fail('prior_audit_pending',409);
     if(prior.inputHash!==current.request_hash||prior.record.id!==current.entity_uid||prior.record.barnId!==current.barn_uid||prior.record.userId!==current.user_uid||prior.record.ownerUid!==current.owner_uid||prior.record.revision!==current.revision)fail('invalid_audit_record',502);
    }
    if(current?payload.expectedRevision!==current.revision:payload.expectedRevision!==undefined)fail('record_changed',409);
    const at=clock();if(!Number.isFinite(Date.parse(at)))fail('invalid_server_time',503);
    let next;
    const alertIds=engine=>{
     if(!Array.isArray(payload.alertKeys)||!payload.alertKeys.length||new Set(payload.alertKeys).size!==payload.alertKeys.length)fail('alert_selection_required');
     return payload.alertKeys.map(key=>{
      const def=definitions.find(d=>d.key===key);if(!def)fail('unknown_alert');
      if(def.input)fail('parameter_storage_unmapped',409);
      const id=approvedAlertIds?.[engine]?.[key];
      if(typeof id!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(id))fail('approved_engine_alert_mapping_required',409);
      return id;
     }).sort().join('\n');
    };
    if(payload.action==='revoke'){
     next={...current,status:'Revoked',consent_state:'Revoked',consent_at:at,consent_source:'rs-inputs/subscriptions:explicit-revoke:v1',revoked_at:at};
    }else if(payload.action==='update'){
     let selected;
     if(payload.alertTypes!==undefined){
      const existing=current.alert_types.split('\n').filter(Boolean);
      if(payload.alertKeys!==undefined||!Array.isArray(payload.alertTypes)||new Set(payload.alertTypes).size!==payload.alertTypes.length)fail('invalid_payload');
      if(payload.alertTypes.some(id=>typeof id!=='string'||!existing.includes(id)))fail('existing_alert_subset_required',409);
      // Exact existing identifiers only. No engine/catalog inference, new selections,
      // parameter parsing or silent rewriting of preferences not explicitly removed.
      selected=existing.filter(id=>payload.alertTypes.includes(id)).join('\n');
     }else selected=alertIds(current.engine_scope);
     next={...current,alert_types:selected};
     if(next.status==='Active')fail('subscription_activation_not_authorized',409);
    }else{
     if(!consentFlow?.approved||!consentFlow.source||!consentFlow.noticeVersion)fail('approved_consent_flow_required',409);
     const engine=payload.engineScope||current?.engine_scope;
     if(!['WEC','WEF'].includes(engine))fail('canonical_engine_required',409);
     if(current&&(current.engine_scope!==engine||current.target_type!=='Barn'||current.target_uid!==barn.id))fail('subscription_scope_changed',409);
     if(!/^\+[1-9]\d{7,14}$/.test(payload.phone||''))fail('e164_phone_required');
     const alerts=alertIds(engine);
     if(!current&&(await store.list(barn.id,user.id)).some(r=>r.engine_scope===engine&&r.target_type==='Barn'&&r.target_uid===barn.id))fail('subscription_scope_exists',409);
     next={...(current||{}),entity_uid:id,barn_uid:barn.id,user_uid:user.id,engine_scope:engine,target_type:'Barn',target_uid:barn.id,alert_types:alerts,phone_e164:payload.phone,status:'Draft',consent_state:'Granted',consent_at:at,consent_source:consentFlow.source,notice_version:consentFlow.noticeVersion,owner_uid:actor.id,record_mode:'Test'};
    }
    next={...next,revision:(current?.revision||0)+1,request_uid:eventId,request_hash:fingerprint};
    guard.markWriteAttempted();
    const saved=await store.put(next,current?.revision);
    await audit(saved);
    return respond({ok:true,subscription:publicRecord(saved)});
   });
  }catch(error){return respond({ok:false,error:error.code||'storage_unavailable'},Number.isInteger(error.status)?error.status:502);}
 },log);
}
