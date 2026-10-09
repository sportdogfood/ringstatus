import { InputError } from './rs-inputs.js';
export const BASE_ID='app9kOZdIaGyKk5uG';
export const SUBSCRIPTIONS='tblbpALv7flu3NNEE';
export const EVENTS='tblwts3huk3w1ACjh';
const names=['entity_uid','barn_uid','user_uid','engine_scope','target_type','target_uid','alert_types','phone_e164','status','consent_state','consent_at','consent_source','notice_version','revoked_at','owner_uid','revision','request_uid','request_hash','record_mode','preferences_json'];
const fail=(code,status=400)=>{throw new InputError(code,status)};
const uid=v=>typeof v==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(v);
const date=v=>typeof v==='string'&&Number.isFinite(Date.parse(v));
const eq=(key,value)=>{if(!uid(value))fail('invalid_identity');return '{'+key+"} = '"+value+"'";};
export function validateSubscription(row,{writing=false}={}){
 if(!row||Object.keys(row).some(k=>!names.includes(k)))fail('invalid_subscription_record',502);
 const personal=row.target_type==='Person'&&typeof row.preferences_json==='string';
 for(const key of personal?['entity_uid','target_uid','owner_uid']:['entity_uid','barn_uid','user_uid','target_uid','owner_uid'])if(!uid(row[key]))fail('invalid_subscription_record',502);
 if(personal&&(row.target_uid!==row.owner_uid||row.barn_uid||row.user_uid||row.engine_scope||row.status==='Active'))fail('invalid_subscription_record',502);
 if((!personal&&!['WEC','WEF'].includes(row.engine_scope))||!['Barn','Person','Horse','Show','Ring'].includes(row.target_type)||!['Draft','Active','Paused','Revoked'].includes(row.status)||!['Unknown','Granted','Revoked'].includes(row.consent_state)||!['Test','Live'].includes(row.record_mode)||!Number.isSafeInteger(row.revision)||row.revision<1)fail('invalid_subscription_record',502);
 if(typeof row.alert_types!=='string'||row.alert_types.length>8192||typeof row.request_uid!=='string'||!row.request_uid||!/^[a-f0-9]{64}$/.test(row.request_hash||''))fail('invalid_subscription_record',502);
 if(['Granted','Revoked'].includes(row.consent_state)&&(!date(row.consent_at)||typeof row.consent_source!=='string'||!row.consent_source))fail('consent_evidence_required',409);
 if(row.consent_state==='Revoked'&&(!date(row.revoked_at)||row.status!=='Revoked'))fail('revocation_evidence_required',409);
 if(row.status==='Active'&&(!/^\+[1-9]\d{7,14}$/.test(row.phone_e164||'')||row.consent_state!=='Granted'||!row.alert_types))fail('active_consent_required',409);
 if(writing&&(row.record_mode!=='Test'||row.status==='Active'))fail('subscription_activation_not_authorized',409);
 return row;
}
export function createSubscriptionStore({env,fetchImpl=fetch,minimumIntervalMs=225}){
 if(env.RS_INPUTS_BASE_ID!==BASE_ID)fail('subscription_destination_required',503);
 if(!env.AIRTABLE_TOKEN)fail('storage_credentials_missing',503);
 let last=0;
 async function call(table,{method='GET',formula,body}={}){
  if(![SUBSCRIPTIONS,EVENTS].includes(table))fail('invalid_table',503);
  const url=new URL('https://api.airtable.com/v0/'+BASE_ID+'/'+table);
  if(formula)url.searchParams.set('filterByFormula',formula);
  const all=[],seen=new Set();
  do{
   const delay=Math.max(0,minimumIntervalMs-(Date.now()-last));if(delay)await new Promise(r=>setTimeout(r,delay));last=Date.now();
   let data;
   try{
    const response=await fetchImpl(url,{method,redirect:'manual',headers:{Authorization:'Bearer '+env.AIRTABLE_TOKEN,'Content-Type':'application/json'},signal:AbortSignal.timeout(15000),...(body?{body:JSON.stringify(body)}:{})});
    if(!response.ok)throw Error();data=await response.json();if(!Array.isArray(data?.records))throw Error();
   }catch{fail(method==='GET'?'storage_unavailable':'write_outcome_unknown',503);}
   all.push(...data.records);
   if(!data.offset||method!=='GET')break;
   if(typeof data.offset!=='string'||seen.has(data.offset))fail('invalid_storage_response',502);
   seen.add(data.offset);url.searchParams.set('offset',data.offset);
  }while(true);
  return all;
 }
 function writable(){if(env.RS_INPUTS_WRITE_MODE!=='isolated-trial')fail('subscription_write_mode_required',503);}
 function decode(row){
  // Empty Airtable text values are omitted; an empty selection is allowed until activation.
  const fields=Object.fromEntries(Object.entries(row.fields||{}).filter(([key])=>names.includes(key)));
  return validateSubscription({alert_types:'',...fields});
 }
 async function byId(id){const rows=await call(SUBSCRIPTIONS,{formula:eq('entity_uid',id)});if(rows.length>1)fail('ambiguous_subscription',409);return rows[0]?decode(rows[0]):null;}
 return {
  capabilities:{atomicCompareAndSwap:false,atomicAudit:false,usage:'isolated-trial-only'},
  async list(barnId,userId){
   const rows=(await call(SUBSCRIPTIONS,{formula:'AND('+eq('barn_uid',barnId)+','+eq('user_uid',userId)+')'})).map(decode);
   if(new Set(rows.map(r=>r.entity_uid)).size!==rows.length)fail('ambiguous_subscription',409);
   return rows;
  },
  byId,
  async put(record,expectedRevision){
   writable();validateSubscription(record,{writing:true});
   const current=await byId(record.entity_uid);
   if(current?current.revision!==expectedRevision:expectedRevision!==undefined)fail('record_changed',409);
   if(record.revision!==(current?.revision||0)+1)fail('record_changed',409);
   if(current&&['barn_uid','user_uid','engine_scope','target_type','target_uid','owner_uid','record_mode'].some(k=>current[k]!==record[k]))fail('immutable_subscription_scope',409);
   // Provider read + upsert is NOT CAS. Caller must hold a qualified durable exclusive scope.
   const rows=await call(SUBSCRIPTIONS,{method:'PATCH',body:{performUpsert:{fieldsToMergeOn:['entity_uid']},records:[{fields:record}]}});
   if(rows.length!==1)fail('write_outcome_unknown',503);
   const saved=decode(rows[0]);
    if(Object.entries(record).some(([k,v])=>record.preferences_json?(saved[k]??'')!==(v??''):saved[k]!==v))fail('write_outcome_unknown',503);
   return saved;
  },
  async event(eventId){
   const rows=await call(EVENTS,{formula:eq('event_uid',eventId)});
   if(rows.length>1)fail('ambiguous_audit_event',409);if(!rows.length)return null;
   const fields=rows[0].fields;
   try{
    const record=JSON.parse(fields.result_json);
    const personal=record.targetType==='Person'&&record.targetUid===record.ownerUid;
    if(!uid(record.id)||(!personal&&(!uid(record.barnId)||!uid(record.userId)))||!uid(record.ownerUid)||!Number.isSafeInteger(record.revision)||!/^[a-f0-9]{64}$/.test(fields.input_hash||'')||fields.outcome!=='committed'||fields.record_mode!=='Test')throw Error();
    if(fields.event_uid!==eventId||fields.entity_uid!==record.id||fields.actor_uid!==record.ownerUid||(fields.barn_uid||null)!==(record.barnId||null)||fields.kind!=='subscriptions'||!date(fields.occurred_at)||!['consent_granted','consent_revoked','preferences_updated'].includes(fields.action))throw Error();
    return {inputHash:fields.input_hash,record};
   }catch{fail('invalid_audit_record',502);}
  },
  async appendEvent(event){
   writable();
   const fields={event_uid:event.eventId,actor_uid:event.actorId,entity_uid:event.record.id,barn_uid:event.barnId,action:event.action,kind:'subscriptions',request_uid:event.requestId,input_hash:event.inputHash,result_json:JSON.stringify(event.record),occurred_at:event.occurredAt,outcome:'committed',record_mode:'Test'};
   const rows=await call(EVENTS,{method:'PATCH',body:{performUpsert:{fieldsToMergeOn:['event_uid']},records:[{fields}]}});
   if(rows.length!==1||Object.entries(fields).some(([k,v])=>(rows[0].fields?.[k]??null)!==(v??null)))fail('write_outcome_unknown',503);
  }
 };
}
