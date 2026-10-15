import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import {subscriptionControlDatabase} from './subscription-control-db.mjs';
import {digest} from '../src/lib/rs-inputs.js';
import {subscriptionAlertDefinitions as definitions} from '../src/lib/rs-input-subscription-alerts.js';
import {createSubscriptionHandler} from '../src/lib/rs-input-subscriptions.js';
import {createTenantSubmissionStore,TABLES} from '../src/lib/rs-tenant-submission-store.js';
import {createTenantSubmissionBridge} from '../src/lib/rs-tenant-submissions.js';
import {handleTenantSubmissionClaim} from '../src/pages/rs-inputs/tenant-submission-claim.js';
import {handleNativeRecognition} from '../src/lib/rs-recognition-native.js';
import {handleAuthenticatedInputRoute} from '../src/pages/rs-inputs/[operation].js';
import {createOtpStore} from '../src/lib/rs-recognition-otp-store.js';
import {TABLE} from '../src/lib/rs-tenant-barn.js';
import {processTenantBarn} from '../src/pages/rs-inputs/tenant-barn-process.js';

// Local stand-ins only for Airtable storage, Cloudflare D1 and SMS delivery.
// Application handlers, validation, cookie issuance and processor are real source.
export async function recognizeTenantsFixture(){
 const db=subscriptionControlDatabase(),actor='person_fixture',origin='https://ringstatus.com';
 const env={AIRTABLE_TOKEN:'synthetic',RS_INPUTS_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_RECOGNITION_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_WRITE_MODE:'isolated-trial',RS_INPUTS_SESSION_SECRET:'ab'.repeat(32),RS_RECOGNITION_CONTROL_DB:db,RS_RECOGNITION_SMS_QUEUE_MODE:'ready-gated',RS_RECOGNITION_SMS_RECORD_MODE:'test',RS_RECOGNITION_SMS_TEST_RECIPIENT:'+16318752160'};
 const recognition=['tbly1PM5iFYqVzKSm','tblfkRSJAEMzuzApR','tblgDWKi0Bb6OcoqS','tblWjbASVMIjFLyW8','tblBgvP7pYSv8o20j','tblxC4SYtzYhJNcJS','tblF1Hdqi3yoKTZPV','tblRvTwo3HYPUkZou','tblYgoeLEey05xgw9','tblnd2ToLs7dTzLAM','tblpyyaOMgjLzLvkP','tblEuwOr1rUKnj1j3','tblwts3huk3w1ACjh'];
 const tenantTables=[...new Set([...Object.values(TABLES),...Object.values(TABLE)])];
 const rows=Object.fromEntries([...recognition,...tenantTables].map(id=>[id,[]])),writes=[];
 let seq=0,code=null;
 const insert=(table,fields)=>{const row={id:'rec'+String(++seq).padStart(14,'0'),fields:structuredClone(fields)};rows[table].push(row);return row};
 const person=insert(recognition[0],{person_uid:actor,person_name:'Fixture Owner',first_name:'Fixture',primary_phone_e164:'+16318752160',input_access:'approved',status:'Test',input_session_version:'cd'.repeat(16)});
 const barnId='rs_'+'a'.repeat(32);
 insert('tblRvTwo3HYPUkZou',{entity_uid:barnId,barn_uid:barnId,name:'Fixture Barn',owner_uid:actor,revision:1});
 const preferences={version:1,enabled:false,phone:'+16318752160',timeZone:'America/New_York',variables:Object.fromEntries(definitions.map(d=>[d.key,{enabled:false,value:''}]))};
 const entity='rs_'+(await digest('sms-preferences|'+actor)).slice(0,32);
 const target=insert(TABLES.target,{entity_uid:entity,owner_uid:actor,target_uid:actor,target_type:'Person',record_mode:'Test',status:'Revoked',consent_state:'Revoked',consent_at:'2026-10-09T16:44:16.668Z',revoked_at:'2026-10-09T16:44:16.668Z',revision:2,request_uid:'a'.repeat(64),request_hash:'b'.repeat(64),preferences_json:JSON.stringify(preferences)});
 const prior=insert(TABLES.event,{event_uid:target.fields.request_uid,input_hash:target.fields.request_hash,actor_uid:actor,entity_uid:entity,kind:'subscriptions',outcome:'committed',record_mode:'Test',occurred_at:'2026-10-09T16:44:17.277Z',action:'consent_revoked',result_json:JSON.stringify({id:entity,ownerUid:actor,targetUid:actor,targetType:'Person',revision:2})});
 definitions.forEach(d=>insert(TABLES.option,{'Opt-in':[target.id],Option:d.key,Choice:'Off',Value:''}));
 const original=structuredClone(prior);
 const fetchImpl=async(input,init={})=>{
  const url=new URL(input);assert.equal(url.hostname,'api.airtable.com','external network prohibited');
  const [,base,table,id]=url.pathname.split('/').filter(Boolean);assert.ok(rows[table],table);assert.equal(base,tenantTables.includes(table)?'app7BXcw801n19QKF':'app9kOZdIaGyKk5uG');
  const method=init.method||'GET';let values=rows[table];
  if(method==='GET'){
   if(id)return Response.json(values.find(r=>r.id===id)||{}, {status:values.some(r=>r.id===id)?200:404});
   const formula=url.searchParams.get('filterByFormula');
   if(formula){const terms=[...formula.matchAll(/\{([^}]+)\}\s*=\s*'([^']*)'/g)];assert.ok(terms.length,formula);values=values.filter(r=>formula.startsWith('OR(')?terms.some(([,k,v])=>r.fields[k]===v):terms.every(([,k,v])=>r.fields[k]===v));}
   return Response.json({records:structuredClone(values)});
  }
  assert.ok(['PATCH','POST'].includes(method));const body=JSON.parse(init.body),batch=body.records||[{id,fields:body.fields}];
  const changed=batch.map(item=>{let row=item.id?values.find(r=>r.id===item.id):null;
   if(!row&&body.performUpsert)row=values.find(r=>body.performUpsert.fieldsToMergeOn.every(k=>r.fields[k]===item.fields[k]));
   if(!row)row=insert(table,{});Object.assign(row.fields,structuredClone(item.fields));writes.push({table,id:row.id});return row;});
  return Response.json(body.records?{records:structuredClone(changed)}:structuredClone(changed[0]));
 };
 const tenantEnv={...env,RS_INPUTS_TENANT_SUBMISSIONS_MODE:'isolated-trial'};
 const bridge=()=>createTenantSubmissionBridge({env:tenantEnv,definitions,store:createTenantSubmissionStore({env:tenantEnv,fetchImpl,minimumIntervalMs:0})});
 const subscriptions=createSubscriptionHandler({env,fetchImpl,definitions,minimumIntervalMs:0});
 const selects=new Set(['Status','status','consent_state','target_type','record_mode','engine_scope','Action','Processing status','Choice']);
 const links=new Set(['Opt-in','Person','Tenant','Roles','Rider','Horse','people','Membership']);
 const normalize=fields=>Object.fromEntries(Object.entries(fields).map(([k,v])=>[k,links.has(k)?v.map(x=>x.id):selects.has(k)?v?.name??v:v]));
 const wrap=row=>row&&({id:row.id,getCellValue(k){const v=row.fields[k];return v===undefined||v===''?null:links.has(k)?v.map(id=>({id})):selects.has(k)?{name:v}:structuredClone(v)}});
 const base={getTable:table=>{assert.ok(rows[table]);return {selectRecordAsync:async id=>wrap(rows[table].find(r=>r.id===id)),selectRecordsAsync:async()=>({records:rows[table].map(wrap)}),createRecordAsync:async fields=>{writes.push({table});return insert(table,normalize(fields)).id},updateRecordAsync:async(id,fields)=>{const row=rows[table].find(r=>r.id===id);assert.ok(row);writes.push({table,id});Object.assign(row.fields,normalize(fields));}}}};
 async function processPreferences(row){
  const processor=await readFile(new URL('../scripts/tenant-preferences-processor.cjs',import.meta.url),'utf8'),outputs={};
  await vm.runInNewContext('(async()=>{'+processor+'})()',{base,input:{config:()=>({recordId:row.id})},output:{set:(k,v)=>outputs[k]=v},TextEncoder,Uint32Array,fetch:async(url,init)=>{assert.equal(url,origin+'/test/rs-inputs/tenant-submission-claim');return handleTenantSubmissionClaim(new Request(url,init),env,bridge());}});
  assert.equal(outputs.status,'Applied',JSON.stringify(outputs));assert.deepEqual(prior,original,'historical consent event changed');return outputs;
 }
 async function processBarn(row){
  const processor=await readFile(new URL('../scripts/tenant-barn-processor.js',import.meta.url),'utf8');
  await vm.runInNewContext('(async()=>{'+processor+'})()',{base,input:{config:()=>({recordId:row.id})},output:{set(){}},fetch:async(url,init)=>{assert.equal(url,origin+'/test/rs-inputs/tenant-barn-process');return processTenantBarn(new Request(url,init),env,fetchImpl);}});
  assert.equal(row.fields['Processing status'],'Applied');
 }
 async function dispatch(request){
  const url=new URL(request.url);
  if(url.pathname.endsWith('/native-recognition')){
   const payload=request.method==='POST'?await request.clone().json():null;
   const response=await handleNativeRecognition(request,env,fetchImpl);
   const result=await response.clone().json();
   if(payload?.action==='otp_start'&&result.otp_request_id)code=await createOtpStore(env).prepare(result.otp_request_id,'+16318752160');
   return response;
  }
  if(url.pathname.endsWith('/subscriptions'))return subscriptions(request);
  return handleAuthenticatedInputRoute({request},env,fetchImpl);
 }
 return {db,env,rows,insert,actor,person,barnId,preferences,target,prior,original,writes,dispatch,processPreferences,processBarn,get code(){return code}};
}
