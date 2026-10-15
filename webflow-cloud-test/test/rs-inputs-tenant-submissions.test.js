import test,{afterEach} from 'node:test';
import assert from 'node:assert/strict';
import {subscriptionControlDatabase} from '../test-support/subscription-control-db.mjs';
import {subscriptionAlertDefinitions as definitions} from '../src/lib/rs-input-subscription-alerts.js';
import {withSubscriptionReservation} from '../src/lib/rs-input-subscription-control.js';
import {digest} from '../src/lib/rs-inputs.js';
const m=await import('../src/lib/rs-tenant-submissions.js').catch(()=>({}));
const dbs=[];afterEach(()=>dbs.splice(0).forEach(d=>d.close()));
const actor={id:'this_sms_test',profile:{personUid:'this_sms_test'},permissions:['inputs:read','inputs:write']};
const preferences=()=>({version:1,enabled:false,phone:'+12025550123',timeZone:'America/New_York',variables:Object.fromEntries(definitions.map(d=>[d.key,{enabled:false,value:''}]))});
async function fixture(){
 const db=subscriptionControlDatabase();dbs.push(db);const env={RS_INPUTS_TENANT_SUBMISSIONS_MODE:'isolated-trial',RS_INPUTS_WRITE_MODE:'isolated-trial',RS_INPUTS_SESSION_SECRET:'ab'.repeat(32),RS_RECOGNITION_CONTROL_DB:db};
 const entity='rs_'+(await digest('sms-preferences|this_sms_test')).slice(0,32);
 const target={id:'recGOGga4HDUSIZXL',fields:{entity_uid:entity,owner_uid:actor.id,target_uid:actor.id,target_type:'Person',record_mode:'Test',status:'Revoked',revision:2,request_uid:'a'.repeat(64),request_hash:'b'.repeat(64),preferences_json:JSON.stringify(preferences())}};
 let submission=null,event=null,creates=0;const updates=[];
 const store={async submission(id){return submission?.fields['Submission ID']===id?structuredClone(submission):null},async create(envelope){creates++;submission={id:'recSynthetic',fields:{'Submission ID':envelope.eventId,'Actor ID':actor.id,'Target table ID':envelope.targetTableId,'Target record ID':envelope.targetRecordId,Action:'Update','Processing status':'Pending','Submitted changes':JSON.stringify(envelope)}};return structuredClone(submission)},async setStatus(id,status,error){updates.push({status,error});submission.fields['Processing status']=status;submission.fields['Error / review note']=error;},async target(){return structuredClone(target)},async event(id){if(id===target.fields.request_uid&&!event)return {fields:{event_uid:id,input_hash:target.fields.request_hash,result_json:JSON.stringify({id:entity,ownerUid:actor.id,targetUid:actor.id,targetType:'Person',revision:2})}};return structuredClone(event)},async options(){return definitions.map(d=>({fields:{'Opt-in':[target.id],Option:d.key,Choice:'Off',Value:''}}))}};
 const bridge=()=>m.createTenantSubmissionBridge({env,store,definitions,clock:()=> '2026-10-10T12:00:00.000Z'});
 return {env,store,target,bridge,get submission(){return submission},get creates(){return creates},updates,set event(v){event=v},async commit(){
  const e=JSON.parse(submission.fields['Submitted changes']),p=e.preferences;
  const result={ok:true,id:entity,ownerUid:actor.id,targetUid:actor.id,targetType:'Person',requestId:e.requestId,actorId:e.actorId,revision:3,status:'Revoked',consentState:'Revoked',consentAt:e.acceptedAt,consentSource:e.source,noticeVersion:e.noticeVersion,revokedAt:e.acceptedAt,preferences:p};
  Object.assign(target.fields,{revision:3,request_uid:e.eventId,request_hash:e.inputHash,preferences_json:JSON.stringify(p),phone_e164:p.phone,status:'Revoked',consent_state:'Revoked',consent_at:e.acceptedAt,consent_source:e.source,notice_version:e.noticeVersion,revoked_at:e.acceptedAt});
  event={fields:{event_uid:e.eventId,input_hash:e.inputHash,request_uid:e.requestId,actor_uid:e.actorId,entity_uid:entity,kind:'subscriptions',outcome:'committed',record_mode:'Test',action:'consent_revoked',occurred_at:e.acceptedAt,result_json:JSON.stringify(result)}};return {e,event,result};
 },payload:{action:'preferences',requestId:'synthetic_request_01',expectedRevision:2,preferences:preferences()}};
}
test('async bridge is available',()=>assert.equal(typeof m.createTenantSubmissionBridge,'function'));
test('intake remains Pending, stable replay creates once, changed hash and competing operation denied',async()=>{
 const f=await fixture(),b=f.bridge();const first=await b.intake({actor,payload:f.payload});assert.equal(first.state,'Pending');assert.equal(first.preferences,undefined);
 assert.deepEqual(await b.intake({actor,payload:f.payload}),first);assert.equal(f.creates,1);
 await assert.rejects(b.intake({actor,payload:{...f.payload,preferences:{...f.payload.preferences,phone:'+12025550124'}}}),{code:'request_id_reused'});
 await assert.rejects(b.intake({actor,payload:{...f.payload,requestId:'another_request_02'}}),{code:'subscription_request_in_progress'});
});
test('only one authenticated immutable claim runs; forged or edited envelopes fail closed',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']);const capability={eventId:e.eventId,...e.admission};
 await assert.rejects(b.claim({capability:{...capability,signature:'0'.repeat(64)}}),{code:'invalid_admission'});
 const outcomes=await Promise.allSettled([b.claim({capability}),b.claim({capability})]);assert.equal(outcomes.filter(r=>r.status==='fulfilled').length,1);assert.equal(outcomes.find(r=>r.status==='fulfilled').value.state,'Claimed');
 f.submission.fields['Submitted changes']=JSON.stringify({...e,preferences:{...e.preferences,phone:'+12025550124'}});
 await assert.rejects(b.claim({capability}),{code:'invalid_admission'});
});
test('partial outcome retains reservation and completion without matching target/options/event is refused',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission};const claim=await b.claim({capability});
 await assert.rejects(b.complete({capability,claimToken:claim.claimToken}),{code:'operation_result_unavailable'});
 await assert.rejects(b.intake({actor,payload:{...f.payload,requestId:'another_request_02'}}),{code:'subscription_request_in_progress'});
 const row=await f.env.RS_RECOGNITION_CONTROL_DB.prepare('SELECT state FROM subscription_write_reservations WHERE actor_uid=?').bind(actor.id).first();assert.equal(row.state,'working');
});


test('committed event alone is Pending until independently verified completion releases exact reservation',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission};const claim=await b.claim({capability});await f.commit();
 assert.equal((await b.status({actor,requestId:e.requestId})).state,'Pending');assert.equal((await b.intake({actor,payload:f.payload})).state,'Pending');
 const saved=await b.complete({capability,claimToken:claim.claimToken});assert.equal(saved.state,'Applied');assert.deepEqual(saved.preferences,e.preferences);assert.equal((await b.status({actor,requestId:e.requestId})).state,'Applied');
 assert.deepEqual(await b.complete({capability,claimToken:claim.claimToken}),saved);
});
test('pending admission cannot be acquired by existing synchronous reservation recovery',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']);let called=false;
 await assert.rejects(withSubscriptionReservation(f.env,{actorId:actor.id,eventId:e.eventId,fingerprint:e.inputHash},async()=>{called=true}),{code:'subscription_request_in_progress'});assert.equal(called,false);
 const row=await f.env.RS_RECOGNITION_CONTROL_DB.prepare('SELECT worker_token FROM subscription_write_reservations WHERE actor_uid=?').bind(actor.id).first();assert.equal(row.worker_token,e.admission.token);
});
test('audit consent evidence must equal the admitted exact outcome',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission};const claim=await b.claim({capability});const committed=await f.commit();
 committed.result.consentSource='forged-source';committed.event.fields.result_json=JSON.stringify(committed.result);f.event=committed.event;
 await assert.rejects(b.complete({capability,claimToken:claim.claimToken}),{code:'operation_result_unavailable'});assert.equal(f.updates.length,0);
});

test('lost completion receipt write blocks next operation; same completion safely finalizes logs only',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission};const claim=await b.claim({capability});await f.commit();const original=f.store.setStatus;f.store.setStatus=async()=>{throw Error('lost receipt write')};
 await assert.rejects(b.complete({capability,claimToken:claim.claimToken}),/lost receipt write/);
 await assert.rejects(b.intake({actor,payload:{...f.payload,requestId:'another_request_02',expectedRevision:3}}),{code:'subscription_request_in_progress'});
 f.store.setStatus=original;assert.equal((await b.complete({capability,claimToken:claim.claimToken})).state,'Applied');assert.match(f.submission.fields['Error / review note'],/^rs-tenant-completed-v1:/);
 // A later D1 operation cannot erase the historical server completion proof.
 await f.env.RS_RECOGNITION_CONTROL_DB.prepare("UPDATE subscription_write_reservations SET request_uid=?,request_hash=?,state='working' WHERE actor_uid=?").bind('d'.repeat(64),'e'.repeat(64),actor.id).run();assert.equal((await b.status({actor,requestId:e.requestId})).state,'Applied');
});
test('lost intake outcome stays reserved without a blind second append',async()=>{
 const f=await fixture(),b=f.bridge();let calls=0;f.store.create=async()=>{calls++;throw Error('lost intake')};await assert.rejects(b.intake({actor,payload:f.payload}),/lost intake/);await assert.rejects(b.intake({actor,payload:f.payload}),{code:'write_outcome_unknown'});assert.equal(calls,1);
});
test('bad option readback and unsigned completion status cannot produce Saved',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission};const claim=await b.claim({capability});await f.commit();f.store.options=async()=>[];
 await assert.rejects(b.complete({capability,claimToken:claim.claimToken}),{code:'options_readback_mismatch'});f.submission.fields['Processing status']='Applied';await assert.rejects(b.status({actor,requestId:e.requestId}),{code:'operation_result_unavailable'});
});
test('admission rejects a working row snapshot even if completion releases before acquire',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});await f.commit();const db=f.env.RS_RECOGNITION_CONTROL_DB,prepare=db.prepare.bind(db);let race=true;
 db.prepare=sql=>({bind(...values){const query=prepare(sql).bind(...values);return {...query,async first(){const row=await query.first();if(race&&sql.startsWith('SELECT * FROM subscription_write_reservations')){race=false;await prepare("UPDATE subscription_write_reservations SET state='idle' WHERE actor_uid=?").bind(actor.id).run();}return row}}}});
 await assert.rejects(b.intake({actor,payload:{...f.payload,requestId:'interleaving_request_02',expectedRevision:3}}),{code:'subscription_request_in_progress'});assert.equal(f.creates,1);
});
test('idle admission CAS cannot overwrite a newer completed reservation row after validation',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission},claim=await b.claim({capability});await f.commit();await b.complete({capability,claimToken:claim.claimToken});
 const db=f.env.RS_RECOGNITION_CONTROL_DB,prepare=db.prepare.bind(db);let race=true;
 db.prepare=sql=>({bind(...values){const query=prepare(sql).bind(...values);return {...query,async run(){if(race&&(sql.startsWith('INSERT INTO subscription_write_reservations')||sql.includes('SET request_uid=?'))){race=false;await prepare("UPDATE subscription_write_reservations SET request_uid=?,request_hash=?,worker_token=?,state='idle' WHERE actor_uid=?").bind('c'.repeat(64),'d'.repeat(64),'new-completed-token',actor.id).run();}return query.run()}}}});
 await assert.rejects(b.intake({actor,payload:{...f.payload,requestId:'interleaving_request_02',expectedRevision:3}}),{code:'subscription_request_in_progress'});assert.equal(f.creates,1);
});
for(const blankPhone of [false,true])test('receipt storage retains working ownership and Pending until exact release; blankPhone='+blankPhone,async()=>{
 const f=await fixture(),b=f.bridge();if(blankPhone)f.payload.preferences.phone='';await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission},claim=await b.claim({capability});await f.commit();if(blankPhone)delete f.target.fields.phone_e164;
 const original=f.store.setStatus;f.store.setStatus=async(...args)=>{const row=await f.env.RS_RECOGNITION_CONTROL_DB.prepare('SELECT state FROM subscription_write_reservations WHERE actor_uid=?').bind(actor.id).first();assert.equal(row.state,'working');await original(...args);assert.equal((await b.status({actor,requestId:e.requestId})).state,'Pending');await assert.rejects(withSubscriptionReservation(f.env,{actorId:actor.id,eventId:e.eventId,fingerprint:e.inputHash},async()=>assert.fail('legacy cannot enter')), {code:'subscription_request_in_progress'});};
 assert.equal((await b.complete({capability,claimToken:claim.claimToken})).state,'Applied');
});
test('receipt persisted but release unavailable stays Pending; same claim completes release without reapplying',async()=>{
 const f=await fixture(),b=f.bridge();await b.intake({actor,payload:f.payload});const e=JSON.parse(f.submission.fields['Submitted changes']),capability={eventId:e.eventId,...e.admission},claim=await b.claim({capability});await f.commit();const db=f.env.RS_RECOGNITION_CONTROL_DB,prepare=db.prepare.bind(db);let lose=true;
 db.prepare=sql=>({bind(...values){const query=prepare(sql).bind(...values);return {...query,async run(){if(lose&&sql.includes("SET state='idle'")){lose=false;throw Error('release unavailable')}return query.run()}}}});
 await assert.rejects(b.complete({capability,claimToken:claim.claimToken}),/release unavailable/);assert.match(f.submission.fields['Error / review note'],/^rs-tenant-completed-v1:/);assert.equal((await b.status({actor,requestId:e.requestId})).state,'Pending');
 const replay=await b.claim({capability});assert.equal(replay.state,'Replayed');assert.equal(replay.claimToken,claim.claimToken);assert.equal((await b.complete({capability,claimToken:replay.claimToken})).state,'Applied');assert.equal(f.updates.length,1);assert.equal(f.target.fields.revision,3);
});
