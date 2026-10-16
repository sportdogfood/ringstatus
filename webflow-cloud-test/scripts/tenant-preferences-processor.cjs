// OFF preparation. Pinned capability verifier route must be deployed before activation.
// The test harness rewrites this literal inside an isolated VM only.
const SYNTHETIC_VM_ONLY = false;
const TABLES={sub:'tblldMTDEC697hPwE',target:'tblCX61u6pjSLrP5S',option:'tblFHcfIFjGuoE2hp',event:'tbluecPFIorOytASP'};
const CLAIM_URL='https://ringstatus.com/test/rs-inputs/tenant-submission-claim';
const fail=(code,status='Failed')=>{const e=new Error(code);e.code=code;e.processingStatus=status;throw e;};
const review=code=>fail(code,'Needs review');
const select=v=>v?.name??v??'';
const cell=(r,k)=>r?.getCellValue(k);
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
// Portable SHA-256 adapter for the baseline digest contract (UTF-8, lowercase hex).
function digest(value){
 const bytes=Array.from(new TextEncoder().encode(value)),bits=bytes.length*8;
 bytes.push(128);while(bytes.length%64!==56)bytes.push(0);
 for(let i=7;i>=0;i--)bytes.push(i>=4?0:(bits>>>((i%4)*8))&255);
 const primes=[];for(let p=2;primes.length<64;p++){let prime=true;for(let d=2;d*d<=p;d++)if(p%d===0){prime=false;break;}if(prime)primes.push(p);}
 const h=primes.slice(0,8).map(p=>(Math.sqrt(p)%1*4294967296)>>>0),k=primes.map(p=>(Math.cbrt(p)%1*4294967296)>>>0);
 const rot=(x,n)=>(x>>>n)|(x<<(32-n));
 for(let o=0;o<bytes.length;o+=64){const w=new Uint32Array(64);for(let i=0;i<16;i++)w[i]=(bytes[o+4*i]<<24)|(bytes[o+4*i+1]<<16)|(bytes[o+4*i+2]<<8)|bytes[o+4*i+3];
  for(let i=16;i<64;i++){const a=w[i-15],b=w[i-2];w[i]=(w[i-16]+(rot(a,7)^rot(a,18)^(a>>>3))+w[i-7]+(rot(b,17)^rot(b,19)^(b>>>10)))>>>0;}
  let [a,b,c,d,e,f,g,z]=h;
  for(let i=0;i<64;i++){const t1=(z+(rot(e,6)^rot(e,11)^rot(e,25))+((e&f)^(~e&g))+k[i]+w[i])>>>0,t2=((rot(a,2)^rot(a,13)^rot(a,22))+((a&b)^(a&c)^(b&c)))>>>0;z=g;g=f;f=e;e=(d+t1)>>>0;d=c;c=b;b=a;a=(t1+t2)>>>0;}
  [a,b,c,d,e,f,g,z].forEach((v,i)=>h[i]=(h[i]+v)>>>0);
 }return h.map(n=>n.toString(16).padStart(8,'0')).join('');
}
const definitions=[
  {
    "key": "class_start_late",
    "group": "Classes",
    "label": "Class delayed",
    "description": "When a class is expected to start later than scheduled."
  },
  {
    "key": "class_start_early",
    "group": "Classes",
    "label": "Class moved earlier",
    "description": "When a class is expected to start earlier than scheduled."
  },
  {
    "key": "class_starts_in1",
    "group": "Classes",
    "label": "Class reminder 1",
    "description": "A reminder before the expected class start.",
    "input": "minutes",
    "presets": [
      30,
      45,
      60,
      75,
      90
    ]
  },
  {
    "key": "class_starts_in2",
    "group": "Classes",
    "label": "Class reminder 2",
    "description": "A second reminder before the expected class start.",
    "input": "minutes",
    "presets": [
      15,
      30,
      45,
      60
    ]
  },
  {
    "key": "class_started",
    "group": "Classes",
    "label": "Class started",
    "description": "When the class begins."
  },
  {
    "key": "class_underway",
    "group": "Classes",
    "label": "Class in progress",
    "description": "An update when the class is underway."
  },
  {
    "key": "class_completed",
    "group": "Classes",
    "label": "Class finished",
    "description": "When the class is complete."
  },
  {
    "key": "trip_starts_in1",
    "group": "Rider trips",
    "label": "Ride reminder 1",
    "description": "A reminder before the expected start of a rider’s trip.",
    "input": "minutes",
    "presets": [
      30,
      45,
      60,
      75,
      90
    ]
  },
  {
    "key": "trip_starts_in2",
    "group": "Rider trips",
    "label": "Ride reminder 2",
    "description": "A second reminder before the expected start of a rider’s trip.",
    "input": "minutes",
    "presets": [
      15,
      30,
      45,
      60
    ]
  },
  {
    "key": "rider_oog",
    "group": "Rider trips",
    "label": "Rider order",
    "description": "An update about the rider’s order of go."
  },
  {
    "key": "rider_oog10",
    "group": "Rider trips",
    "label": "Rides to go",
    "description": "Tell me when this many entries remain before my rider’s turn.",
    "input": "entries",
    "presets": [
      7,
      10,
      15
    ],
    "defaultValue": "10"
  },
  {
    "key": "rider_started",
    "group": "Rider trips",
    "label": "Rider started",
    "description": "When the rider begins their trip."
  },
  {
    "key": "rider_first_score",
    "group": "Rider trips",
    "label": "First score",
    "description": "When the rider’s first score is reported."
  },
  {
    "key": "rider_first_time",
    "group": "Rider trips",
    "label": "First time",
    "description": "When the rider’s first time is reported."
  },
  {
    "key": "rider_results",
    "group": "Rider trips",
    "label": "Rider results",
    "description": "When results for the rider become available."
  },
  {
    "key": "groom_tasks_at1",
    "group": "Groom tasks",
    "label": "Task reminder 1",
    "description": "A groom task reminder at your chosen time.",
    "input": "time"
  },
  {
    "key": "groom_tasks_at2",
    "group": "Groom tasks",
    "label": "Task reminder 2",
    "description": "A second groom task reminder at your chosen time.",
    "input": "time"
  },
  {
    "key": "groom_tasks_at3",
    "group": "Groom tasks",
    "label": "Task reminder 3",
    "description": "A third groom task reminder at your chosen time.",
    "input": "time"
  }
]
;
function validatePreferences(value,definitions){
 if(!value||value.version!==1||typeof value.enabled!=='boolean'||typeof value.phone!=='string'||typeof value.timeZone!=='string'||!value.variables||Object.keys(value).some(k=>!['version','enabled','phone','timeZone','variables'].includes(k)))fail('invalid_preferences');
 try{new Intl.DateTimeFormat('en',{timeZone:value.timeZone}).format();}catch{fail('invalid_time_zone');}
 if(!value.timeZone||value.timeZone.length>80||value.phone.length>30||Object.keys(value.variables).length!==definitions.length)fail('invalid_preferences');
 const variables={};
 for(const def of definitions){
  const p=value.variables[def.key];
  if(!p||typeof p.enabled!=='boolean'||typeof p.value!=='string'||Object.keys(p).some(k=>!['enabled','value'].includes(k)))fail('invalid_preferences');
  if(p.value&&(def.presets?!def.presets.map(String).includes(p.value):def.input==='time'?!/^([01]\d|2[0-3]):[0-5]\d$/.test(p.value):true))fail('invalid_alert_value');
  if(value.enabled&&p.enabled&&def.input&&!p.value)fail('alert_value_required');
  variables[def.key]={enabled:p.enabled,value:p.value};
 }
 let phone=value.phone.trim();
 if(phone){
  if(!/^\+?[\d ()-]+$/.test(phone))fail('invalid_phone');
  let digits=phone.replace(/\D/g,'');if(!phone.startsWith('+')&&digits.length===10)digits='1'+digits;
  phone='+'+digits;if(!/^\+[1-9]\d{7,14}$/.test(phone))fail('invalid_phone');
 }
 if(value.enabled&&(!phone||!Object.values(variables).some(p=>p.enabled)))fail('phone_and_alert_required');
 return {version:1,enabled:value.enabled,phone,timeZone:value.timeZone,variables};
}

async function processSubmission(){
 const config=input.config();const subs=base.getTable(TABLES.sub);let submission=null,attempted=false,claimToken=null,capability=null,completionAttempted=false,terminalResult=null;
 const bridge=async action=>{if(action==='complete')completionAttempted=true;let response,result;try{response=await fetch(CLAIM_URL,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,capability,...(action==='complete'?{claimToken}:{})})});result=await response.json();}catch{review('server_admission_unavailable');}if(!response.ok||result?.ok!==true)review(result?.error||'server_admission_required');if(result.state==='Applied')terminalResult=result;return result;};
 const status=async(value,error='')=>{
  if(submission&&!SYNTHETIC_VM_ONLY){
   const fresh=await subs.selectRecordAsync(submission.id);
   if(select(cell(fresh,'Processing status'))==='Applied'&&String(cell(fresh,'Error / review note')||'').startsWith('rs-tenant-completed-v1:')){
    // Never erase a server receipt on a lost reply or delayed failure path.
    try{if(!capability)throw Error();let saved=terminalResult||await bridge('claim');if(saved.state==='Replayed'){claimToken=saved.claimToken;saved=await bridge('complete');}const expected=JSON.parse(cell(fresh,'Submitted changes'));if(saved.state!=='Applied'||saved.requestId!==expected.requestId||saved.actorId!==expected.actorId||saved.revision!==expected.expectedRevision+1||!same(saved.preferences,expected.preferences))throw Error();output.set('result_json',JSON.stringify(saved));output.set('status','Applied');output.set('error','');}
    catch{output.set('status','Needs review');output.set('error','completion_confirmation_unavailable');}return;
   }
  }
  if(!SYNTHETIC_VM_ONLY&&completionAttempted){output.set('status','Needs review');output.set('error','completion_confirmation_unavailable');return;}
  if(submission&&!(value==='Applied'&&!SYNTHETIC_VM_ONLY))await subs.updateRecordAsync(submission.id,{'Processing status':{name:value},'Error / review note':error});output.set('status',value);output.set('error',error);
 };
 try{
  if(typeof config.recordId!=='string'||!/^rec[A-Za-z0-9]+$/.test(config.recordId))fail('invalid_submission_record_id');
  submission=await subs.selectRecordAsync(config.recordId);
  if(!submission)fail('submission_not_found');
  if(select(cell(submission,'Processing status'))!=='Pending'){output.set('status','Skipped');output.set('error','submission_not_pending');return;}
  const raw=cell(submission,'Submitted changes');if(typeof raw!=='string'||raw.length>16384)fail('invalid_envelope');
  let e;try{e=JSON.parse(raw);}catch{fail('invalid_envelope');}
  const allowed=['version','kind','actorId','targetTableId','targetRecordId','requestId','eventId','inputHash','expectedRevision','acceptedAt','noticeVersion','source','preferences','admission'];
  if(!e||e.version!==1||e.kind!=='personal-preferences'||Object.keys(e).some(k=>!allowed.includes(k)))fail('invalid_envelope');
  const ACTOR=e.actorId,TARGET=e.targetRecordId;
  if(!/^[A-Za-z0-9_-]{1,128}$/.test(ACTOR||'')||!/^rec[A-Za-z0-9]+$/.test(TARGET||'')||cell(submission,'Actor ID')!==e.actorId||e.targetRecordId!==TARGET||e.targetTableId!==TABLES.target||cell(submission,'Target table ID')!==e.targetTableId||cell(submission,'Target record ID')!==e.targetRecordId||select(cell(submission,'Action'))!=='Update'||(cell(submission,'Membership')||[]).length)fail('submission_scope_mismatch');
  if(!/^\w[\w-]{7,127}$/.test(e.requestId||'')||!Number.isSafeInteger(e.expectedRevision)||e.expectedRevision<1||!Number.isFinite(Date.parse(e.acceptedAt))||e.noticeVersion!=='sms-preferences-v1'||e.source!=='/rs-barn-onboarding-alerts:explicit-save')fail('invalid_operation_evidence');
  const preferences=validatePreferences(e.preferences,definitions);
  if(!same(preferences,e.preferences))fail('preferences_not_normalized');
  const eventId=digest('subscriptions|'+e.actorId+'|'+e.requestId),inputHash=digest(JSON.stringify({preferences,expectedRevision:e.expectedRevision}));
  if(e.eventId!==eventId||e.inputHash!==inputHash||cell(submission,'Submission ID')!==eventId)fail('operation_hash_mismatch');
  if(!SYNTHETIC_VM_ONLY){
   if(e.admission?.version!==1||!/^[a-f0-9-]{36}$/.test(e.admission.token||'')||!/^[a-f0-9]{64}$/.test(e.admission.signature||''))review('server_admission_required');
   capability={eventId,...e.admission};const admitted=await bridge('claim');
   if(admitted.state==='Applied'){output.set('result_json',JSON.stringify(admitted));await status('Applied');return;}
   if(admitted.state==='Replayed'){claimToken=admitted.claimToken;const completed=await bridge('complete');if(completed.state!=='Applied'||completed.requestId!==e.requestId||completed.actorId!==ACTOR)review('completion_result_mismatch');output.set('result_json',JSON.stringify(completed));await status('Applied');return;}
   if(admitted.state!=='Claimed'||typeof admitted.claimToken!=='string')review('server_admission_required');claimToken=admitted.claimToken;
  }
  const targets=base.getTable(TABLES.target),options=base.getTable(TABLES.option),events=base.getTable(TABLES.event);
  const entityId='rs_'+digest('sms-preferences|'+ACTOR).slice(0,32);
  const scope=r=>{if(!r||r.id!==TARGET||cell(r,'entity_uid')!==entityId||cell(r,'owner_uid')!==ACTOR||cell(r,'target_uid')!==ACTOR||select(cell(r,'target_type'))!=='Person'||select(cell(r,'record_mode'))!=='Test'||!['Draft','Revoked','Paused'].includes(select(cell(r,'status')))||['engine_scope','alert_types'].some(k=>cell(r,k))||(cell(r,'Managed by')||[]).length||(cell(r,'Selected favorites')||[]).length)review('preference_scope_mismatch');return r;};
  let target=scope(await targets.selectRecordAsync(TARGET));
  if(!Number.isSafeInteger(cell(target,'revision'))||cell(target,'revision')<1)review('invalid_target_revision');
  const allOptions=(await options.selectRecordsAsync()).records.filter(r=>(cell(r,'Opt-in')||[]).some(v=>v.id===TARGET));
  if(allOptions.length!==18||definitions.length!==18||new Set(allOptions.map(r=>cell(r,'Option'))).size!==18||allOptions.some(r=>(cell(r,'Opt-in')||[]).length!==1||!definitions.some(d=>d.key===cell(r,'Option'))))review('exact_existing_options_required');
  const findEvent=async id=>{const rows=(await events.selectRecordsAsync()).records.filter(r=>cell(r,'event_uid')===id);if(rows.length>1)review('ambiguous_audit_event');return rows[0]||null;};
  const readAudit=r=>{let a;try{a=JSON.parse(cell(r,'result_json'));}catch{review('invalid_audit_record');}if(cell(r,'actor_uid')!==ACTOR||cell(r,'entity_uid')!==entityId||cell(r,'kind')!=='subscriptions'||cell(r,'outcome')!=='committed'||select(cell(r,'record_mode'))!=='Test'||!Number.isFinite(Date.parse(cell(r,'occurred_at')))||!['consent_granted','consent_revoked','preferences_updated'].includes(cell(r,'action'))||cell(r,'barn_uid')||a.id!==entityId||a.ownerUid!==ACTOR||a.targetType!=='Person'||a.targetUid!==ACTOR||!Number.isSafeInteger(a.revision))review('invalid_audit_record');return a;};
  const replay=await findEvent(eventId);
  if(replay){const result=readAudit(replay);if(cell(replay,'input_hash')!==inputHash||cell(replay,'request_uid')!==e.requestId)review('request_id_reused');if(!same(validatePreferences(result.preferences,definitions),preferences)||result.revision!==e.expectedRevision+1)review('invalid_operation_result');
   // Read current scope/options but return the immutable stored operation outcome;
   // newer operations must not be overwritten by an old replay.
   output.set('result_json',cell(replay,'result_json'));await status('Applied');return;
  }
  const recovery=cell(target,'request_uid')===eventId;
  if(recovery){if(cell(target,'request_hash')!==inputHash)review('request_id_reused');if(cell(target,'revision')!==e.expectedRevision+1||cell(target,'preferences_json')!==JSON.stringify(preferences))review('target_operation_mismatch');}
  else{const prior=await findEvent(cell(target,'request_uid'));if(!prior)review('prior_audit_pending');const previous=readAudit(prior);if(cell(prior,'input_hash')!==cell(target,'request_hash')||previous.revision!==cell(target,'revision'))review('prior_audit_mismatch');if(cell(target,'revision')!==e.expectedRevision)review('record_changed');}
  const expected={preferences_json:JSON.stringify(preferences),phone_e164:preferences.phone,status:{name:preferences.enabled?'Draft':'Revoked'},consent_state:{name:preferences.enabled?'Granted':'Revoked'},consent_at:e.acceptedAt,consent_source:e.source,notice_version:e.noticeVersion,revoked_at:preferences.enabled?null:e.acceptedAt,revision:e.expectedRevision+1,request_uid:eventId,request_hash:inputHash};
  // Re-read Pending + immutable envelope immediately before any business effect.
  const fresh=await subs.selectRecordAsync(submission.id);if(select(cell(fresh,'Processing status'))!=='Pending'||cell(fresh,'Submitted changes')!==raw)review('submission_changed');
  const before=scope(await targets.selectRecordAsync(TARGET));if(cell(before,'revision')!==cell(target,'revision')||cell(before,'request_uid')!==cell(target,'request_uid')||cell(before,'request_hash')!==cell(target,'request_hash'))review('record_changed');
  if(!recovery){attempted=true;await targets.updateRecordAsync(TARGET,expected);}
  for(const r of allOptions){const p=preferences.variables[cell(r,'Option')];attempted=true;await options.updateRecordAsync(r.id,{Choice:{name:p.enabled?'On':'Off'},Value:p.value});}
  const verifyTarget=async()=>{const r=scope(await targets.selectRecordAsync(TARGET));for(const [k,v]of Object.entries(expected)){if(k==='phone_e164'&&v===''){if((cell(r,k)??'')!=='')review('target_readback_mismatch');continue;}const isSelect=['status','consent_state','target_type','record_mode','engine_scope'].includes(k);if(isSelect?select(cell(r,k))!==select(v):!same(cell(r,k)??null,v??null))review('target_readback_mismatch');}return r;};
  await verifyTarget();const readOptions=(await options.selectRecordsAsync()).records.filter(r=>(cell(r,'Opt-in')||[]).some(v=>v.id===TARGET));if(readOptions.length!==18||readOptions.some(r=>{const p=preferences.variables[cell(r,'Option')];return !p||(cell(r,'Opt-in')||[]).length!==1||select(cell(r,'Choice'))!==(p.enabled?'On':'Off')||(cell(r,'Value')||'')!==p.value;}))review('options_readback_mismatch');
  const result={ok:true,requestId:e.requestId,actorId:ACTOR,id:entityId,ownerUid:ACTOR,targetType:'Person',targetUid:ACTOR,revision:expected.revision,status:expected.status.name,consentState:expected.consent_state.name,consentAt:e.acceptedAt,consentSource:e.source,noticeVersion:e.noticeVersion,revokedAt:expected.revoked_at,preferences};
  // Check again before create; Airtable has no unique constraint or atomic CAS.
  if(await findEvent(eventId))review('audit_appeared_concurrently');
  attempted=true;await events.createRecordAsync({event_uid:eventId,actor_uid:ACTOR,entity_uid:entityId,action:preferences.enabled?'consent_granted':'consent_revoked',kind:'subscriptions',request_uid:e.requestId,input_hash:inputHash,result_json:JSON.stringify(result),occurred_at:e.acceptedAt,outcome:'committed',record_mode:{name:'Test'}});
  const audit=await findEvent(eventId);if(!audit||cell(audit,'input_hash')!==inputHash||cell(audit,'request_uid')!==e.requestId||!same(readAudit(audit),result))review('audit_readback_mismatch');await verifyTarget();
  const end=await subs.selectRecordAsync(submission.id);if(cell(end,'Submitted changes')!==raw||select(cell(end,'Processing status'))!=='Pending')review('submission_changed');
  if(!SYNTHETIC_VM_ONLY){const completed=await bridge('complete');if(completed.state!=='Applied'||completed.requestId!==e.requestId||completed.actorId!==ACTOR||completed.revision!==result.revision||!same(completed.preferences,preferences))review('completion_result_mismatch');}
  output.set('result_json',JSON.stringify(result));await status('Applied');
 }catch(error){if(error.code==='submission_claim_in_progress'){output.set('status','Skipped');output.set('error',error.code);return;}const value=attempted?'Needs review':error.processingStatus||'Failed';try{await status(value,error.code||'uncertain_storage_failure');}catch{output.set('status','Needs review');output.set('error','submission_status_write_unknown');}}
}
await processSubmission();

