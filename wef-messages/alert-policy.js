'use strict';
// Offline policy only. Every input is a supplied assertion, never live authority.
const {createHash}=require('node:crypto');
const POLICY='wef-messages-v1';
const ID_KEYS=['customer','show','date','ring','group','class','occurrence','phase','entry','trip'];
const REQUIRED_SCOPE=ID_KEYS.slice(0,8);
const SOURCES=new Set(['wef-data','wef-live','wef-ring-pace','wef-class-calculator','wef-entry-calculator']);
const BASES={'wef-data':'appdl16HJ5ETm8N69','wef-live':'appgzosUcVqA4vZdf','wef-ring-pace':'appYOykKIy1kAquhP','wef-class-calculator':'appeqxxJyPGS1iqdm','wef-entry-calculator':'appyoLTcNmLrgpHcg'};
const REMINDERS=new Set(['class_starts_in1','class_starts_in2','trip_starts_in1','trip_starts_in2','rider_oog10']);
const CHANGES=new Set(['class_start_early','class_start_late']);
const STARTED=new Set(['class_started','rider_started']);
const STATUS=new Set(['eligible','send-intent','submitted','delivered','failed','unknown']);
const text=x=>typeof x==='string'&&x.trim().length>0;
const positive=x=>Number.isSafeInteger(x)&&x>0;
const time=x=>typeof x==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(x)&&Number.isFinite(Date.parse(x))?Date.parse(x):NaN;
const iso=x=>new Date(x).toISOString();
function canonical(x){
  if(Array.isArray(x))return '['+x.map(canonical).join(',')+']';
  if(x!==null&&typeof x==='object')return '{'+Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+canonical(x[k])).join(',')+'}';
  return JSON.stringify(x);
}
const hash=x=>createHash('sha256').update(canonical(x)).digest('hex');
const copy=x=>JSON.parse(JSON.stringify(x));
// Reject accessors, exotic prototypes, non-JSON values and cycles before reading.
function jsonData(x,parents=new Set()){
  if(x===null||typeof x==='string'||typeof x==='boolean')return true;
  if(typeof x==='number')return Number.isFinite(x);
  if(typeof x!=='object'||parents.has(x))return false;
  if(!Array.isArray(x)&&Object.getPrototypeOf(x)!==Object.prototype)return false;
  if(Object.getOwnPropertySymbols(x).length)return false;
  parents.add(x);
  for(const [key,d] of Object.entries(Object.getOwnPropertyDescriptors(x))){
    if(Array.isArray(x)&&key==='length')continue;
    if(!('value' in d)||['__proto__','constructor','prototype'].includes(key)||!jsonData(d.value,parents)){parents.delete(x);return false;}
  }
  parents.delete(x);return true;
}
function identity(i){
  if(!i||!['customer','show','ring','group','class'].every(k=>positive(i[k]))||!text(i.occurrence)||i.phase!=='base')return false;
  if(typeof i.date!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(i.date))return false;
  const d=Date.parse(i.date+'T00:00:00Z');
  return Number.isFinite(d)&&iso(d).slice(0,10)===i.date;
}
const scopeOf=i=>Object.fromEntries(ID_KEYS.filter(k=>i[k]!==undefined).map(k=>[k,i[k]]));
const sameScope=(a,b,keys=REQUIRED_SCOPE)=>Boolean(a&&b&&keys.every(k=>a[k]===b[k]));
const personal=c=>c.option.startsWith('trip_')||c.option.startsWith('rider_');
const conditionOf=c=>Object.fromEntries(['id','option','value','unit','previous','current','target_at','previous_target_at','actual_at','confirmations'].filter(k=>c[k]!==undefined).map(k=>[k,copy(c[k])]));
function safeRef(r){return {source:r.source,base:r.base,table:r.table,key:r.key,revision:r.revision,content_hash:r.content_hash,scope:scopeOf(r.scope),observed_at:r.observed_at,valid_until:r.valid_until,...(r.method!==undefined?{method:r.method}:{})};}
function signedState(s){const out=copy(s);delete out.head_hash;out.head_hash=hash(out);return out;}
function validState(s,scope){
  if(!s||s.version!==1||s.scope_key!==scope||!Number.isSafeInteger(s.revision)||s.revision<1||!text(s.preference_revision))return false;
  const fields=['version','scope_key','preference_revision','revision','seen_event_ids','event_hashes','consumed_slots','notifications','last_change_at','last_decision','head_hash'];
  if(Object.keys(s).some(k=>!fields.includes(k)))return false;
  for(const name of ['seen_event_ids','consumed_slots'])if(!Array.isArray(s[name])||!s[name].every(text)||new Set(s[name]).size!==s[name].length)return false;
  if(!s.event_hashes||Array.isArray(s.event_hashes)||!Object.entries(s.event_hashes).every(([k,v])=>text(k)&&/^[a-f0-9]{64}$/.test(v)))return false;
  if(s.seen_event_ids.some(id=>!s.event_hashes[id]))return false;
  if(s.last_change_at!==null&&!Number.isFinite(time(s.last_change_at)))return false;
  if(!Array.isArray(s.notifications)||new Set(s.notifications.map(n=>n.id)).size!==s.notifications.length)return false;
  for(const n of s.notifications){
    const keys=['id','event_ids','slots','status','attempt_count','last_attempt_at','definite_nonacceptance'];
    if(Object.keys(n).some(k=>!keys.includes(k))||!text(n.id)||!STATUS.has(n.status)||!Number.isSafeInteger(n.attempt_count)||n.attempt_count<0||n.attempt_count>2||typeof n.definite_nonacceptance!=='boolean')return false;
    if(!['event_ids','slots'].every(k=>Array.isArray(n[k])&&n[k].length&&n[k].every(text)&&new Set(n[k]).size===n[k].length))return false;
    if(n.last_attempt_at!==null&&!Number.isFinite(time(n.last_attempt_at)))return false;
    if(n.attempt_count>0&&n.last_attempt_at===null)return false;
  }
  const material=copy(s);delete material.head_hash;return s.head_hash===hash(material);
}
function evaluateAlert(input){
  const blocked=reason=>({decision:'withheld',reasons:[reason],snapshot:null,nextState:null});
  try{
    if(!jsonData(input)||!input||Array.isArray(input))return blocked('invalid_json_input');
    const {event:e,recipient:r,prior,now}=input,n=time(now);
    if(!Number.isFinite(n)||!e||!r||e.schema_version!==1||e.policy_version!==POLICY||!text(e.id)||e.source_lane!=='data_live'||!identity(e.identity)||!['active','initial','recovery'].includes(e.mode))return blocked('invalid_event_envelope');
    if(!text(r.person_uid)||!text(r.tenant_uid)||!text(r.preference_revision))return blocked('recipient_identity_missing');
    let day;try{day=new Intl.DateTimeFormat('en-CA',{timeZone:e.timezone,year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(n));}catch{return blocked('timezone_invalid');}
    if(day!==e.identity.date)return blocked('wrong_show_day');
    if(!['available','partial','unavailable'].includes(e.availability)||!e.proof||e.proof.phase!==true)return blocked('phase_or_availability_unproven');
    if(!['scratch','gone','retracted','superseded'].every(k=>typeof e[k]==='boolean'))return blocked('target_state_unknown');
    if(!Array.isArray(e.conditions)||!e.conditions.length||new Set(e.conditions.map(c=>c.id)).size!==e.conditions.length||new Set(e.conditions.map(c=>c.option)).size!==e.conditions.length)return blocked('conditions_missing_or_ambiguous');
    if(e.conditions.some(c=>!text(c.id)||!text(c.option)||!(REMINDERS.has(c.option)||CHANGES.has(c.option)||STARTED.has(c.option))))return blocked('unsupported_condition');
    if(e.conditions.some(personal)&&(!positive(e.identity.entry)||!text(e.identity.trip)))return blocked('personal_identity_missing');
    const scope=canonical({identity:scopeOf(e.identity),person_uid:r.person_uid,tenant_uid:r.tenant_uid});
    if(prior!==undefined&&prior!==null&&!validState(prior,scope))return blocked('prior_chain_invalid');
    const state=prior?copy(prior):{version:1,scope_key:scope,preference_revision:r.preference_revision,revision:0,seen_event_ids:[],event_hashes:{},consumed_slots:[],notifications:[],last_change_at:null,last_decision:null};
    delete state.head_hash;
    const detected=time(e.detected_at),observed=time(e.observed_at),end=time(e.condition_end),expiry=time(e.valid_until);
    if(![detected,observed,end,expiry].every(Number.isFinite)||detected>n||observed>n||observed>detected||expiry<=observed)return blocked('event_time_invalid');
    if(!Array.isArray(e.input_refs)||!e.input_refs.length)return blocked('provenance_missing');
    let until=Math.min(expiry,end,detected+120000);
    const refs=[];
    for(const ref of e.input_refs){
      if(!ref||!SOURCES.has(ref.source)||ref.base!==BASES[ref.source]||!/^tbl[A-Za-z0-9]{14}$/.test(ref.table)||!text(ref.key)||!text(ref.revision)||!/^([a-f0-9]{64})$/.test(ref.content_hash)||!sameScope(ref.scope,e.identity))return blocked('provenance_scope_invalid');
      if(ref.source==='wef-ring-pace'&&!['schedule_window','observed_interval'].includes(ref.method))return blocked('pace_method_unproven');
      const at=time(ref.observed_at),limit=time(ref.valid_until),maxAge=ref.source==='wef-live'||(ref.source==='wef-ring-pace'&&ref.method==='observed_interval')?600000:1800000;
      if(!Number.isFinite(at)||!Number.isFinite(limit)||at>n||limit<=at||n-at>=maxAge)return blocked('source_stale_or_invalid');
      if((ref.scope.entry!==undefined&&ref.scope.entry!==e.identity.entry)||(ref.scope.trip!==undefined&&ref.scope.trip!==e.identity.trip))return blocked('provenance_personal_scope_mismatch');
      until=Math.min(until,limit,at+maxAge);refs.push(safeRef(ref));
    }
    if(observed!==refs.reduce((earliest,ref)=>Math.min(earliest,time(ref.observed_at)),Infinity))return blocked('observation_provenance_mismatch');
    const conditions=e.conditions.map(conditionOf);
    // Untrusted extras are excluded from hashes and outputs, never copied.
    const eventHash=hash({id:e.id,identity:scopeOf(e.identity),source_lane:e.source_lane,policy:e.policy_version,availability:e.availability,detected_at:e.detected_at,observed_at:e.observed_at,valid_until:e.valid_until,condition_end:e.condition_end,proof:{start:e.proof.start===true,phase:e.proof.phase===true,order:e.proof.order===true,anchor:e.proof.anchor===true,eligibility:e.proof.eligibility===true,observed_transition:e.proof.observed_transition===true},scratch:e.scratch,gone:e.gone,retracted:e.retracted,superseded:e.superseded,conditions,input_refs:refs});
    if(state.event_hashes[e.id]&&state.event_hashes[e.id]!==eventHash)return blocked('event_identity_reused_for_changed_material');
    const noOp=()=>({decision:'noop',reasons:['already_processed'],snapshot:null,nextState:copy(prior)});
    function finish(decision,reasons,selected=[],notification=null,forceRevision=false){
      if(!forceRevision&&prior&&state.seen_event_ids.includes(e.id)&&state.last_decision===decision)return noOp();
      if(!state.seen_event_ids.includes(e.id))state.seen_event_ids.push(e.id);
      state.event_hashes[e.id]=eventHash;state.revision++;state.last_decision=decision;
      const next=signedState(state);
      const payload={schema_version:1,customer_id:e.identity.customer,occurrence_id:e.identity.occurrence,recipient_person_uid:r.person_uid,tenant_uid:r.tenant_uid,preference_revision:r.preference_revision,condition_ids:selected.map(c=>c.id).sort(),options:selected.map(c=>c.option).sort(),conditions:selected,notification_id:notification?.id??null,attempt_id:notification?hash([notification.id,notification.attempt_count+1]):null,state_revision:next.revision,prior_snapshot_hash:prior?.head_hash??null,state:next};
      const material={scope,source_event_id:e.id,decision,reasons,payload,input_refs:refs,observed_at:e.observed_at,valid_until:iso(until),policy:POLICY};
      const digest=hash(material);
      const snapshot={'snapshot-key':hash([scope,next.revision,digest]),'scope-key':scope,'source-event-id':e.id,'alert-kind':selected.length===1?selected[0].option:selected.length?'combined':'event-decision','source-lane':e.source_lane,'show-id':e.identity.show,'date':e.identity.date,'ring-number':e.identity.ring,'group-id':e.identity.group,'class-id':e.identity.class,'entry-id':e.identity.entry??null,'trip-key':e.identity.trip??null,'phase':e.identity.phase,'availability':e.availability,'decision':decision,'evaluated-at':now,'observed-at':e.observed_at,'valid-until':iso(until),'policy-version':POLICY,'reason-codes-json':JSON.stringify(reasons),'input-refs-json':canonical(refs),'payload-json':canonical(payload),'content-hash':digest};
      return {decision,reasons:copy(reasons),snapshot,nextState:next};
    }
    if(n>=until)return finish('expired',['event_expired']);
    if(e.availability==='unavailable')return finish('withheld',['answer_unavailable']);
    if(e.scratch||e.gone||e.retracted||e.superseded)return finish('suppressed',['target_no_longer_actionable']);
    const authorityAt=time(r.authority_observed_at),authorityUntil=time(r.authority_valid_until);
    if(r.represented_person_uid!==r.person_uid||r.membership_person_uid!==r.person_uid||r.membership_tenant_uid!==r.tenant_uid||r.membership_status!=='Enabled'||r.scope_permitted!==true||!sameScope(r.scope,e.identity,ID_KEYS.filter(k=>e.identity[k]!==undefined))||r.consent!=='Granted'||r.status!=='Active'||r.record_mode!=='Production'||r.enabled!==true||r.revoked!==false||r.provider_suppressed!==false||!Number.isFinite(authorityAt)||!Number.isFinite(authorityUntil)||authorityAt>n||authorityUntil<=n||authorityUntil<=authorityAt)return finish('suppressed',['recipient_not_currently_eligible']);
    until=Math.min(until,authorityUntil);
    if(!prior||e.mode!=='active'||state.preference_revision!==r.preference_revision){state.preference_revision=r.preference_revision;return finish('baseline',['initial_recovery_or_preferences_changed']);}
    const existing=state.notifications.find(item=>item.event_ids.includes(e.id));
    const retry=existing?.status==='failed'&&existing.definite_nonacceptance===true&&existing.attempt_count===1&&n-time(existing.last_attempt_at)>=60000;
    if(state.seen_event_ids.includes(e.id)&&!retry)return noOp();
    const selected=[],disabled=[];
    for(const c of conditions){
      const option=r.options?.[c.option];
      if(!option||option.enabled!==true){disabled.push(c.option);continue;}
      if(personal(c)&&(!e.proof.phase||!e.proof.order||!e.proof.eligibility))return finish('withheld',['personal_evidence_unproven']);
      if(REMINDERS.has(c.option)){
        const unit=c.option==='rider_oog10'?'entries':'minutes';
        if(!Number.isFinite(c.value)||c.value<=0||c.unit!==unit||option.value!==c.value||!Number.isFinite(c.previous)||!Number.isFinite(c.current)||c.current<0||!(c.previous>c.value&&c.current<=c.value))return finish('withheld',['threshold_contract_invalid']);
        if(unit==='entries'&&(!Number.isSafeInteger(c.value)||!Number.isSafeInteger(c.previous)||!Number.isSafeInteger(c.current)))return finish('withheld',['entry_count_not_integer']);
        if(unit==='minutes'&&(!e.proof.start||!Number.isFinite(time(c.target_at))||time(c.target_at)<=n))return finish('withheld',['start_estimate_missing_or_past']);
        if(c.option.startsWith('trip_')&&!e.proof.anchor)return finish('withheld',['personal_anchor_unproven']);
      }else if(CHANGES.has(c.option)){
        const old=time(c.previous_target_at),target=time(c.target_at),direction=c.option==='class_start_late'?1:-1;
        if(!e.proof.start||!Number.isFinite(old)||!Number.isFinite(target)||target<=n||direction*(target-old)<300000||!Array.isArray(c.confirmations)||c.confirmations.length!==2)return finish('withheld',['estimate_change_unconfirmed']);
        const [a,b]=c.confirmations;
        if(!text(a.id)||!text(b.id)||a.id===b.id||!Number.isFinite(time(a.observed_at))||!Number.isFinite(time(b.observed_at))||time(a.observed_at)>=time(b.observed_at)||time(b.observed_at)>observed||n-time(a.observed_at)>=600000||!Number.isFinite(time(a.target_at))||!Number.isFinite(time(b.target_at))||direction*(time(a.target_at)-old)<300000||b.target_at!==c.target_at)return finish('withheld',['estimate_change_unconfirmed']);
        if(state.last_change_at!==null&&n-time(state.last_change_at)<600000)return finish('withheld',['estimate_change_spacing']);
      }else if(!e.proof.observed_transition||!Number.isFinite(time(c.actual_at))||time(c.actual_at)>observed)return finish('withheld',['actual_transition_unproven']);
      selected.push(c);
    }
    if(!selected.length)return finish('suppressed',['no_enabled_matching_choice']);
    // A consumed milestone is not revived by a different producer revision.
    const slot=c=>hash([scope,c.option]);
    const allSlots=selected.filter(c=>!CHANGES.has(c.option)).map(slot);
    if(!retry&&allSlots.some(key=>state.consumed_slots.includes(key)))return finish('suppressed',['milestone_already_consumed']);
    let combined=selected.slice();
    for(const family of ['class_starts_in','trip_starts_in']){
      const members=combined.filter(c=>c.option.startsWith(family));
      if(members.length>1){const nearer=Math.min(...members.map(c=>c.value));combined=combined.filter(c=>!c.option.startsWith(family)||c.value===nearer);}
    }
    combined.sort((a,b)=>a.id.localeCompare(b.id));
    const id=hash([scope,e.id,combined.map(c=>[c.id,c.option]),'sms']);
    let notification=existing;
    if(existing&&!retry)return finish('suppressed',['notification_already_reserved']);
    if(retry){
      if(existing.id!==id)return blocked('retry_notification_identity_changed');
      notification.status='eligible';notification.definite_nonacceptance=false;
    }else{
      notification={id,event_ids:[e.id,...combined.map(c=>c.id)],slots:allSlots,status:'eligible',attempt_count:0,last_attempt_at:null,definite_nonacceptance:false};
      // Condition IDs and the envelope ID are separate namespaces in producer input.
      notification.event_ids=[...new Set(notification.event_ids)];
      if(!notification.slots.length)notification.slots=[hash([scope,e.id,'change'])];
      state.notifications.push(notification);
    }
    for(const key of allSlots)if(!state.consumed_slots.includes(key))state.consumed_slots.push(key);
    if(combined.some(c=>CHANGES.has(c.option)))state.last_change_at=now;
    return finish('eligible',[retry?'definite_nonacceptance_retry':'validated_condition'],combined,notification,Boolean(retry));
  }catch{return blocked('invalid_input_or_state');}
}
module.exports={evaluateAlert};
