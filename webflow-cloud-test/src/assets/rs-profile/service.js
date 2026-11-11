import {recognitionAdapter} from '../rs-recognition/widget.js';
import {safeStorage,browserStorage} from '../rs-recognition/native-client.js';
const endpoint='/test/rs-profile/data',storage=safeStorage(browserStorage('localStorage'));
const pendingKey=uid=>'rs_profile_pending_v1:'+uid,snapshotKey=uid=>'rs_profile_snapshot_v1:'+uid;
let currentUid=null,confirmed=null,pending=[],generation=0,loading=null,saveVersion=0,currentFingerprint=null,refreshing=null,recognizing=null;
const listeners=new Set(),running=new Map(),copy=value=>structuredClone(value);
// These Profile errors are raised by profileFields before runTenantSave or writes.
const noWriteProfileErrors=new Set(['invalid_profile','invalid_profile_field','name_required','invalid_email','invalid_photo','https_image_url_required','invalid_tags','invalid_phone','invalid_pin']);
const recoveringOperations=new Set(['member_status','team_create','invitation_create','invitation_response']);
const operationKey=b=>b.operation==='profile'?'profile':b.operation==='member_status'?'member:'+b.membershipId:'team';
const fingerprintOf=async value=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(value)))),b=>b.toString(16).padStart(2,'0')).join('');
function viewPreferences(state){try{state.hidden=JSON.parse(storage.getItem('rs_profile_hidden_v1:'+currentUid)||'[]').filter(x=>Array.isArray(x)&&x[0]===currentUid);}catch{state.hidden=[];}return state;}
export function saveViewPreferences(hidden){if(currentUid)storage.setItem('rs_profile_hidden_v1:'+currentUid,JSON.stringify(hidden.filter(x=>x[0]===currentUid)));}
function retainPending(){
 if(!currentUid)return;if(!pending.length){storage.removeItem(pendingKey(currentUid));return;}
 const saved=pending.map(({uid,fingerprint,body,sent,needsPin,error,blocked,noWrite,uncertain})=>{const safe={uid,fingerprint,body:copy(body),sent,needsPin,error,blocked,noWrite,uncertain};if(safe.body.fields?.pin){delete safe.body.fields.pin;safe.needsPin=true;}return safe;});
 storage.setItem(pendingKey(currentUid),JSON.stringify(saved));
}
function remember(fingerprint=currentFingerprint){if(!confirmed||!currentUid||!fingerprint)return;const state=copy(confirmed);for(const person of state.profiles||[])delete person.pin;storage.setItem(snapshotKey(currentUid),JSON.stringify({uid:currentUid,deviceFingerprint:fingerprint,state}));const deviceToken=storage.getItem('rs_recognition_device_token_v1');if(deviceToken)storage.setItem('rs_recognition_identity_v1',JSON.stringify({personUid:currentUid,deviceToken,expiresAt:Number(storage.getItem('rs_recognition_device_expires_v1'))||Date.now()+365*86400000}));}
const geoipPinged=new Set(),geoipKey=uid=>'rs_profile_geoip_v1:'+uid;
async function pingGeoip(uid,check,fingerprint){
 const key=uid+':'+fingerprint;if(!fingerprint||geoipPinged.has(key))return;geoipPinged.add(key);
 try{
  const response=await fetch(endpoint+'?view=geoip',{credentials:'same-origin',signal:AbortSignal.timeout(2500),headers:{'X-RS-Actor-Scope':uid}});
  if(!response.ok)return;const data=await response.json();
  if(uid!==currentUid||check!==generation||fingerprint!==currentFingerprint||!confirmed||storage.getItem('rs_recognition_pending_signout_v1')||data.person_uid!==uid)return;
  const geo=data.geoip;if(!geo||!Number.isFinite(geo.latitude)||!Number.isFinite(geo.longitude)||Math.abs(geo.latitude)>90||Math.abs(geo.longitude)>180||!['cloudflare','geoip'].includes(geo.source)||!Number.isFinite(Date.parse(geo.observedAt)))return;
  storage.setItem(geoipKey(uid),JSON.stringify({uid,deviceFingerprint:fingerprint,geoip:{latitude:geo.latitude,longitude:geo.longitude,source:geo.source,observedAt:geo.observedAt}}));
 }catch{/* Optional metadata never delays or interrupts Profile. */}
}
export function profileState(){
 if(!confirmed)return null;const state=viewPreferences(copy(confirmed));
 try{const saved=JSON.parse(storage.getItem(geoipKey(currentUid))||'null');if(saved?.uid===currentUid&&saved.deviceFingerprint===currentFingerprint)state.geoip=saved.geoip;}catch{}
 for(const entry of pending){if(entry.body.operation==='member_status'){const member=state.memberships.find(m=>m.id===entry.body.membershipId);if(member&&member.userId!==currentUid)member.active=entry.body.active;}if(entry.body.operation==='profile'){const own=state.profiles.find(p=>p.id===currentUid),fields={...entry.body.fields};delete fields.pin;if(own)Object.assign(own,fields);}}
 state.pendingActions=pending.filter(p=>p.body.operation!=='profile').map(p=>({...copy(p.body),error:p.error||''}));state.pendingSave=!!pending.length;state.pendingNeedsPin=pending.some(p=>p.needsPin);state.pendingError=pending.find(p=>p.error)?.error||'';return state;
}
function notify(error){for(const listener of listeners)listener(profileState(),error);}
export function subscribeProfile(listener){listeners.add(listener);return()=>listeners.delete(listener);}
function restore(uid){
 if(currentUid===uid)return;currentUid=uid;confirmed=null;pending=[];
 try{const saved=JSON.parse(storage.getItem(pendingKey(uid))||'null');pending=(Array.isArray(saved)?saved:saved?[saved]:[]).filter(p=>p.uid===uid&&p.body?.requestId).map(p=>({...p,sent:p.sent!==false,uncertain:p.uncertain??(p.sent!==false&&!p.noWrite&&!p.blocked),error:recoveringOperations.has(p.body.operation)&&!p.blocked&&!p.needsPin?'':p.error}));}catch{}
}
function mergeConfirmed(next,entry){
 if(!confirmed){confirmed=next;return;}
 if(entry.body.operation==='profile'){const person=next.profiles.find(p=>p.id===currentUid);confirmed.profiles=confirmed.profiles.map(p=>p.id===currentUid?person||p:p);}
 else if(entry.body.operation==='member_status'){const member=next.memberships.find(m=>m.id===entry.body.membershipId);confirmed.memberships=confirmed.memberships.map(m=>m.id===entry.body.membershipId?member||m:m);}
 else{const own=confirmed.profiles.find(p=>p.id===currentUid);confirmed=next;if(own)confirmed.profiles=confirmed.profiles.map(p=>p.id===currentUid?own:p);}
}
function revisionFor(entry){if(entry.body.operation==='profile')return confirmed.profiles.find(p=>p.id===currentUid)?.revision;if(entry.body.operation==='member_status')return confirmed.memberships.find(m=>m.id===entry.body.membershipId)?.revision;return entry.body.expectedRevision;}
function settle(entry,error){const waiters=entry.waiters||[];entry.waiters=[];for(const waiter of waiters)error?waiter.reject(error):waiter.resolve(profileState());}
function drain(key){
 if(running.has(key))return running.get(key);const actor=currentUid,check=generation;
 const work=(async()=>{while(actor===currentUid&&check===generation){
  const entry=pending.find(p=>operationKey(p.body)===key);if(!entry||entry.needsPin||entry.error||entry.blocked)return;
  try{
   if(confirmed?.loaded?.profile===false)await loadProfileView('profile',{priority:true});
   if(entry.ready)await entry.ready;if(actor!==currentUid||check!==generation)throw Error('Profile changed before this save.');
   if(!entry.sent){const revision=revisionFor(entry);if(revision!==undefined)entry.body.expectedRevision=revision;const original={...entry.body};delete original.requestId;entry.fingerprint=await fingerprintOf(original);if(actor!==currentUid||check!==generation)throw Error('Profile changed before this save.');entry.sent=true;retainPending();}
   const response=await fetch(endpoint,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-RS-Actor-Scope':actor},body:JSON.stringify(entry.body),signal:AbortSignal.timeout(20000)});
   const data=await response.json();if(!response.ok||!data.ok){const error=Error(data.error||'The save has not been confirmed.');error.status=response.status;throw error;}
   if(actor!==currentUid||check!==generation||data.state?.identity.currentId!==actor)throw Error('Profile changed during this save.');
   mergeConfirmed(data.state,entry);saveVersion++;viewEtags.clear();pending=pending.filter(p=>p!==entry);retainPending();remember();settle(entry);notify();
  }catch(error){if(actor!==currentUid||check!==generation){settle(entry,error);return;}entry.error=error.message;entry.blocked=error.status>=400&&error.status<500;entry.uncertain||=entry.sent&&(!Number.isInteger(error.status)||error.status>=500||error.status<400);entry.noWrite=!entry.uncertain&&entry.body.operation==='profile'&&error.status===400&&noWriteProfileErrors.has(error.message);retainPending();settle(entry,error);if([401,403].includes(error.status)){confirmed=null;storage.removeItem(snapshotKey(actor));}notify(error);return;}
 }})();running.set(key,work);void work.finally(()=>{if(running.get(key)===work)running.delete(key);});return work;
}
function resume(){for(const entry of pending)if(!entry.needsPin&&!entry.error)void drain(operationKey(entry.body));}
function mergeLoaded(next){
 const sameHorseTeam=confirmed?.horseTeamId&&next.memberships?.some(m=>m.userId===currentUid&&m.active&&m.teamId===confirmed.horseTeamId)&&!next.teamConflict;
 if(sameHorseTeam&&!next.loaded?.horses)next={...next,horses:confirmed.horses,horseTeamId:confirmed.horseTeamId,loaded:{...next.loaded,horses:confirmed.loaded?.horses}};
 else if(confirmed?.loaded?.horses&&!sameHorseTeam){delete confirmed.horses;delete confirmed.horseTeamId;delete confirmed.loaded.horses;viewEtags.delete('horses');}
 if(!confirmed||!next.loaded||next.loaded.team){confirmed=next;return;}
 // A roster refresh replaces its records and invalidates related invitation choices.
 if(next.loaded.roster){confirmed={...next,loaded:{...next.loaded,team:false}};return;}
 const own=rows=>rows.filter(m=>m.userId===currentUid).map(m=>[m.id,m.teamId,m.active,m.role]);
 if(JSON.stringify(own(confirmed.memberships))!==JSON.stringify(own(next.memberships))){confirmed=next;return;}
 const byId=(old,rows)=>[...new Map([...old,...rows].map(row=>[row.id,row])).values()];
 confirmed={...confirmed,...next,profiles:byId(confirmed.profiles,next.profiles),teams:byId(confirmed.teams||[],next.teams||[]),memberships:next.loaded.roster?next.memberships:[...confirmed.memberships.filter(m=>m.userId!==currentUid),...next.memberships],invitations:next.loaded.team?next.invitations:confirmed.invitations,loaded:{...confirmed.loaded,...Object.fromEntries(Object.entries(next.loaded).filter(([,value])=>value))}};
}
const packageQueue=[];let activePackage=null;
function prioritizePackage(view){
 const job=activePackage?.view===view?activePackage:packageQueue.find(job=>job.view===view);
 if(job)job.priority=true;
 if(activePackage&&!activePackage.priority&&activePackage.view!==view){activePackage.paused=true;activePackage.controller.abort();}
}
async function pumpPackages(){
 if(activePackage||!packageQueue.length)return;
 const index=packageQueue.findIndex(job=>job.priority),job=packageQueue.splice(index<0?0:index,1)[0];activePackage=job;job.paused=false;job.controller=new AbortController();
 try{job.resolve(await fetchProfile(...job.args,job.controller.signal));}
 catch(error){if(job.paused&&job.generation===generation)packageQueue.unshift(job);else job.reject(error);}
 finally{if(activePackage===job)activePackage=null;void pumpPackages();}
}
function readProfile(uid,check,fingerprint,view='profile'){
 return new Promise((resolve,reject)=>{packageQueue.push({view,args:[uid,check,fingerprint,view],generation:check,priority:false,resolve,reject});void pumpPackages();});
}
const scopedLoads=new Map();
let horsesRequested=false;
const viewEtags=new Map();
let periodicRefresh=null;
export function refreshProfile(){
 if(periodicRefresh)return periodicRefresh;
 if(!currentUid||!confirmed)return Promise.resolve(null);
 const actor=currentUid,check=generation;
 // Team contains the roster and own profile; otherwise refresh the largest prepared view.
 const view=confirmed.loaded?.team?'team':confirmed.loaded?.roster?'roster':'profile';
 const refreshHorses=horsesRequested||!!confirmed.loaded?.horses;
 const work=(async()=>{await (scopedLoads.get(view)||readProfile(actor,check,currentFingerprint,view));if(refreshHorses&&actor===currentUid&&check===generation&&confirmed)return loadProfileView('horses',{background:true});return profileState();})().catch(error=>{if(actor===currentUid&&check===generation){if([401,403].includes(error.status)){confirmed=null;storage.removeItem(snapshotKey(actor));viewEtags.clear();}notify(error);}throw error;});
 periodicRefresh=work;void work.finally(()=>{if(periodicRefresh===work)periodicRefresh=null;}).catch(()=>{});return work;
}
export function loadProfileView(view,{priority=false,background=false}={}){
 if(!['profile','roster','team','horses'].includes(view)||!currentUid||!confirmed)return Promise.reject(Error('Profile is required first.'));
 if(priority)prioritizePackage(view);
 if(view==='profile'&&refreshing)return refreshing;
 if(view==='horses')horsesRequested=true;
 if(scopedLoads.has(view))return scopedLoads.get(view);
 const actor=currentUid,check=generation,fingerprint=currentFingerprint;
 const work=(priority||background?readProfile:fetchProfile)(actor,check,fingerprint,view).catch(error=>{if(actor===currentUid&&check===generation){if([401,403].includes(error.status)){confirmed=null;storage.removeItem(snapshotKey(actor));}notify(error);}throw error;});
 if(priority)prioritizePackage(view);
 scopedLoads.set(view,work);void work.finally(()=>{if(scopedLoads.get(view)===work)scopedLoads.delete(view);}).catch(()=>{});return work;
}
async function fetchProfile(uid,check,fingerprint,view='profile',signal){
 const startedSaveVersion=saveVersion,response=await fetch(endpoint+'?view='+view,{credentials:'same-origin',cache:'no-store',headers:{'X-RS-Actor-Scope':uid,...(viewEtags.has(view)?{'If-None-Match':viewEtags.get(view)}:{})},signal:signal?AbortSignal.any([signal,AbortSignal.timeout(20000)]):AbortSignal.timeout(20000)});
 if(signal?.aborted){if(check!==generation)throw Error('Profile changed while loading.');throw signal.reason;}
 if(check!==generation||currentUid!==uid||recognitionAdapter.profileDeviceFingerprint?.()!==fingerprint)throw Error('Profile changed while loading.');
 if(response.status===304){resume();return profileState();}
 const data=await response.json();
 if(signal?.aborted){if(check!==generation)throw Error('Profile changed while loading.');throw signal.reason;}
 if(!response.ok||!data.ok){const error=Error(data.error||'Profile could not be loaded.');error.status=response.status;throw error;}
 if(data.person_uid!==uid||data.state?.identity.currentId!==uid)throw Error('Profile identity did not match Recognize.');
 if(check!==generation||currentUid!==uid)throw Error('Profile changed while loading.');
 const before=JSON.stringify(confirmed);
 if(startedSaveVersion!==saveVersion){viewEtags.delete(view);if(view!=='profile')return fetchProfile(uid,check,fingerprint,view,signal);}else{
  if(view==='horses'){
   // A package response cannot replace newer Profile or Team records.
   const own=state=>JSON.stringify((state?.memberships||[]).filter(m=>m.userId===uid).map(m=>[m.id,m.teamId,m.active,m.role]));
   if(confirmed?.loaded?.profile===false)mergeLoaded(data.state);
   if(own(confirmed)!==own(data.state)||confirmed?.teamConflict!==data.state.teamConflict){viewEtags.delete(view);return profileState();}
   confirmed={...confirmed,horses:data.state.horses,horseTeamId:data.state.horseTeamId,loaded:{...confirmed.loaded,horses:true}};
  }else mergeLoaded(data.state);const etag=response.headers.get('ETag');if(etag)viewEtags.set(view,etag);}
 if(before!==JSON.stringify(confirmed)){remember(fingerprint);notify();}resume();return profileState();
}
export function prepareProfile(uid){return loadProfile(uid);}
export function loadProfile(resolvedUid){
 if(resolvedUid!==undefined)return loadResolvedProfile(resolvedUid,recognitionAdapter.profileDeviceFingerprint?.());
 if(!confirmed&&!storage.getItem('rs_recognition_pending_signout_v1')){try{
  const identity=JSON.parse(storage.getItem('rs_recognition_identity_v1')||'null'),token=storage.getItem('rs_recognition_device_token_v1');
  if(identity?.personUid&&token&&identity.deviceToken===token&&identity.expiresAt>Date.now()){
   const cached=JSON.parse(storage.getItem(snapshotKey(identity.personUid))||'null');
   if(cached?.uid===identity.personUid&&cached.deviceFingerprint&&cached.state?.identity?.currentId===identity.personUid){restore(identity.personUid);currentFingerprint=cached.deviceFingerprint;confirmed=cached.state;notify();}
  }
 }catch{}}
 const check=generation;
 if(!recognizing){const work=Promise.resolve().then(()=>recognitionAdapter.current());recognizing=work;void work.finally(()=>{if(recognizing===work)recognizing=null;}).catch(()=>{});}
 return recognizing.then(uid=>{if(check!==generation)throw Error('Profile changed while loading.');return loadResolvedProfile(uid,recognitionAdapter.profileDeviceFingerprint?.());});
}
function loadResolvedProfile(uid,fingerprint){
 // Resolve the current server identity/device before reusing any private work.
 if(currentUid&&(uid!==currentUid||fingerprint!==currentFingerprint))clearProfile();
 if(!uid){clearProfile();return Promise.resolve(null);}
 if(loading)return loading;
 if(refreshing&&currentUid)return confirmed?Promise.resolve(profileState()):refreshing;
 const check=generation;restore(uid);currentFingerprint=fingerprint;
 // Recognize has confirmed this person on the new device session. The working
 // copy belongs to the person, not the retired sign-in token.
 if(!confirmed&&!storage.getItem('rs_recognition_pending_signout_v1')){try{const cached=JSON.parse(storage.getItem(snapshotKey(uid))||'null');if(fingerprint&&cached?.uid===uid&&cached.state?.identity?.currentId===uid){confirmed=cached.state;remember(fingerprint);notify();}}catch{}}
 // The existing authenticated Recognize reply is enough to paint the person.
 // This is not a loaded Profile record or an editable set of blank fields.
 const identity=recognitionAdapter.profileIdentity?.();
 if(!confirmed&&fingerprint&&identity?.uid===uid&&!storage.getItem('rs_recognition_pending_signout_v1')){
  confirmed={identity:{currentId:uid},profiles:[{id:uid,name:identity.name,first:identity.name,last:'',tags:[]}],teams:[],memberships:[],invitations:[],loaded:{profile:false,roster:false,team:false}};
  remember(fingerprint);notify();
 }
 const refresh=readProfile(uid,check,fingerprint);void refresh.then(()=>pingGeoip(uid,check,fingerprint)).catch(()=>{});refreshing=refresh;void refresh.finally(()=>{if(refreshing===refresh)refreshing=null;}).catch(()=>{});
 const work=confirmed?Promise.resolve(profileState()):refresh;
 if(confirmed)void refresh.catch(error=>{if(check!==generation||currentUid!==uid||currentFingerprint!==fingerprint)return;if([401,403].includes(error.status)){confirmed=null;storage.removeItem(snapshotKey(uid));}notify(error);});
 loading=work;void work.finally(()=>{if(loading===work)loading=null;}).catch(()=>{});return work;
}

export function saveProfile(operation,values){
 if(!currentUid||!confirmed)return Promise.reject(Error('Recognize is required first.'));
 const actor=currentUid,check=generation,input={operation,...copy(values)},key=operationKey(input);
 // Only an explicit correction can replace a proven no-write validation failure.
 if(operation==='profile'){const rejected=pending.filter(p=>p.noWrite&&p.body.operation==='profile'&&JSON.stringify(p.body.fields)!==JSON.stringify(input.fields));if(rejected.length){input.fields=Object.assign({},...rejected.map(p=>p.body.fields),input.fields);pending=pending.filter(p=>!rejected.includes(p));}}
 let entry=pending.find(p=>operationKey(p.body)===key&&JSON.stringify(p.body.fields||{})===JSON.stringify(input.fields||{})&&(p.body.operation==='profile'||Object.keys(input).every(k=>k==='expectedRevision'||JSON.stringify(p.body[k])===JSON.stringify(input[k]))));
 if(!entry&&input.fields?.pin)entry=pending.find(p=>p.needsPin&&p.body.operation==='profile');
 if(entry?.blocked)return Promise.reject(Error(entry.error));
 if(!entry){entry={uid:actor,body:{...input,requestId:crypto.randomUUID()},sent:false,needsPin:false,error:'',waiters:[]};pending.push(entry);}
 const result=new Promise((resolve,reject)=>{(entry.waiters||=[]).push({resolve,reject});});
 entry.ready=(async()=>{
  const fingerprint=await fingerprintOf(input);if(actor!==currentUid||check!==generation)throw Error('Profile changed before this save.');
  if(entry.needsPin){const original={...entry.body,fields:{...entry.body.fields,pin:input.fields?.pin}};delete original.requestId;if(!input.fields?.pin||entry.fingerprint&&await fingerprintOf(original)!==entry.fingerprint)throw Error('Retry the pending save with the same PIN.');entry.body.fields.pin=input.fields.pin;entry.needsPin=false;}
  entry.fingerprint||=fingerprint;entry.error='';retainPending();
 })();retainPending();notify();
 void entry.ready.then(()=>{for(const item of pending)if(operationKey(item.body)===key&&!item.needsPin&&!item.blocked)item.error='';void drain(key);},error=>{if(actor!==currentUid||check!==generation){settle(entry,error);return;}entry.error=error.message;retainPending();settle(entry,error);notify(error);});return result;
}
export function clearProfile(){generation++;if(activePackage){activePackage.paused=false;activePackage.controller.abort();}for(const job of packageQueue.splice(0))job.reject(Error('Profile changed while loading.'));for(const entry of pending)settle(entry,Error('Profile changed during this save.'));currentUid=null;confirmed=null;pending=[];loading=null;refreshing=null;currentFingerprint=null;recognizing=null;periodicRefresh=null;horsesRequested=false;viewEtags.clear();scopedLoads.clear();running.clear();notify();}
