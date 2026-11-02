import {recognitionAdapter} from '../rs-recognition/widget.js';
import {safeStorage,browserStorage} from '../rs-recognition/native-client.js';

const endpoint='/test/rs-profile/data';
let currentUid=null,pending=null,generation=0;
const storage=safeStorage(browserStorage('localStorage'));
const pendingKey=uid=>'rs_profile_pending_v1:'+uid;
function viewPreferences(state){try{state.hidden=JSON.parse(storage.getItem('rs_profile_hidden_v1:'+currentUid)||'[]').filter(x=>Array.isArray(x)&&x[0]===currentUid);}catch{state.hidden=[];}return state;}
export function saveViewPreferences(hidden){if(currentUid)storage.setItem('rs_profile_hidden_v1:'+currentUid,JSON.stringify(hidden.filter(x=>x[0]===currentUid)));}
function retainPending(){if(!pending)return;const safe=structuredClone(pending);if(safe.body.fields?.pin){delete safe.body.fields.pin;safe.needsPin=true;}storage.setItem(pendingKey(pending.uid),JSON.stringify(safe));}
export async function loadProfile(){
 const check=++generation;
 const uid=await recognitionAdapter.current();if(!uid){currentUid=null;return null;}
 const response=await fetch(endpoint,{credentials:'same-origin',cache:'no-store',headers:{'X-RS-Actor-Scope':uid},signal:AbortSignal.timeout(20000)});
 const data=await response.json();if(!response.ok||!data.ok)throw Error(data.error||'Profile could not be loaded.');
 if(data.person_uid!==uid||data.state?.identity.currentId!==uid)throw Error('Profile identity did not match Recognize.');
 if(check!==generation)throw Error('Profile changed while loading.');
 if(currentUid!==uid)pending=null;currentUid=uid;
 try{pending=JSON.parse(storage.getItem(pendingKey(uid))||'null')||pending;}catch{}
 if(pending?.uid!==uid)pending=null;
 if(pending&&['member_status','team_create','invitation_create','invitation_response'].includes(pending.body.operation)){const {operation,requestId,...values}=pending.body;try{return await saveProfile(operation,values);}catch(error){data.state.pendingError=error.message;}}
 if(pending){data.state.pendingSave=true;data.state.pendingNeedsPin=!!pending.needsPin;const own=data.state.profiles.find(p=>p.id===uid);if(own&&pending.body.operation==='profile'){Object.assign(own,pending.body.fields);own.revision=pending.body.expectedRevision;}}
 return viewPreferences(data.state);
}
export async function saveProfile(operation,values){
 if(!currentUid)throw Error('Recognize is required first.');
 const actor=currentUid,scopeGeneration=generation;
 const input={operation,...values};
 const fingerprintOf=async value=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(value)))),b=>b.toString(16).padStart(2,'0')).join('');
 const fingerprint=await fingerprintOf(input);
 if(actor!==currentUid||scopeGeneration!==generation)throw Error('Profile changed before this save.');
 if(pending&&pending.fingerprint!==fingerprint)throw Error('Retry the pending save before starting another change.');
 if(pending?.needsPin&&input.fields?.pin){pending.body.fields.pin=input.fields.pin;pending.needsPin=false;}
 if(!pending)pending={uid:currentUid,fingerprint,body:{...input,requestId:crypto.randomUUID()}};
 retainPending();const operationPending=pending;
 const response=await fetch(endpoint,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-RS-Actor-Scope':actor},body:JSON.stringify(operationPending.body),signal:AbortSignal.timeout(20000)});
 const data=await response.json();if(!response.ok||!data.ok){if([400,403,404].includes(response.status)||response.status===409&&['actor_changed','record_changed','profile_operation_not_connected','team_membership_exists','invitation_pending'].includes(data.error)){storage.removeItem(pendingKey(actor));if(pending===operationPending)pending=null;}throw Error(data.error||'The save has not been confirmed.');}
 if(actor!==currentUid||data.state?.identity.currentId!==actor)throw Error('Profile changed during this save.');
 storage.removeItem(pendingKey(actor));if(pending===operationPending)pending=null;return viewPreferences(data.state);
}
export function clearProfile(){generation++;currentUid=null;pending=null;}
