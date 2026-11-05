import {mountRecognize} from './widget-view.js';
import {runSilentRecognition,flushIamHistory,safeStorage,browserStorage} from './native-client.js';

const endpoint=new URL('/test/rs-inputs/native-recognition',location.origin);
const tokenKey='rs_recognition_device_token_v1',sessionKey='rs_native_recognition_session_v1';
const persistentStorage=safeStorage(browserStorage('localStorage')),visitStorage=safeStorage(browserStorage('sessionStorage'));
const pendingKey='rs_recognition_pending_signout_v1';
let pendingCredential=null,signOutWork=null,deviceFingerprint=null;
function session(){let uid=visitStorage.getItem(sessionKey);if(!uid){uid=crypto.randomUUID();visitStorage.setItem(sessionKey,uid);}return uid;}
async function sendAction(body){
 const target=new URL(endpoint);target.searchParams.set('operation','action');
 const response=await fetch(target,{method:'POST',credentials:'same-origin',signal:AbortSignal.timeout(12000),headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
 const data=await response.json();if(!response.ok||!data.ok)throw Error(data.error||'recognition_unavailable');
 if(data.history_event_uid)void flushIamHistory({endpoint,persistentStorage,eventUid:data.history_event_uid});
 return data;
}
// One browser-wide action lock keeps clearing-cookie responses ahead of any
// later credential response, including when separate tabs share the cookie.
function exclusive(work){return navigator.locks?navigator.locks.request('rs-recognition-actions',work):work();}
async function drainSignOut(){
 const raw=persistentStorage.getItem(pendingKey),saved=JSON.parse(raw||'null');if(!saved)return;
 await sendAction(saved.body);if(persistentStorage.getItem(pendingKey)===raw)persistentStorage.removeItem(pendingKey);
}
function finishSignOut(){
 if(!signOutWork)signOutWork=exclusive(drainSignOut).finally(()=>{signOutWork=null;});return signOutWork;
}
async function action(body){return exclusive(async()=>{await drainSignOut();return sendAction(body);});}
export const recognitionAdapter={
 profileDeviceFingerprint(){return deviceFingerprint;},
 async current(){
  if(persistentStorage.getItem(pendingKey)){void finishSignOut().catch(()=>{});return null;}
  let result;await exclusive(async()=>{if(persistentStorage.getItem(pendingKey))return;await runSilentRecognition({endpoint,persistentStorage,sessionStorageImpl:visitStorage,redirectOnRecognized:false,onOutcome:value=>{result=value;}});});
  if(result?.outcome==='unavailable')throw Error('recognition_unavailable');
  deviceFingerprint=result?.data?.device_fingerprint||null;
  return result?.outcome==='recognized'?result.data.person_uid:null;
 },
 async recognize(value){
  await finishSignOut();
  const digits=String(value).replace(/[\s()+.-]/g,'');
  const kind=/^\d{4}$/.test(digits)?'pin':/^\d{7}$/.test(digits)?'invite':'phone';
  if(!pendingCredential||pendingCredential.value!==digits)pendingCredential={action:'credential',kind,value:digits,request_id:crypto.randomUUID(),session_uid:crypto.randomUUID()};
  const credentialSession=pendingCredential.session_uid,data=await action(pendingCredential);pendingCredential=null;
  if(!data.verified)return null;
  visitStorage.setItem(sessionKey,credentialSession);
  persistentStorage.setItem(tokenKey,data.device_token);
  return recognitionAdapter.current();
 },
 async signOut(){
  const body={action:'retire_device',device_fingerprint:deviceFingerprint,session_uid:session(),session_event_uid:crypto.randomUUID()};
  persistentStorage.setItem(pendingKey,JSON.stringify({body,continuation:'login'}));
  persistentStorage.removeItem('rs_recognition_identity_v1');persistentStorage.removeItem('rs_recognition_device_expires_v1');persistentStorage.removeItem(tokenKey);
  visitStorage.removeItem('rs_native_recognition_confirmation_v1');visitStorage.setItem(sessionKey,crypto.randomUUID());
  await finishSignOut();
 }
};

export function mountRecognitionWidget(root){
 if(!root||root.__rsRecognitionWidget)return root?.__rsRecognitionWidget;
 root.hidden=true;
 const work=import('/test/rs-profile-v3/embed.js');root.__rsRecognitionWidget=work;
 void work.catch(()=>{root.setAttribute('data-rs-recognition-status','unavailable');});
 return work;
}
