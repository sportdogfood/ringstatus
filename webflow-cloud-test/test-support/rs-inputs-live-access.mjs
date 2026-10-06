// Isolated local-handler integration driver. Requires an authorized MCP controller
// on stdin/stdout. Never imported by application routes and never opens a server.
// Authentication is a fixture: this does NOT verify sign-in, deployed HTTP, or UI.
import { createInterface } from 'node:readline';
import { writeFile } from 'node:fs/promises';
import { handleInputRoute } from '../src/pages/rs-inputs/[operation].js';
import { handleAuthenticatedInputRoute } from '../src/pages/rs-inputs/[operation].js';
import { issueAccess } from '../scripts/rs-inputs-access.mjs';

const runId = process.argv[2];
const reportPath = process.argv[3];
if (!/^rs_auth_[A-Za-z0-9_]{8,48}$/.test(runId || '') || !reportPath) throw new Error('Explicit unique fixture run ID and report path required');
const baseId = 'app9kOZdIaGyKk5uG';
const origin = 'https://local-handler.invalid'; // Request object only; never navigated or fetched.
const allowed = new Set(['rs_people_test', 'rs_devices_test', 'rs_phone_aliases_test', 'rs_recognition_sessions_test', 'rs_input_barns', 'rs_input_users', 'rs_input_riders', 'rs_input_horses', 'rs_input_locations', 'rs_input_events']);
const lines = createInterface({ input: process.stdin, terminal: false });
let nextId = 0;
const pending = new Map();
const expired = new Set();
const emit = value => process.stdout.write(JSON.stringify(value) + '\n');
lines.on('line', line => {
  const response = JSON.parse(line);
  const request = pending.get(response.id);
  if (!request && expired.has(response.id)) return; // A timeout leaves write outcome unknown.
  if (!request) throw new Error('Unexpected bridge response');
  pending.delete(response.id);
  request.resolve(Response.json(response.body, { status: response.status }));
});
lines.on('close', () => { for (const request of pending.values()) request.reject(new Error('Connector controller disconnected')); });
async function connectorFetch(value, options = {}) {
  const url = new URL(value);
  const path = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
  if (url.origin !== 'https://api.airtable.com' || path[0] !== 'v0' || path[1] !== baseId || !allowed.has(path[2]) || path.length > 4) throw new Error('Connector target outside isolated test');
  const id = ++nextId;
  const result = new Promise((resolve, reject) => {
    let timer;
    const clear = () => { clearTimeout(timer); options.signal?.removeEventListener('abort', abort); };
    const abort = () => { clear(); pending.delete(id); expired.add(id); reject(new Error('Connector outcome unknown after timeout/abort')); };
    if (options.signal?.aborted) { reject(new Error('Request already aborted')); return; }
    timer = setTimeout(abort, 45000);
    options.signal?.addEventListener('abort', abort, { once: true });
    pending.set(id, { resolve: value => { clear(); resolve(value); }, reject: error => { clear(); reject(error); } });
  });
  if (!pending.has(id)) return result;
  // Intentionally omit Authorization and all other headers: MCP owns credentials.
  emit({ type: 'connector-request', id, url: url.href, method: options.method || 'GET', body: options.body ? JSON.parse(options.body) : undefined });
  return result;
}
const env = { RS_INPUTS_BASE_ID:baseId, RS_INPUTS_RECOGNITION_BASE_ID:baseId, AIRTABLE_TOKEN:'CONNECTOR_TEST_ONLY', RS_INPUTS_WRITE_MODE:'isolated-trial', RS_INPUTS_SESSION_SECRET:[...crypto.getRandomValues(new Uint8Array(32))].map(b=>b.toString(16).padStart(2,'0')).join('') };
const uid=part=>`${runId}_${part}`;
const phases=[], fixtures={};let cookie,report,unknownCreation=false;
const route=(op,body,authenticated=true)=>handleAuthenticatedInputRoute({request:new Request(`${origin}/rs-inputs/${op}`,{method:body?'POST':'GET',headers:{...(body?{Origin:origin,'Content-Type':'application/json'}:{}),...(authenticated&&cookie?{Cookie:cookie}:{})},...(body?{body:JSON.stringify(body)}:{})}),locals:{}},env,connectorFetch);
async function table(table,method='GET',body,formula){const url=new URL(`https://api.airtable.com/v0/${baseId}/${table}`);if(formula)url.searchParams.set('filterByFormula',formula);const res=await connectorFetch(url,{method,...(body?{body:JSON.stringify(body)}:{})});if(!res.ok)throw Error('Provider failure');return res.json();}
async function check(label,response,status=200){const body=await response.json();if(response.status!==status)throw Error(`${label}: ${response.status}/${body.error}`);phases.push({phase:label,status:'PASS'});emit({type:'progress',phase:label,status:'PASS'});return body;}
try{
 try{fixtures.person=(await table('rs_people_test','POST',{records:[{fields:{person_uid:uid('person'),person_name:`QA ${runId} Invited`,status:'Active',access_level:'Guest'}}]})).records[0];if(!fixtures.person?.id)throw Error('Missing fixture');}catch(e){unknownCreation=true;throw e;}
 await check('no-session-denied',await route('state',undefined,false),401);
 const grant=await issueAccess({env,personUid:uid('person'),decision:'invited',fetchImpl:connectorFetch});
 const accepted=await route('access',{token:grant.token},false);cookie=accepted.headers.get('Set-Cookie')?.split(';')[0];await check('invite-redeemed-cookie-issued',accepted);if(!cookie)throw Error('Cookie missing');
 await check('same-invitation-replay-rejected',await route('access',{token:grant.token},false),401);
 await check('signed-session-verified',await route('access'));
 const candidate=await check('invited-profile-before-device',await route('recognition'));if(candidate.recognized||candidate.profile?.person_uid!==uid('person'))throw Error('Invited identity mismatch');
 await check('explicit-new-device-association',await route('recognition',{action:'confirm_device',device_token:uid('device_token'),values:{},requestId:uid('confirm')}));
 const recognized=await check('recognized-after-confirm',await route(`recognition?device_token=${uid('device_token')}`));if(!recognized.recognized||recognized.profile?.person_uid!==uid('person'))throw Error('Recognition mismatch');
 const barn=await check('cookie-authorized-barn-create',await route('record',{kind:'barn',draft:{name:`QA ${runId} Barn`},requestId:uid('barn')}));fixtures.barnId=barn.record.id;
 const linked=await check('explicit-person-roster-link',await route('profile-link',{barnId:fixtures.barnId,requestId:uid('link')}));if(linked.record.recognitionPersonId!==uid('person'))throw Error('Wrong person linked');
 const horse=await check('horse-create',await route('record',{kind:'horses',barnId:fixtures.barnId,draft:{name:`QA ${runId} Horse`},requestId:uid('horse')}));fixtures.horseId=horse.record.id;
 const edit={kind:'horses',barnId:fixtures.barnId,draft:{id:fixtures.horseId,name:`QA ${runId} Horse saved`},expectedRevision:horse.record.revision,requestId:uid('edit')};
 await check('inline-edit-save',await route('record',edit));await check('inline-edit-retry-same-record',await route('record',edit));await check('stale-edit-rejected',await route('record',{...edit,requestId:uid('stale')}),409);
 const state=await check('fresh-reload',await route('state'));if(state.state.horses.filter(h=>h.id===fixtures.horseId).length!==1||state.state.horses.find(h=>h.id===fixtures.horseId).revision!==2)throw Error('Save readback mismatch');
 await issueAccess({env,personUid:uid('person'),decision:'revoked',fetchImpl:connectorFetch});await check('revoked-session-denied',await route('state'),401);
 await check('logout-cookie-cleared',await route('logout',{}));
 report={runId,status:'PASS',scope:'LOCAL_SIGNED_SESSION_HANDLER_LIVE_AIRTABLE_VIA_MCP',phases,fixtures,signedCookieHandlerVerified:true,browserVerified:false,deployedHttpVerified:false,nativeRestCredentialVerified:false,atomicityVerified:false};
}catch(error){report={runId,status:'FAIL',error:String(error.message),phases,fixtures,unknownCreation};}
finally{
 try{
  if(fixtures.person?.id){await table('rs_people_test','PATCH',{records:[{id:fixtures.person.id,fields:{status:'Inactive',input_access:'revoked',input_invite_hash:'',input_invite_expires_at:null,input_session_version:crypto.randomUUID().replaceAll('-','')}}]});}
  const devices=await table('rs_devices_test','GET',undefined,`{device_token} = '${uid('device_token')}'`);fixtures.devices=devices.records.map(r=>r.id);for(const r of devices.records)await table('rs_devices_test','PATCH',{records:[{id:r.id,fields:{status:'Retired'}}]});
  const sessions=await table('rs_recognition_sessions_test','GET',undefined,`{session_event_uid} = 'inputs_${uid('confirm')}'`);fixtures.recognitionAudits=sessions.records.map(r=>r.id);for(const r of sessions.records)await table('rs_recognition_sessions_test','PATCH',{records:[{id:r.id,fields:{automation_status:'processed',automation_processed_at:new Date().toISOString()}}]});
  report.cleanup='Completed writes; requires independent fresh readback. Recognition event manually marked processed, not automation evidence.';
 }catch(error){report.cleanup='UNVERIFIED: '+error.message;report.status='FAIL';}
 report.fixtures=fixtures;await writeFile(reportPath,JSON.stringify(report,null,2)+'\n');emit({type:'complete',report});lines.close();
}
