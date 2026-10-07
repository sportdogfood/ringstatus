// Dedicated isolated-trial fixture only. Never uses the owner's browser/session.
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { issueAccess } from './rs-inputs-access.mjs';
export const TARGET = 'https://ringstatus.webflow.io/test';
export const BASE = 'app9kOZdIaGyKk5uG';
const check = (condition, code) => { if (!condition) throw Object.assign(new Error(code), { code }); };

export async function runRecognitionLive({ execute = false, env = process.env, fetchImpl = fetch } = {}) {
  const report = { status: execute ? 'BLOCKED' : 'DRY_RUN', scope: 'DEPLOYED_RECOGNITION_API', target: TARGET, phases: [], cleanup: 'NOT_NEEDED', limitations: ['API test; does not verify browser UI, cookie storage, layout, or scheduled execution.', 'Recognition audit records remain as evidence; downstream automation is not claimed.'] };
  if (!execute) return report;
  const uid = env.RS_CI_PERSON_UID;
  if (env.RS_CI_TARGET !== TARGET || !/^rs_ci_[A-Za-z0-9_]{4,80}$/.test(uid || '') || !env.RS_CI_AIRTABLE_TOKEN) {
    report.errorCode = 'dedicated_fixture_configuration_required';
    return report;
  }
  let unknownWriteOutcome = false;
  const networkFetch = fetchImpl;
  fetchImpl = async (input, init = {}) => {
    try { const response = await networkFetch(input, init); if (init.method && init.method !== 'GET' && response.status >= 500) unknownWriteOutcome = true; return response; }
    catch (error) { if (init.method && init.method !== 'GET') unknownWriteOutcome = true; throw error; }
  };
  const bindings = { AIRTABLE_TOKEN: env.RS_CI_AIRTABLE_TOKEN, RS_INPUTS_BASE_ID: BASE, RS_INPUTS_RECOGNITION_BASE_ID: BASE, RS_INPUTS_WRITE_MODE: 'isolated-trial' };
  const runId = `rs_ci_${randomUUID().replaceAll('-', '')}`;
  const deviceToken = `device_token_${runId}`;
  const origin = new URL(TARGET).origin;
  let person, cookie = '', cleanupRequired = false, failed = false, activePhase = 'fixture-preflight';
  report.runId = runId;
  async function table(name, { formula, method = 'GET', records } = {}) {
    check(['rs_people_test','rs_devices_test','rs_recognition_sessions_test'].includes(name), 'unexpected_fixture_table');
    const url = new URL(`https://api.airtable.com/v0/${BASE}/${name}`);
    if (formula) { url.searchParams.set('filterByFormula', formula); url.searchParams.set('maxRecords','2'); }
    const response = await fetchImpl(url, { method, redirect: 'error', signal: AbortSignal.timeout(15000), headers: { Authorization: `Bearer ${bindings.AIRTABLE_TOKEN}`, 'Content-Type':'application/json' }, ...(records ? { body: JSON.stringify({ records }) } : {}) });
    if (method !== 'GET' && response.status >= 500) unknownWriteOutcome = true;
    check(response.ok, 'fixture_storage_unavailable');
    let body;
    try { body = await response.json(); } catch (error) { if (method !== 'GET') unknownWriteOutcome = true; throw error; }
    check(Array.isArray(body.records) && !body.offset && body.records.length <= 1, 'ambiguous_fixture');
    return body.records;
  }
  async function api(path, { body, authenticated = true, expectedStatus = 200, expectedError, requestOrigin = origin } = {}) {
    const response = await fetchImpl(`${TARGET}/rs-inputs/${path}`, { method: body ? 'POST' : 'GET', redirect:'error', signal:AbortSignal.timeout(20000), headers:{ ...(authenticated && cookie ? {Cookie:cookie}:{}), ...(body ? {'Content-Type':'application/json',Origin:requestOrigin}:{}) }, ...(body ? {body:JSON.stringify(body)}:{}) });
    const result = await response.json().catch(()=>null);
    if (body && !result) unknownWriteOutcome = true;
    if (body && response.status >= 500) unknownWriteOutcome = true;
    check(response.status === expectedStatus, 'unexpected_http_status');
    check(expectedError ? result?.ok === false && result.error === expectedError : result?.ok === true, 'unexpected_response');
    return { response, result };
  }
  async function phase(name, action) {
    activePhase = name;
    await action();
    report.phases.push({ phase:name, status:'PASS' });
  }
  try {
    await phase('fixture-preflight',async()=>{
      [person] = await table('rs_people_test',{formula:`{person_uid} = '${uid}'`});
      check(person?.fields?.person_uid === uid && /^QA CI Recognize\b/.test(person.fields.person_name || '') && ['active','test'].includes(String(person.fields.status).toLowerCase()),'dedicated_fixture_required');
      report.personRecordId = person.id;
    });
    await phase('unauthenticated-denied',()=>api('access',{authenticated:false,expectedStatus:401,expectedError:'authentication_required'}));
    await phase('fake-invitation-rejected',()=>api('access',{body:{token:'A'.repeat(43)},authenticated:false,expectedStatus:401,expectedError:'invalid_invitation'}));
    await phase('wrong-origin-denied',()=>api('access',{body:{token:'A'.repeat(43)},authenticated:false,requestOrigin:'https://cross-origin.invalid',expectedStatus:403,expectedError:'origin_denied'}));
    cleanupRequired = true; // A failed issuance can have an unknown write outcome.
    let grant;
    await phase('issue-dedicated-invitation',async()=>{grant=await issueAccess({env:bindings,personUid:uid,decision:'invited',fetchImpl});});
    await phase('accept-invitation',async()=>{
      const {response,result}=await api('access',{body:{token:grant.token},authenticated:false});
      const header=response.headers.get('Set-Cookie')||'';
      check(result.actor?.id===uid && result.actor.profile?.personUid===uid,'wrong_signed_identity');
      check(header.startsWith('__Secure-rs_input_access=') && /; HttpOnly(?:;|$)/.test(header) && /; Secure(?:;|$)/.test(header) && /; SameSite=Strict(?:;|$)/.test(header) && /; Path=\/test\/rs-inputs(?:;|$)/.test(header),'invalid_session_cookie');
      cookie=header.split(';')[0];
    });
    await phase('invitation-replay-denied',()=>api('access',{body:{token:grant.token},authenticated:false,expectedStatus:401,expectedError:'invalid_invitation'}));
    await phase('invitation-consumed-in-storage',async()=>{const [row]=await table('rs_people_test',{formula:`{person_uid} = '${uid}'`});check(row?.id===person.id && !row.fields.input_invite_hash && !row.fields.input_invite_expires_at,'invitation_not_consumed');});
    await phase('signed-session-reloaded',async()=>{const {result}=await api('access');check(result.actor?.id===uid,'wrong_signed_identity');});
    await phase('invited-profile-before-confirmation',async()=>{const {result}=await api(`recognition?device_token=${deviceToken}`);check(result.recognized===false && result.profile?.person_uid===uid,'unexpected_preconfirmation_identity');});
    const confirmation={action:'confirm_device',device_token:deviceToken,values:{},requestId:`${runId}_confirm`};
    const recognized=result=>check(result.recognized===true && result.device==='active' && result.profile?.person_uid===uid,'recognition_mismatch');
    await phase('confirm-browser-association',async()=>recognized((await api('recognition',{body:confirmation})).result));
    await phase('identical-confirmation-retry',async()=>recognized((await api('recognition',{body:confirmation})).result));
    await phase('fresh-recognition-request',async()=>recognized((await api(`recognition?device_token=${deviceToken}`)).result));
    await phase('device-and-audit-readback',async()=>{
      const [device]=await table('rs_devices_test',{formula:`{device_token} = '${deviceToken}'`});
      check(device?.fields?.status==='Active' && device.fields.person?.length===1 && device.fields.person[0]===person.id,'device_storage_mismatch');
      const [audit]=await table('rs_recognition_sessions_test',{formula:`{session_event_uid} = 'inputs_${confirmation.requestId}'`});
      check(audit?.fields?.person?.[0]===person.id && audit.fields.device?.[0]===device.id && audit.fields.event_type==='success' && audit.fields.event_result==='matched','audit_storage_mismatch');
      report.deviceRecordId=device.id;report.auditRecordId=audit.id;
    });
    await phase('retire-browser-association',async()=>{const {result}=await api('recognition',{body:{action:'retire_device',device_token:deviceToken,values:{},requestId:`runId_${runId}_retire`}});check(result.recognized===false && result.device==='retired','retirement_failed');});
    await phase('retired-device-denied',async()=>{const {result}=await api(`recognition?device_token=${deviceToken}`);check(result.recognized===false && result.device==='retired','retired_device_recognized');});
    await phase('revoke-signed-session',()=>issueAccess({env:bindings,personUid:uid,decision:'revoked',fetchImpl}));
    await phase('revoked-session-denied',()=>api('access',{expectedStatus:401,expectedError:'authentication_required'}));
    await phase('logout-clears-cookie',async()=>{const {response}=await api('logout',{body:{}});check(/Max-Age=0/.test(response.headers.get('Set-Cookie')||''),'logout_cookie_not_cleared');});
  } catch(error) {
    if (error?.code === 'write_outcome_unknown') unknownWriteOutcome = true;
    failed=true;report.status='FAIL';report.failedPhase=activePhase;report.errorCode=/^[a-z_]+$/.test(error?.code||'')?error.code:'request_or_assertion_failed';report.phases.push({phase:activePhase,status:'FAIL'});
  } finally {
    if(cleanupRequired) {
      const cleanupErrors=[];
      try {await issueAccess({env:bindings,personUid:uid,decision:'revoked',fetchImpl});}catch{cleanupErrors.push('access_revocation_failed');}
      try {
        const devices=await table('rs_devices_test',{formula:`{device_token} = '${deviceToken}'`});
        for(const device of devices){check(device.fields.person?.length===1 && device.fields.person[0]===person.id,'cleanup_owner_mismatch');await table('rs_devices_test',{method:'PATCH',records:[{id:device.id,fields:{status:'Retired'}}]});}
        const [personAfter]=await table('rs_people_test',{formula:`{person_uid} = '${uid}'`});
        check(personAfter?.fields?.input_access==='revoked' && !personAfter.fields.input_invite_hash && !personAfter.fields.input_invite_expires_at,'cleanup_access_readback_failed');
        const remaining=await table('rs_devices_test',{formula:`{device_token} = '${deviceToken}'`});
        check(remaining.every(row=>row.fields.status==='Retired'),'cleanup_device_readback_failed');
      }catch{cleanupErrors.push('fixture_cleanup_unverified');}
      report.cleanup=cleanupErrors.length?'FAIL':unknownWriteOutcome?'UNVERIFIED_WRITE_OUTCOME':'PASS';
      if (unknownWriteOutcome) {report.unknownWriteOutcome=true;failed=true;}
      if(cleanupErrors.length){report.cleanupErrors=cleanupErrors;failed=true;}
    }
  }
  report.status=failed?'FAIL':'PASS';return report;
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href){
  const args=process.argv.slice(2);
  if(args.some(arg=>arg!=='--run')||args.length>1){console.error('Use --run or no arguments (dry run).');process.exitCode=1;}
  else {const report=await runRecognitionLive({execute:args[0]==='--run'});console.log(JSON.stringify(report,null,2));if(['FAIL','BLOCKED'].includes(report.status))process.exitCode=1;}
}
