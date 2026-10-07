#!/usr/bin/env node
import { createHash, randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';

const JOURNEY = ['session-required', 'session-read-required', 'same-origin-required', 'state-before', 'known-recognition', 'recognition-does-not-create-roster', 'unknown-recognition', 'retired-recognition', 'create-barn', 'link-profile', 'link-profile-again', 'create-user', 'create-rider', 'create-location', 'create-horse', 'save-edit', 'stale-edit-rejected', 'identical-retry', 'reload'];
const PREREQUISITES = ['Observed test application base URL including its mount path; exact expected origin', 'Known isolated-trial environment and unique run ID', 'Explicit server-issued test session cookie (not a device token or invented principal)', 'Existing known fixture device token and expected canonical person ID', 'Optional existing retired fixture token for a read-only retired-device check'];
const requireCheck = (condition, code) => { if (!condition) throw Object.assign(new Error(code), { code }); };
const validId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,160}$/.test(value);
const stateShape = state => state?.version === 1 && ['barns', 'users', 'riders', 'locations', 'horses'].every(kind => Array.isArray(state[kind]));
const rosterSnapshot = state => JSON.stringify(['barns', 'users', 'riders', 'locations', 'horses'].map(kind => state[kind].map(row => [row.id, row.revision]).sort((a, b) => String(a[0]).localeCompare(String(b[0])))));

// API checks only. Does not provision identity, retire devices, delete records, or test browser interaction.
export async function run({ baseUrl, expectedOrigin, environment, cookie, deviceToken, retiredDeviceToken, expectedPersonId, runId, execute = false, fetchImpl = globalThis.fetch, onProgress = () => {}, timeoutMs = 15000 } = {}) {
  const report = { status: execute ? 'BLOCKED' : 'DRY_RUN', scope: 'API_ONLY', phases: JOURNEY.map(phase => ({ phase, status: 'NOT_RUN' })), ids: {}, prerequisites: PREREQUISITES, recordsRetained: true, limitations: ['Does not verify browser interaction, deployment configuration, or provider concurrency guarantees.', 'Synthetic operational records remain labelled QA for review; no cleanup or identity mutation is performed.', 'After a failure or unknown write outcome, inspect returned record/request IDs before rerunning.'] };
  if (!execute) return report;
  let endpoint, origin;
  try {
    const url = new URL(baseUrl);
    requireCheck(url.protocol === 'https:' && !url.username && !url.password && !url.search && !url.hash && !/%|\/\//.test(url.pathname), 'invalid_test_base_url');
    requireCheck(expectedOrigin === url.origin, 'expected_origin_required');
    requireCheck(environment === 'isolated-trial', 'isolated_trial_required');
    requireCheck(typeof cookie === 'string' && cookie.includes('=') && !/[\r\n]/.test(cookie), 'server_session_required');
    requireCheck(typeof deviceToken === 'string' && deviceToken.trim() && !/[\r\n]/.test(deviceToken), 'fixture_device_required');
    requireCheck(validId(expectedPersonId), 'expected_person_required');
    requireCheck(/^[A-Za-z0-9_-]{8,64}$/.test(runId || ''), 'unique_run_id_required');
    requireCheck(Number.isInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 60000, 'invalid_timeout');
    endpoint = `${url.origin}${url.pathname.replace(/\/$/, '')}/rs-inputs`;
    origin = url.origin;
  } catch (error) {
    report.errorCode = error.code || 'invalid_test_base_url';
    return report;
  }
  const requestId = phase => createHash('sha256').update(`rs-inputs-e2e|${runId}|${phase}`).digest('hex');
  const name = kind => `QA ${runId} ${kind}`;
  let activePhase;
  function notify(value) { try { onProgress(value); } catch { /* reporting cannot change the test outcome */ } }
  async function request(path, { body, authenticated = true, requestOrigin = origin, status = 200, errorCode } = {}) {
    const phaseReport = report.phases.find(row => row.phase === activePhase);
    if (validId(body?.requestId)) phaseReport.requestId = body.requestId;
    let response;
    try {
      response = await fetchImpl(`${endpoint}${path}`, { method: body ? 'POST' : 'GET', redirect: 'error', cache: 'no-store', signal: AbortSignal.timeout(timeoutMs), headers: { ...(authenticated ? { Cookie: cookie } : {}), ...(body ? { 'Content-Type': 'application/json', Origin: requestOrigin } : {}) }, ...(body ? { body: JSON.stringify(body) } : {}) });
    } catch { throw Object.assign(new Error('request_outcome_unknown'), { code: 'request_outcome_unknown' }); }
    phaseReport.httpStatus = response.status;
    const result = await response.json().catch(() => null);
    // Keep identifiers even when a broken authorization boundary unexpectedly writes a row.
    if (validId(result?.record?.id)) report.ids[activePhase] = result.record.id;
    requireCheck(response.status === status, 'unexpected_http_status');
    requireCheck(result && (errorCode ? result.ok === false && result.error === errorCode : result.ok === true), 'unexpected_response');
    return result;
  }
  async function phase(label, action) {
    activePhase = label;
    const row = report.phases.find(item => item.phase === label);
    try { const result = await action(); row.status = 'PASS'; notify({ phase: label, status: 'PASS', ...(report.ids[label] ? { id: report.ids[label] } : {}) }); return result; }
    catch (error) { row.status = 'FAIL'; report.status = 'FAIL'; report.failedPhase = label; report.errorCode = error.code || 'assertion_failed'; notify({ phase: label, status: 'FAIL' }); throw error; }
  }
  function recordBody(kind, draft, barnId = '', label = activePhase, expectedRevision) {
    return { kind, draft, barnId, requestId: requestId(label), ...(expectedRevision === undefined ? {} : { expectedRevision }) };
  }
  function saved(result, kind, expectedName, barnId) {
    requireCheck(validId(result?.record?.id) && Number.isInteger(result.record.revision) && stateShape(result.state), 'invalid_saved_record');
    requireCheck(result.record.name === expectedName && result.record.barnId === (barnId || result.record.id), 'saved_record_mismatch');
    const rows = result.state[kind === 'barn' ? 'barns' : kind];
    requireCheck(rows.filter(row => row.id === result.record.id).length === 1, 'saved_record_missing');
    return result.record;
  }
  try {
    const deniedBody = recordBody('barn', { name: name('boundary probe') }, '', 'boundary-probe');
    await phase('session-required', () => request('/record', { body: deniedBody, authenticated: false, status: 401, errorCode: 'authentication_required' }));
    await phase('session-read-required', () => request('/state', { authenticated: false, status: 401, errorCode: 'authentication_required' }));
    await phase('same-origin-required', () => request('/record', { body: deniedBody, requestOrigin: origin === 'https://cross-origin.invalid' ? 'https://other-origin.invalid' : 'https://cross-origin.invalid', status: 403, errorCode: 'origin_denied' }));
    const before = await phase('state-before', async () => { const r = await request('/state'); requireCheck(stateShape(r.state), 'invalid_state'); return r.state; });
    await phase('known-recognition', async () => {
      const r = await request(`/recognition?device_token=${encodeURIComponent(deviceToken)}`);
      requireCheck(r.recognized === true && r.device === 'active' && r.profile?.person_uid === expectedPersonId, 'known_person_mismatch');
    });
    await phase('recognition-does-not-create-roster', async () => { const r = await request('/state'); requireCheck(stateShape(r.state) && rosterSnapshot(r.state) === rosterSnapshot(before), 'recognition_changed_roster'); });
    await phase('unknown-recognition', async () => {
      const r = await request(`/recognition?device_token=${encodeURIComponent(`device_token_${randomUUID().replaceAll('-', '')}`)}`);
      // A verified input session supplies its invited principal before device confirmation.
      requireCheck(r.recognized === false && r.profile?.person_uid === expectedPersonId && r.device === 'unknown', 'unknown_device_mismatch');
    });
    if (retiredDeviceToken) await phase('retired-recognition', async () => { const r = await request(`/recognition?device_token=${encodeURIComponent(retiredDeviceToken)}`); requireCheck(r.recognized === false && r.profile === null && r.device === 'retired', 'retired_device_mismatch'); });
    else report.phases.find(item => item.phase === 'retired-recognition').status = 'SKIPPED_NO_FIXTURE';
    const barn = await phase('create-barn', async () => { const r = await request('/record', { body: recordBody('barn', { name: name('Barn') }) }); const row = saved(r, 'barn', name('Barn')); requireCheck(r.state.users.every(user => user.barnId !== row.id), 'implicit_roster_membership'); return row; });
    const linked = await phase('link-profile', async () => {
      const r = await request('/profile-link', { body: { barnId: barn.id, requestId: requestId('link-profile') } });
      requireCheck(validId(r.record?.id) && r.record.barnId === barn.id && r.record.recognitionPersonId === expectedPersonId && stateShape(r.state), 'linked_person_mismatch'); return r.record;
    });
    await phase('link-profile-again', async () => {
      const r = await request('/profile-link', { body: { barnId: barn.id, requestId: requestId('link-profile-again') } });
      requireCheck(stateShape(r.state) && r.record?.id === linked.id && r.state.users.filter(user => user.barnId === barn.id && user.recognitionPersonId === expectedPersonId).length === 1, 'duplicate_profile_link');
    });
    const user = await phase('create-user', async () => saved(await request('/record', { body: recordBody('users', { name: name('User'), email: '' }, barn.id) }), 'users', name('User'), barn.id));
    const rider = await phase('create-rider', async () => saved(await request('/record', { body: recordBody('riders', { name: name('Rider'), userId: user.id }, barn.id) }), 'riders', name('Rider'), barn.id));
    const location = await phase('create-location', async () => saved(await request('/record', { body: recordBody('locations', { name: name('Location'), address: 'QA synthetic trial location' }, barn.id) }), 'locations', name('Location'), barn.id));
    const horse = await phase('create-horse', async () => saved(await request('/record', { body: recordBody('horses', { name: name('Horse'), riderId: rider.id, locationId: location.id }, barn.id) }), 'horses', name('Horse'), barn.id));
    const editBody = recordBody('horses', { ...horse, name: name('Horse edited') }, barn.id, 'save-edit', horse.revision);
    const edited = await phase('save-edit', async () => { const row = saved(await request('/record', { body: editBody }), 'horses', name('Horse edited'), barn.id); requireCheck(row.id === horse.id && row.revision === horse.revision + 1, 'edit_revision_mismatch'); return row; });
    await phase('stale-edit-rejected', () => request('/record', { body: recordBody('horses', { ...horse, name: name('Horse stale') }, barn.id, 'stale-edit', horse.revision), status: 409, errorCode: 'record_changed' }));
    await phase('identical-retry', async () => { const r = await request('/record', { body: editBody }); requireCheck(r.record?.id === edited.id && r.record?.revision === edited.revision && r.record?.name === edited.name, 'idempotency_mismatch'); });
    await phase('reload', async () => {
      const r = await request(`/state?barn_id=${encodeURIComponent(barn.id)}`); const s = r.state;
      requireCheck(stateShape(s) && s.selectedBarn === barn.id, 'reload_state_mismatch');
      for (const [kind, row] of [['barns', barn], ['users', user], ['riders', rider], ['locations', location], ['horses', edited]]) requireCheck(s[kind].filter(item => item.id === row.id && item.name === row.name).length === 1, 'reload_record_mismatch');
      const reloadedHorse = s.horses.find(row => row.id === horse.id); const reloadedRider = s.riders.find(row => row.id === rider.id);
      requireCheck(reloadedHorse.revision === edited.revision && reloadedHorse.riderId === rider.id && reloadedHorse.locationId === location.id && reloadedRider.userId === user.id, 'reload_relationship_mismatch');
      requireCheck(s.users.filter(row => row.barnId === barn.id && row.recognitionPersonId === expectedPersonId).length === 1 && s.horses.filter(row => row.barnId === barn.id).length === 1, 'reload_duplicate_record');
    });
    report.status = report.phases.some(row => row.status === 'SKIPPED_NO_FIXTURE') ? 'PARTIAL' : 'PASS';
  } catch { /* fail first; preserve phase statuses and returned IDs, never log response bodies */ }
  return report;
}

export async function main(argv = process.argv.slice(2), env = process.env, dependencies = {}) {
  const options = {}; const valueFlags = ['--base-url', '--expected-origin', '--environment', '--run-id', '--cookie-env', '--device-token-env', '--retired-device-token-env', '--expected-person-id'];
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === '--run' || key === '--dry-run') { options[key.slice(2)] = true; continue; }
    if (!valueFlags.includes(key) || !argv[i + 1] || argv[i + 1].startsWith('--') || Object.hasOwn(options, key.slice(2))) throw new Error('Unsupported, duplicate, or incomplete argument. Secrets are accepted only through named environment variables.');
    options[key.slice(2)] = argv[++i];
  }
  if (options.run && options['dry-run']) throw new Error('Choose only one mode.');
  const secret = key => { const variable = options[key]; if (variable === undefined) return undefined; if (!/^[A-Z][A-Z0-9_]*$/.test(variable)) throw new Error('Invalid credential environment variable name.'); return env[variable]; };
  return run({ ...dependencies, baseUrl: options['base-url'], expectedOrigin: options['expected-origin'], environment: options.environment, runId: options['run-id'], cookie: secret('cookie-env'), deviceToken: secret('device-token-env'), retiredDeviceToken: secret('retired-device-token-env'), expectedPersonId: options['expected-person-id'], execute: !!options.run });
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2), process.env, { onProgress: value => process.stderr.write(`${JSON.stringify(value)}\n`) }).then(report => { process.stdout.write(`${JSON.stringify(report, null, 2)}\n`); if (report.status === 'PARTIAL') process.exitCode = 2; else if (['FAIL', 'BLOCKED'].includes(report.status)) process.exitCode = 1; }).catch(error => { process.stderr.write(`${JSON.stringify({ status: 'BLOCKED', error: error.message })}\n`); process.exitCode = 1; });
}
