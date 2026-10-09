import { recognitionConfig } from './rs-recognition-config.js';
import { handleAuthenticatedInputRoute } from '../pages/rs-inputs/[operation].js';
import { handleAccessRoute, createAccessStore, accessHash } from './rs-inputs-access.js';
import { createInputRecognition } from './rs-inputs-recognition.js';
import { recordRecognitionSession } from './rs-recognition-session.js';
import { createRecoverySms, requireSmsAutomation } from './rs-recognition-sms.js';

const respond = (body, status = 200) => Response.json(body, { status, headers: { 'Cache-Control': 'no-store' } });
const safePerson = p => p ? Object.fromEntries([
  ['person_uid', p.person_uid], ['person_name', p.person_name], ['first_name', p.first_name],
  ['last_name', p.last_name], ['primary_phone_e164', p.sms], ['email', p.email]
].map(([key, value]) => [key, typeof value === 'string' ? value : ''])) : {};

// A recognition-only façade for the existing signed-session contract. It must
// be mounted inside /rs-inputs so the existing path-scoped cookie reaches it.
export async function handleNativeRecognition(request, env, fetchImpl = fetch) {
  try {
    const config = recognitionConfig(env);
    if (env.RS_INPUTS_BASE_ID && env.RS_INPUTS_BASE_ID !== config.baseId) return respond({ ok: false, error: 'recognition_base_mismatch' }, 503);
    const bindings = { ...env, RS_INPUTS_BASE_ID: config.baseId, RS_INPUTS_RECOGNITION_BASE_ID: config.baseId };
    const incoming = new URL(request.url);
    if (!/\/rs-inputs\/native-recognition$/.test(incoming.pathname)) return respond({ ok: false, error: 'native_cookie_scope_required' }, 400);
    const operation = incoming.searchParams.get('operation');
    if (!['recognize', 'device', 'action', 'session', 'sms_prepare', 'sms_outcome'].includes(operation)) return respond({ ok: false, error: 'unsupported_operation' }, 400);
    if (request.method !== (operation === 'device' ? 'GET' : 'POST')) return respond({ ok: false, error: 'method_not_allowed' }, 405);
    const target = new URL(request.url);
    target.pathname = incoming.pathname.replace(/native-recognition$/, operation === 'session' ? 'session' : 'recognition');
    target.search = '';
    const payload = operation === 'device' ? null : await request.json().catch(() => null);
    if (operation !== 'device' && (!payload || Array.isArray(payload))) return respond({ ok: false, error: 'invalid_request' }, 400);
    if (operation === 'recognize') {
      if (request.headers.get('Origin') !== incoming.origin) return respond({ ok: false, error: 'origin_denied' }, 403);
      const cookies = (request.headers.get('Cookie') || '').split(';').map(v => v.trim()).filter(v => v.startsWith('__Host-rs_recognition_device='));
      if (cookies.length > 1) return respond({ ok: false, error: 'invalid_device_token' }, 400);
      const token = cookies.length ? cookies[0].slice('__Host-rs_recognition_device='.length) : payload.device_token;
      // Do not revive the historical timestamp/Math.random token fallback.
      if (typeof token !== 'string' || !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(token)) return respond({ ok: true, recognized: false });
      if (typeof payload.session_uid !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(payload.session_uid)) return respond({ ok: false, error: 'invalid_session_uid' }, 400);
      const recognition = createInputRecognition({ env: bindings, fetchImpl, verifiedInputAccess: true });
      const found = await recognition.recognizeDevice(token);
      const response = respond({ ok: true, recognized: found.recognized, ...(found.recognized ? { first_name: found.profile.first_name, person_name: found.profile.person_name } : {}) });
      if (!found.recognized) {
        response.headers.set('Set-Cookie', '__Host-rs_recognition_device=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax');
        return response;
      }
      // Reuse the existing canonical session/Geo-IP logger. This is recognition,
      // not permission to edit profiles or access protected Inputs routes.
      const eventUid = await accessHash(`${payload.session_uid}:${token}`);
      await recordRecognitionSession({ env: bindings, fetchImpl, request, payload: {
        session_uid: payload.session_uid, session_event_uid: eventUid,
        idempotency_key: `native_silent:${eventUid}`, event_type: 'recognition',
        event_result: 'matched', recognition_status: 'confirmed', matched_by: 'device_token',
        person_record_id: found.personRecordId, device_record_id: found.deviceRecordId, detail: { source: 'native_silent_recognition' }
      } });
      response.headers.set('Set-Cookie', `__Host-rs_recognition_device=${token}; Path=/; Max-Age=31536000; HttpOnly; Secure; SameSite=Lax`);
      return response;
    }
    if (operation === 'sms_prepare' || operation === 'sms_outcome') {
      requireSmsAutomation(request, env);
      const sms = createRecoverySms({ env: bindings, fetchImpl });
      return respond(operation === 'sms_prepare' ? await sms.prepare(payload.request_record_id) : await sms.outcome(payload));
    }
    if (operation === 'action' && payload.action === 'recovery') {
      if (request.headers.get('Origin') !== incoming.origin) return respond({ ok: false, error: 'origin_denied' }, 403);
      return respond(await createRecoverySms({ env: bindings, fetchImpl }).request(payload));
    }
    const deviceToken = operation === 'device' ? incoming.searchParams.get('device_token') : payload.device_token;
    if (typeof deviceToken !== 'string' || !deviceToken || deviceToken.length > 255) return respond({ ok: false, error: 'invalid_device_token' }, 400);
    if (operation === 'device') target.searchParams.set('device_token', deviceToken);
    if (operation === 'session' || (operation === 'action' && ['create_profile', 'recovery'].includes(payload.action))) {
      const forwarded = new Request(target, { method: request.method, headers: request.headers });
      return await handleAccessRoute(forwarded, bindings, fetchImpl, async actor => {
        if (operation === 'action') return respond({ ok: false, error: payload.action === 'recovery' ? 'sms_recovery_unconfigured' : 'invitation_only' }, payload.action === 'recovery' ? 503 : 403);
        const recognition = createInputRecognition({ env: bindings, fetchImpl, principalPersonUid: actor.id, verifiedInputAccess: true });
        const lookup = await recognition.lookup(deviceToken);
        const person = await createAccessStore({ env: bindings, fetchImpl }).byUid(actor.id);
        // Client IDs, results and arbitrary detail never select log identity.
        await recordRecognitionSession({ env: bindings, fetchImpl, request, payload: {
          session_uid: payload.session_uid, session_event_uid: payload.session_event_uid,
          idempotency_key: `native_lookup:${payload.session_event_uid}`, event_type: 'recognition',
          event_result: lookup.recognized ? 'matched' : 'not_matched',
          recognition_status: lookup.recognized ? 'confirmed' : 'rejected', matched_by: lookup.recognized ? 'device_token' : 'none',
          person_record_id: person.id, detail: { source: 'native_invited_lookup' }
        } });
        return respond({ ok: true });
      });
    }
    const values = operation === 'action' ? Object.fromEntries([
      ['person_name', payload.user], ['first_name', payload.first], ['last_name', payload.last],
      ['sms', payload.sms], ['pin', payload.pin], ['email', payload.email], ['identifier', payload.sms]
    ].filter(([, value]) => typeof value === 'string')) : null;
    const forwarded = new Request(target, { method: request.method, headers: request.headers,
      ...(operation === 'action' ? { body: JSON.stringify({ action: payload.action, requestId: payload.session_event_uid, device_token: deviceToken, values }) } : {}) });
    const response = await handleAuthenticatedInputRoute({ request: forwarded }, bindings, fetchImpl);
    if (response.status === 401 && operation === 'action' && payload.action === 'phone_login') {
      if (request.headers.get('Origin') !== incoming.origin) return respond({ ok: false, error: 'origin_denied' }, 403);
      // Reuse the existing SMS queue; a typed number is not an access credential.
      return respond(await createRecoverySms({ env: bindings, fetchImpl }).request({ session_event_uid: payload.session_event_uid, sms: payload.sms }));
    }
    const data = await response.json();
    if (!response.ok || !data.ok) return respond({ ok: false, error: data.error || 'recognition_unavailable' }, response.status);
    return respond({ ok: true, recognized: data.recognized === true, invited_profile: !!data.profile,
      requires_device_confirmation: !!data.profile && data.device !== 'active', device_state: data.device,
      ...safePerson(data.profile), ...(payload?.action === 'retire_device' ? { retired: true } : {}),
      ...(payload?.action === 'confirm_device' ? { confirmed: data.recognized === true } : {}) });
  } catch (error) {
    return respond({ ok: false, error: /^[a-z_]+$/.test(error.code || '') ? error.code : 'recognition_unavailable' }, error.status || 503);
  }
}
