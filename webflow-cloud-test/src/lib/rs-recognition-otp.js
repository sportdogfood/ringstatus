import { createInputRecognition, InputRecognitionError } from './rs-inputs-recognition.js';
import { createRecoverySms } from './rs-recognition-sms.js';
import { createOtpStore } from './rs-recognition-otp-store.js';

// Existing Airtable-native Twilio delivery; code checks stay in the server.
export async function handleRecognitionOtp({ request, env, payload, fetchImpl = fetch,
  recognitionFactory = createInputRecognition, uuid = () => crypto.randomUUID() }) {
  const fail = (code, status = 400) => { throw new InputRecognitionError(code, status); };
  if (request.headers.get('Origin') !== new URL(request.url).origin) fail('origin_denied', 403);
  if (env.RS_INPUTS_WRITE_MODE !== 'isolated-trial') fail('recognition_writes_disabled', 503);
  const store = createOtpStore(env);
  const digits = typeof payload.sms === 'string' ? payload.sms.replace(/\D/g, '') : '';
  const phone = '+' + (digits.length === 10 ? '1' + digits : digits);
  if (!/^\+1\d{10}$/.test(phone)) fail('invalid_sms');
  // Keep the owner's exact live-test recipient restriction until release approval.
  const allowed = String(env.RS_RECOGNITION_SMS_TEST_RECIPIENT || '').replace(/\D/g, '');
  if (!allowed || phone !== '+' + allowed) fail('otp_recipient_not_allowed', 403);
  if (!['otp_start', 'otp_check'].includes(payload.action)) fail('unsupported_action');
  if (payload.action === 'otp_check' && (!/^\d{6}$/.test(payload.code || '') || !/^otp_[a-f0-9-]{36}$/i.test(payload.otp_request_id || ''))) fail('invalid_otp');
  const recognition = recognitionFactory({ env, fetchImpl, verifiedInputAccess: true });
  const candidate = await recognition.verificationCandidate(phone);
  const requestId = 'otp_' + uuid();
  const neutral = { ok: true, accepted: true, otp_required: true, otp_request_id: requestId };
  if (!candidate) return payload.action === 'otp_start' ? neutral : { ok: true, verified: false };
  if (payload.action === 'otp_start') {
    await store.start(requestId, phone);
    await createRecoverySms({ env, fetchImpl }).request({ session_event_uid: requestId, sms: phone });
    return neutral;
  }
  const checked = await store.consume(payload.otp_request_id, phone, payload.code);
  if (!checked.verified) return { ok: true, ...checked };
  // An approved code is not an Inputs permission grant. Reuse the existing
  // recognition device binding, with a server-generated token, in this browser.
  const confirmed = await recognition.verificationCandidate(phone);
  if (!confirmed || confirmed.personUid !== candidate.personUid) fail('unauthorized_person', 403);
  const deviceToken = uuid();
  const bound = await recognitionFactory({ env, fetchImpl, principalPersonUid: candidate.personUid, verifiedInputAccess: true }).action({
    action: 'confirm_device', requestId: uuid(), device_token: deviceToken, values: {}
  }, request);
  if (bound.recognized !== true) fail('otp_binding_unconfirmed', 503);
  return { ok: true, verified: true, device_token: deviceToken };
}
