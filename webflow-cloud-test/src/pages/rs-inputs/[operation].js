import { env } from 'cloudflare:workers';
import { handleAccessRoute } from '../../lib/rs-inputs-access.js';
import { handleInputRequest } from '../../lib/rs-inputs.js';
import { createAirtableInputStore } from '../../lib/rs-inputs-airtable.js';
import { createInputRecognition } from '../../lib/rs-inputs-recognition.js';

// Internal domain dispatch. The public ALL entry point derives its actor from a verified cookie.
export async function handleInputRoute({ request, locals }, bindings = env, fetchImpl = fetch) {
  let store, recognition;
  try {
    if (locals.rsInputActor) {
      const operation = new URL(request.url).pathname.split('/').filter(Boolean).at(-1);
      if (operation === 'recognition') {
        recognition = createInputRecognition({ env: bindings, fetchImpl, principalPersonUid: locals.rsInputActor.profile?.personUid, verifiedInputAccess: locals.rsInputAccessVerified === true });
      } else if (['state', 'record', 'profile-link'].includes(operation)) {
        store = createAirtableInputStore({ env: bindings, fetchImpl });
      }
    }
  } catch (error) {
    const traceId = crypto.randomUUID();
    const code = error.code || 'storage_unavailable';
    const status = error.status || 503;
    console.info(JSON.stringify({ event: 'rs_input_request', traceId, operation: 'configuration', status, code }));
    return Response.json({ ok: false, error: code }, { status, headers: { 'Cache-Control': 'no-store', 'X-Request-Id': traceId } });
  }
  return handleInputRequest({ request, actor: locals.rsInputActor, store, recognition, log: event => console.info(JSON.stringify(event)) });
}
export const handleAuthenticatedInputRoute = (context, bindings = env, fetchImpl = fetch) =>
  handleAccessRoute(context.request, bindings, fetchImpl, actor =>
    handleInputRoute({ request: context.request, locals: { rsInputActor: actor, rsInputAccessVerified: true } }, bindings, fetchImpl));
export const ALL = context => handleAuthenticatedInputRoute(context);
