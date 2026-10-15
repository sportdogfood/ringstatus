import { env } from 'cloudflare:workers';
import { handleAccessRoute } from '../../lib/rs-inputs-access.js';
import { handleInputRequest } from '../../lib/rs-inputs.js';
import { createInputStore } from '../../lib/rs-inputs-store.js';
import { createInputRecognition } from '../../lib/rs-inputs-recognition.js';
import { createTenantBarnBridge } from '../../lib/rs-tenant-barn.js';

// Internal domain dispatch. The public ALL entry point derives its actor from a verified cookie.
export async function handleInputRoute({ request, locals }, bindings = env, fetchImpl = fetch) {
  let store, recognition;
  try {
    if (locals.rsInputActor) {
      const operation = new URL(request.url).pathname.split('/').filter(Boolean).at(-1);
      if (operation === 'recognition') {
        recognition = createInputRecognition({ env: bindings, fetchImpl, principalPersonUid: locals.rsInputActor.profile?.personUid, verifiedInputAccess: locals.rsInputAccessVerified === true });
      } else if (['state', 'record', 'profile-link'].includes(operation)) {
        store = createInputStore({ env: bindings, fetchImpl });
        const tenantNative=request.headers.get('X-RS-Tenants')==='barn-rider-v1';
        if(tenantNative&&request.headers.get('X-RS-Actor')!==locals.rsInputActor.id)return Response.json({ok:false,error:'authentication_required'},{status:401,headers:{'Cache-Control':'no-store'}});
        if(tenantNative){
        const bridge=createTenantBarnBridge({env:bindings,legacy:store,actor:locals.rsInputActor,fetchImpl,includeState:request.headers.get('X-RS-Response')!=='record'});
        store=bridge.store;
        if(operation==='record'&&request.method==='POST'){
          if(request.headers.get('Origin')!==new URL(request.url).origin) return Response.json({ok:false,error:'origin_denied'},{status:403});
          const raw=await request.clone().text();
          if(raw.length>16384)return Response.json({ok:false,error:'request_too_large'},{status:413});
          let payload;try{payload=JSON.parse(raw)}catch{return Response.json({ok:false,error:'invalid_json'},{status:400})}
          if(['barn','riders'].includes(payload?.kind))return Response.json(await bridge.intake(payload),{headers:{'Cache-Control':'no-store'}});
        }
        }
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
