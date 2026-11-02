import { env } from 'cloudflare:workers';
import { isIP } from 'node:net';
import { handleNativeRecognition } from '../../lib/rs-recognition-native.js';

// Explicitly authorized recognition-only entry within the existing cookie path.
export const ALL = async ({ request, locals }) => {
  // Temporary transport validation; no IP values or credentials are recorded.
  let metadata;
  try {
    const probe = request.headers.get('X-RS-IP-Transport-Probe');
    if (request.method === 'POST' && request.headers.get('Origin') === new URL(request.url).origin && ['normal', 'forged-xff', 'forged-cf', 'duplicate-xff', 'ipv6-xff'].includes(probe)) {
      const forwarded = (request.headers.get('X-Forwarded-For') || '').split(',').map(value => value.trim()).filter(Boolean);
      metadata = {
        probe,
        forwardedCount: forwarded.length,
        forwardedAllValid: forwarded.length > 0 && forwarded.every(value => isIP(value) !== 0),
        forwardedKinds: forwarded.slice(0, 8).map(value => {
          const family = isIP(value), [a, b] = value.split('.').map(Number);
          const privateIp = family === 4 ? a === 10 || a === 127 || a === 0 || a === 172 && b >= 16 && b <= 31 || a === 192 && b === 168 || a === 169 && b === 254 || a === 100 && b >= 64 && b <= 127 : /^(?:f[cd]|fe[89ab]|::1$|::$)/i.test(value);
          return family ? `ipv${family}-${privateIp ? 'private' : 'public'}` : 'invalid';
        }),
        addressHeaderNames: ['cf-connecting-ip', 'cf-connecting-ipv6', 'x-forwarded-for', 'x-real-ip', 'true-client-ip', 'forwarded', 'x-client-ip', 'x-envoy-external-address', 'fastly-client-ip', 'cloudfront-viewer-address', 'x-wf-client-ip', 'x-webflow-client-ip', 'x-original-forwarded-for', 'x-forwarded-client-ip', 'fly-client-ip'].filter(name => request.headers.has(name)),
        forwardedHasIpv4Canary: forwarded.includes('203.0.113.77'),
        forwardedHasIpv6Canary: forwarded.includes('2001:db8::77'),
        cfPresent: request.headers.has('CF-Connecting-IP'),
        cfIsCanary: request.headers.get('CF-Connecting-IP') === '203.0.113.77'
      };
      console.log('[rs-ip-transport-20261102]', JSON.stringify(metadata));
    }
  } catch { /* A diagnostic failure must not change recognition. */ }
  const response = await handleNativeRecognition(request, env, fetch, locals?.cfContext);
  if (metadata) {
    try { response.headers.set('X-RS-IP-Transport', JSON.stringify(metadata)); }
    catch { /* Preserve the original response if diagnostics cannot be attached. */ }
  }
  return response;
};
