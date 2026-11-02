import { env } from 'cloudflare:workers';
import { isIP } from 'node:net';
import { handleNativeRecognition } from '../../lib/rs-recognition-native.js';

// Explicitly authorized recognition-only entry within the existing cookie path.
export const ALL = ({ request, locals }) => {
  // Temporary transport validation; no IP values or credentials are recorded.
  try {
    const probe = request.headers.get('X-RS-IP-Transport-Probe');
    if (request.method === 'POST' && request.headers.get('Origin') === new URL(request.url).origin && ['normal', 'forged-xff', 'forged-cf', 'duplicate-xff', 'ipv6-xff'].includes(probe)) {
      const forwarded = (request.headers.get('X-Forwarded-For') || '').split(',').map(value => value.trim()).filter(Boolean);
      console.log('[rs-ip-transport-20261102]', JSON.stringify({
        probe,
        forwardedCount: forwarded.length,
        forwardedAllValid: forwarded.length > 0 && forwarded.every(value => isIP(value) !== 0),
        forwardedHasIpv4Canary: forwarded.includes('203.0.113.77'),
        forwardedHasIpv6Canary: forwarded.includes('2001:db8::77'),
        cfPresent: request.headers.has('CF-Connecting-IP'),
        cfIsCanary: request.headers.get('CF-Connecting-IP') === '203.0.113.77'
      }));
    }
  } catch { /* A diagnostic failure must not change recognition. */ }
  return handleNativeRecognition(request, env, fetch, locals?.cfContext);
};
