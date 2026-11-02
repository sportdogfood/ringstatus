import { env } from 'cloudflare:workers';
import { handleNativeRecognition } from '../../lib/rs-recognition-native.js';

// Explicitly authorized recognition-only entry within the existing cookie path.
export const ALL = async ({ request, locals }) => {
  // Temporary owner-approved diagnostics; remove after the live header check.
  const url = new URL(request.url);
  if (request.method === 'POST' && request.headers.get('Origin') === url.origin && url.searchParams.get('operation') === 'action') {
    try {
      if ((await request.clone().json()).action === 'credential') {
        const ip = (request.headers.get('CF-Connecting-IP') || '').trim().toLowerCase();
        console.log('[rs-recognition-ip-diagnostic-20261102]', JSON.stringify({
          cfConnectingIpPresent: request.headers.has('CF-Connecting-IP'),
          cfConnectingIpLength: ip.length,
          cfConnectingIpAccepted: !!ip && ip.length <= 45 && /^[0-9a-f:.]+$/.test(ip),
          xForwardedForPresent: request.headers.has('X-Forwarded-For'),
          xRealIpPresent: request.headers.has('X-Real-IP'),
          trueClientIpPresent: request.headers.has('True-Client-IP'),
          forwardedPresent: request.headers.has('Forwarded'),
          cfConnectingIpv6Present: request.headers.has('CF-Connecting-IPv6')
        }));
      }
    } catch { /* Diagnostics must not alter the existing handler outcome. */ }
  }
  return handleNativeRecognition(request, env, fetch, locals?.cfContext);
};
