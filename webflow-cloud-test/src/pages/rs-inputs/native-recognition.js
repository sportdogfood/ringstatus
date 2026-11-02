import { env } from 'cloudflare:workers';
import { isIP } from 'node:net';
import { handleNativeRecognition } from '../../lib/rs-recognition-native.js';

// Explicitly authorized recognition-only entry within the existing cookie path.
export const ALL = ({ request, locals }) => {
  // Webflow forwards the visitor address in X-Forwarded-For.
  if (!request.headers.get('CF-Connecting-IP')) {
    const ip = (request.headers.get('X-Forwarded-For') || '').split(',')[0].trim();
    if (isIP(ip)) {
      const headers = new Headers(request.headers);
      headers.set('CF-Connecting-IP', ip);
      request = new Request(request, { headers });
    }
  }
  return handleNativeRecognition(request, env, fetch, locals?.cfContext);
};
