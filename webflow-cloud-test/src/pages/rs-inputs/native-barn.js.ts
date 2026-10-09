import nativeClient from '../../assets/rs-barn-onboarding/native-client.js?raw';

export function GET() {
  return new Response(nativeClient, {
    headers: { 'content-type': 'text/javascript; charset=utf-8', 'cache-control': 'no-store' }
  });
}
