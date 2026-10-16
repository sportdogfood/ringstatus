import source from '../../assets/rs-recognition/embed.js?raw';

export function GET() {
  return new Response(source, { headers: { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'no-store' } });
}
