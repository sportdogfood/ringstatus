import persistence from '../../assets/rs-inputs/persistence.js?raw';
export function GET() {
 return new Response(persistence, {headers: {'content-type':'text/javascript; charset=utf-8','cache-control':'no-store'}});
}
