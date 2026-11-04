import {env} from 'cloudflare:workers';
import {approximateLocation} from '../../lib/wef-schedule/geo.mjs';
export const prerender=false;
export function GET({request}){
 const headers={'Cache-Control':'private, no-store'};
 if(env.RS_SCHEDULE_READ_ENABLED!=='true')return Response.json({error:'schedule_not_enabled'},{status:503,headers});
 return Response.json(approximateLocation(request),{headers});
}
