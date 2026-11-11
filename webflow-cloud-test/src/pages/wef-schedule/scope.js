import {env} from 'cloudflare:workers';
import {sharedBase} from '../../lib/wef-schedule/shared-base.mjs';
import {scheduleScope} from '../../lib/wef-schedule/shared-feed.mjs';
export const prerender=false;
export async function GET(){
 try{return Response.json(await scheduleScope(sharedBase({token:env.AIRTABLE_TOKEN})),{headers:{'Cache-Control':'no-store'}});}
 catch{return Response.json({error:'schedule_scope_unavailable'},{status:503,headers:{'Cache-Control':'no-store'}});}
}
