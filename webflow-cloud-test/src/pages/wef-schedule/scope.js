import {env} from 'cloudflare:workers';
import {loadScope} from '../../lib/wef-schedule/data.mjs';
export const prerender=false;
export async function GET(){
 try{return Response.json(await loadScope({token:env.AIRTABLE_TOKEN}),{headers:{'Cache-Control':'no-store'}});}
 catch{return Response.json({error:'schedule_scope_unavailable'},{status:503,headers:{'Cache-Control':'no-store'}});}
}
