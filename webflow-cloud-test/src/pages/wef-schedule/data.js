import {env} from 'cloudflare:workers';
import {sharedBase} from '../../lib/wef-schedule/shared-base.mjs';
import {readScheduleFeed} from '../../lib/wef-schedule/shared-feed.mjs';
export const prerender=false;
export async function GET({request}){
 const showId=new URL(request.url).searchParams.get('show');
 const lane=new URL(request.url).searchParams.get('lane')||'full';
 if(!/^rec[A-Za-z0-9]{14}$/.test(showId||'')||!['day0','full'].includes(lane))return Response.json({error:'invalid_scope'},{status:400});
 try{
  const data=await readScheduleFeed(sharedBase({token:env.AIRTABLE_TOKEN}),showId,lane);
  return Response.json(data,{headers:{'Cache-Control':'no-store'}});
 }catch{
  return Response.json({error:'schedule_refresh_failed'},{status:503,headers:{'Cache-Control':'no-store'}});
 }
}
