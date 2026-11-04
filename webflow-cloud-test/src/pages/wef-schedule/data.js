import {env} from 'cloudflare:workers';
import {loadSchedule} from '../../lib/wef-schedule/data.mjs';
export const prerender=false;
export async function GET({request}){
 const showId=new URL(request.url).searchParams.get('show');
 const lane=new URL(request.url).searchParams.get('lane')||'full';
 if(!/^rec[A-Za-z0-9]{14}$/.test(showId||'')||!['day0','full'].includes(lane))return Response.json({error:'invalid_scope'},{status:400});
 try{
  const data=await loadSchedule({showId,lane,token:env.AIRTABLE_TOKEN});
  return Response.json(data,{headers:{'Cache-Control':'no-store'}});
 }catch{
  return Response.json({error:'schedule_refresh_failed'},{status:503,headers:{'Cache-Control':'no-store'}});
 }
}
