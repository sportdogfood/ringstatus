import {env} from 'cloudflare:workers';
import {InputError} from '../../lib/rs-inputs.js';
import {logHorseInput} from '../../lib/wef-schedule/horse-input.mjs';
export const prerender=false;
export async function POST({request}){
 try{return Response.json(await logHorseInput(request,env),{headers:{'Cache-Control':'private, no-store'}});}
 catch(error){return Response.json({ok:false,error:error instanceof InputError?error.message:'change_log_unavailable'},{status:error.status||503,headers:{'Cache-Control':'private, no-store'}});}
}
