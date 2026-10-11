import {env} from 'cloudflare:workers';
import {createInputStore} from '../../lib/rs-inputs-store.js';
import {createTenantRest,createTenantBarnBridge,TABLE} from '../../lib/rs-tenant-barn.js';
export async function processTenantBarn(request,bindings=env,fetchImpl=fetch){
 try{
  if(request.method!=='POST')return Response.json({ok:false,error:'method_not_allowed'},{status:405});
  const raw=await request.text();if(raw.length>2048)throw Error();const body=JSON.parse(raw);
  if(!['claim','complete'].includes(body.action)||!/^rec[A-Za-z0-9]+$/.test(body.submissionId||'')||!/^[a-f0-9]{64}$/.test(body.signature||''))throw Error();
  const call=createTenantRest(bindings,fetchImpl),row=await call(TABLE.submissions,{id:body.submissionId});
  const data=JSON.parse(row.fields['Submitted changes']);
  // This actor remains untrusted until the bridge verifies the entire signed envelope.
  const bridge=createTenantBarnBridge({env:bindings,actor:data.actor,legacy:createInputStore({env:bindings,fetchImpl}),fetchImpl,call});
  const result=await bridge[body.action](body.submissionId,body.signature);
  return Response.json(result,{headers:{'Cache-Control':'no-store'}});
 }catch(error){return Response.json({ok:false,error:error.code||'invalid_submission',processingClaimed:error.processingClaimed===true},{status:error.status||400,headers:{'Cache-Control':'no-store'}});}
}
export const POST=({request})=>processTenantBarn(request);
