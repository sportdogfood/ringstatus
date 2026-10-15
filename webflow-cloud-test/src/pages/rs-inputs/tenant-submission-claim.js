import {env} from 'cloudflare:workers';
import {createTenantSubmissionBridge} from '../../lib/rs-tenant-submissions.js';
import {subscriptionAlertDefinitions} from '../../lib/rs-input-subscription-alerts.js';
export async function handleTenantSubmissionClaim(request,bindings=env,bridge){
 try{
  if(request.method!=='POST')return Response.json({ok:false,error:'method_not_allowed'},{status:405});
  if(!request.headers.get('Content-Type')?.toLowerCase().startsWith('application/json'))return Response.json({ok:false,error:'json_required'},{status:415});
  const reader=request.body?.getReader();if(!reader)throw Error();let size=0;const chunks=[];
  while(true){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>2048){await reader.cancel();return Response.json({ok:false,error:'request_too_large'},{status:413});}chunks.push(value)}
  const bytes=new Uint8Array(size);let offset=0;for(const value of chunks){bytes.set(value,offset);offset+=value.length}const body=JSON.parse(new TextDecoder().decode(bytes));
  if(!body||Object.keys(body).some(k=>!['action','capability','claimToken'].includes(k))||!['claim','complete'].includes(body.action))return Response.json({ok:false,error:'invalid_payload'},{status:400});
  const tenantEnv={...bindings,RS_INPUTS_TENANT_SUBMISSIONS_MODE:bindings.RS_INPUTS_TENANT_SUBMISSIONS_MODE??bindings.RS_INPUTS_WRITE_MODE};
  const service=bridge||createTenantSubmissionBridge({env:tenantEnv,definitions:subscriptionAlertDefinitions});
  const result=await service[body.action]({capability:body.capability,...(body.action==='complete'?{claimToken:body.claimToken}:{})});
  return Response.json(result,{headers:{'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'}});
 }catch(error){return Response.json({ok:false,error:error.code||'invalid_payload'},{status:error.status||400,headers:{'Cache-Control':'no-store'}})}
}
export const ALL=context=>handleTenantSubmissionClaim(context.request);
