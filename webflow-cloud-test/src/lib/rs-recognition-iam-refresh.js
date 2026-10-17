import {recognitionConfig} from './rs-recognition-config.js';
import {createInputRecognition} from './rs-inputs-recognition.js';
import {createTenantRecognitionPolicy} from './rs-tenant-recognition-policy.js';
import {createTenantRest,TABLE} from './rs-tenant-barn.js';
import {refreshIam,iamEnabled,iamActor} from './rs-recognition-iam.js';

// Trusted server-side caller after an authorized Tenants save; no public bypass.
export async function loadIamSnapshot({env,fetchImpl=fetch,requiredOwnerUid,activeDeviceToken}){
 const config=recognitionConfig(env);
 async function read(table){
  const url=new URL(`https://api.airtable.com/v0/${config.baseId}/${table}`),rows=[],seen=new Set();
  do{const response=await fetchImpl(url,{headers:{Authorization:`Bearer ${config.token}`},redirect:'manual',signal:AbortSignal.timeout(15000)});if(!response.ok)throw Error('iam_source_unavailable');const data=await response.json();if(!Array.isArray(data.records))throw Error('iam_source_invalid');rows.push(...data.records);if(!data.offset)return rows;if(seen.has(data.offset))throw Error('iam_source_invalid');seen.add(data.offset);url.searchParams.set('offset',data.offset);}while(true);
 }
 const call=createTenantRest(env,fetchImpl);
 const identities=await read(config.people),aliases=await read(config.aliases),devices=await read(config.devices);
 const people=await call(TABLE.people),memberships=await call(TABLE.memberships),tenants=await call(TABLE.barn),roles=await call(TABLE.roles);
 await refreshIam({env,people,memberships,tenants,roles,identities,aliases,devices,requiredOwnerUid,activeDeviceToken});
 return {people:people.length,memberships:memberships.length,tenants:tenants.length,identities:identities.length};
}

// Explicit owner administration only. Never called by recognition or sign-out.
export async function handleIamRefresh(request,env,fetchImpl=fetch){
 const reply=(body,status=200)=>Response.json(body,{status,headers:{'Cache-Control':'no-store'}});
 try{
  const url=new URL(request.url);
  if(request.method!=='POST'||request.headers.get('Origin')!==url.origin)return reply({ok:false,error:'origin_denied'},403);
  const cookies=(request.headers.get('Cookie')||'').split(';').map(x=>x.trim()).filter(x=>x.startsWith('__Host-rs_recognition_device='));
  if(cookies.length!==1)return reply({ok:false,error:'authentication_required'},401);
  const config=recognitionConfig(env),bindings={...env,RS_INPUTS_BASE_ID:config.baseId,RS_INPUTS_RECOGNITION_BASE_ID:config.baseId};
  const actor=await iamEnabled(env)?await iamActor(request,env):null;
  const found=actor?{recognized:true,profile:{person_uid:actor.id}}:await createInputRecognition({env:bindings,fetchImpl,verifiedInputAccess:true}).recognizeDevice(cookies[0].slice('__Host-rs_recognition_device='.length));
  if(!found.recognized)return reply({ok:false,error:'authentication_required'},401);
  const eligibility=await createTenantRecognitionPolicy({env,fetchImpl})({fields:{person_uid:found.profile.person_uid}});
  if(!eligibility.allowed||!eligibility.owner)return reply({ok:false,error:'owner_required'},403);
  const sourceCounts=await loadIamSnapshot({env,fetchImpl,requiredOwnerUid:found.profile.person_uid,activeDeviceToken:cookies[0].slice('__Host-rs_recognition_device='.length)});
  return reply({ok:true,refreshed:true,sourceCounts});
 }catch(error){return reply({ok:false,error:error.code||'iam_refresh_unavailable'},error.status||503);}
}
