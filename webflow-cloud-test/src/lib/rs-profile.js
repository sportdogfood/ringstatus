import {remembered} from './rs-recognition-iam.js';
import {createTenantRest,TABLE} from './rs-tenant-barn.js';
import {recognitionConfig} from './rs-recognition-config.js';
import {prepareManagedCredentials} from './rs-recognition-action.js';
import {readTenantSave,runTenantSave,payloadHash} from './rs-tenant-save-operation.js';
import {requireClaimDatabase} from './rs-recognition-claims.js';
import {digest,InputError} from './rs-inputs.js';
import {queueIamRefresh} from './rs-recognition-iam-connections.js';

const fail=(code,status=400)=>{throw new InputError(code,status);};
const active=row=>['active','test'].includes(String(row?.fields.status||'').toLowerCase());
const quote=value=>String(value).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
const tags=value=>String(value||'').split(',').map(x=>x.trim()).filter(Boolean);
async function recognitionRows(env,fetchImpl,{uid,id,fields,allowMissing=false}){
 const config=recognitionConfig(env),url=new URL(`https://api.airtable.com/v0/${config.baseId}/${config.people}`);
 if(!fields){url.searchParams.set('filterByFormula',`{person_uid} = '${quote(uid)}'`);url.searchParams.set('maxRecords','2');}
 const response=await fetchImpl(url,{method:fields?'PATCH':'GET',redirect:'manual',signal:AbortSignal.timeout(15000),headers:{Authorization:`Bearer ${config.token}`,'Content-Type':'application/json'},...(fields?{body:JSON.stringify({records:[{id,fields}]})}:{})});
 if(!response.ok){const error=new InputError(fields?'write_outcome_unknown':'storage_unavailable',503);error.noWrite=fields&&response.status>=400&&response.status<500&&response.status!==408;throw error;}
 const body=await response.json();if(!Array.isArray(body.records))fail('storage_unavailable',503);if(body.records.length!==1){if(allowMissing)return null;fail('recognition_profile_ambiguous',409);}return body.records[0];
}

async function context(request,env,fetchImpl){
 // Recognize first. Private Profile access is a separate decision, not Team membership.
 const identity=await remembered(request,env);if(!identity)fail('authentication_required',401);
 if(identity.identity_only)fail('credential_confirmation_required',401);
 const expectedActor=request.headers.get('X-RS-Actor-Scope');if((request.method==='POST'||expectedActor)&&expectedActor!==identity.person_uid)fail('actor_changed',409);
 const call=createTenantRest(env,fetchImpl),matches=await call(TABLE.people,{formula:`{recognition_person_uid} = '${quote(identity.person_uid)}'`});
 if(matches.length!==1)fail('profile_not_found',404);if(!active(matches[0]))fail('profile_inactive',403);
 const person=matches[0],recognition=await recognitionRows(env,fetchImpl,{uid:identity.person_uid});
 if(!active(recognition))fail('profile_inactive',403);
 const memberships=await call(TABLE.memberships),roles=await call(TABLE.roles);
 return {env,callFetch:fetchImpl,identity,person,recognition,call,memberships,roles};
}

async function stateOf(c){
 const uid=c.identity.person_uid,own=c.person,identity=c.recognition.fields;
 const ownMemberships=c.memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===own.id&&m.fields.Tenant?.length===1);
 const teamConflict=ownMemberships.length>1,teamIds=teamConflict?[]:ownMemberships.filter(m=>m.fields.Status==='Enabled').map(m=>m.fields.Tenant[0]);
 const teams=[];for(const id of teamIds){const row=await c.call(TABLE.barn,{id});if(row.fields.tenant_active===true)teams.push({id:row.id,name:row.fields.tenant_official_name||'',active:true});}
 const ownOwner=ownMemberships.length===1&&c.roles.some(r=>r.fields.rec_name==='owner'&&ownMemberships[0].fields.Roles?.includes(r.id));
 const linked=c.memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Tenant?.length===1&&teams.some(t=>t.id===m.fields.Tenant[0])&&(ownOwner||m.fields.Status==='Enabled'));
 const memberships=linked.map(m=>({id:m.id,teamId:m.fields.Tenant[0],personRecordId:m.fields.Person[0],role:c.roles.some(r=>r.fields.rec_name==='owner'&&m.fields.Roles?.includes(r.id))?'Owner':'Member',active:m.fields.Status==='Enabled'&&teams.some(t=>t.id===m.fields.Tenant[0]&&t.active)}));
 const profiles=[{id:uid,recordId:own.id,first:identity.first_name||'',last:identity.last_name||'',name:own.fields.name||'',displayName:own.fields['display-name']||'',email:own.fields.email||'',phone:identity.primary_phone_e164||own.fields.phone_e164||'',photo:own.fields['profile-img']||'',tags:tags(own.fields.tags),level:'',hasPin:/^\d{4}$/.test(identity.member_pin||''),revision:own.fields.revision||0,created_at:own.createdTime||''}];
 for(const id of new Set(linked.map(m=>m.fields.Person[0]))){if(id===own.id)continue;const row=await c.call(TABLE.people,{id});if(!active(row))continue;const names=row.fields.recognition_person_uid?await recognitionRows(c.env,c.callFetch,{uid:row.fields.recognition_person_uid,allowMissing:true}):null;profiles.push({id:row.fields.recognition_person_uid||row.fields.entity_uid||row.id,recordId:row.id,first:names?.fields.first_name||'',last:names?.fields.last_name||'',name:row.fields.name||'',photo:row.fields['profile-img']||'',tags:tags(row.fields.tags),created_at:row.createdTime||''});}
 for(const m of memberships){const row=await requireClaimDatabase(c.env).prepare('SELECT result_json FROM recognize_claims WHERE claim_key=?').bind('profile-membership-revision:'+m.id).first();m.revision=row?JSON.parse(row.result_json).revision:0;}
 for(const m of memberships)m.userId=profiles.find(p=>p.recordId===m.personRecordId)?.id;
 return {version:1,identity:{currentId:uid},profiles,teams,memberships:memberships.filter(m=>m.userId),invitations:[],hidden:[],locations:[],teamConflict};
}

function profileFields(body,c){
 const f=body.fields;if(!f||typeof f!=='object'||Array.isArray(f))fail('invalid_profile');
 const allowed=['first','last','email','phone','pin','photo','tags'];if(Object.keys(f).some(k=>!allowed.includes(k)))fail('invalid_profile_field');
 const current=c.recognition.fields,first=String(f.first??current.first_name??'').trim(),last=String(f.last??current.last_name??'').trim();
 if(first.length>200||last.length>200)fail('name_required');
 const email=String(f.email??c.person.fields.email??'').trim().toLowerCase();if(email&&!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))fail('invalid_email');
 const photo=String(f.photo??c.person.fields['profile-img']??'').trim();if(photo){let url;try{url=new URL(photo);}catch{}if(!url||url.protocol!=='https:'||url.username||url.password)fail('https_image_url_required');}
 const selected=f.tags??tags(c.person.fields.tags);if(!Array.isArray(selected)||selected.some(x=>!['trainer','groom','rider','vet','farrier','owner','contact','family'].includes(x)))fail('invalid_tags');
 const phone=String(f.phone??current.primary_phone_e164??'').replace(/\D/g,'');if(!/^(1)?\d{10}$/.test(phone))fail('invalid_phone');
 const pin=String(f.pin||'');if(pin&&!/^\d{4}$/.test(pin))fail('invalid_pin');
 return {first,last,email,photo,tags:[...new Set(selected)],phone:'+'+(phone.length===10?'1':'')+phone,pin};
}

export async function handleProfile(request,env,fetchImpl=fetch,executionContext){
 const reply=(body,status=200)=>Response.json(body,{status,headers:{'Cache-Control':'no-store'}});
 try{
  if(!['GET','POST'].includes(request.method))fail('method_not_allowed',405);
  if(request.method==='POST'&&request.headers.get('Origin')!==new URL(request.url).origin)fail('origin_denied',403);
  const c=await context(request,env,fetchImpl);
  if(request.method==='GET')return reply({ok:true,person_uid:c.identity.person_uid,state:await stateOf(c)});
  const body=await request.json();if(!/^[a-f0-9-]{36}$/i.test(body.requestId||''))fail('invalid_request_id');
  if(!['profile','member_status'].includes(body.operation))fail('profile_operation_not_connected',409);
  const inputHash=await payloadHash({actor:c.identity.person_uid,body}),eventId=await digest('profile|'+c.identity.person_uid+'|'+body.requestId),saved=await readTenantSave(env,eventId);
  if(saved.row){const old=JSON.parse(saved.row.result_json||'{}');if(old.actorId!==c.identity.person_uid||old.payloadHash!==inputHash)fail('request_id_reused',409);if(saved.row.state==='complete'){await queueIamRefresh({env,operationId:eventId,executionContext,fetchImpl});return reply({ok:true,state:await stateOf(c)});}}
  let draft,fields,target;
  if(body.operation==='profile'){
   draft=profileFields(body,c);if(!saved.row&&body.expectedRevision!==(c.person.fields.revision||0))fail('record_changed',409);
   fields={name:(draft.first+' '+draft.last).trim()||c.person.fields.name,email:draft.email,phone_e164:draft.phone,'profile-img':draft.photo,tags:draft.tags.join(','),revision:body.expectedRevision+1,request_uid:eventId,request_hash:inputHash};target=c.person;
  }else{
   const own=c.memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===c.person.id&&m.fields.Tenant?.length===1);
   if(own.length!==1||own[0].fields.Status!=='Enabled'||!c.roles.some(r=>r.fields.rec_name==='owner'&&own[0].fields.Roles?.includes(r.id)))fail('team_owner_required',403);
   const team=await c.call(TABLE.barn,{id:own[0].fields.Tenant[0]});if(team.fields.tenant_active!==true)fail('team_inactive',403);
   target=c.memberships.find(m=>m.id===body.membershipId&&m.fields.Tenant?.length===1&&m.fields.Tenant[0]===team.id);
   if(!target||c.roles.some(r=>r.fields.rec_name==='owner'&&target.fields.Roles?.includes(r.id)))fail('member_unavailable',403);
   if(typeof body.active!=='boolean')fail('invalid_membership_status');const version=await requireClaimDatabase(env).prepare('SELECT result_json FROM recognize_claims WHERE claim_key=?').bind('profile-membership-revision:'+target.id).first();const revision=version?JSON.parse(version.result_json).revision:0;if(!saved.row&&body.expectedRevision!==revision)fail('record_changed',409);fields={Status:body.active?'Enabled':'Disabled'};
  }
  const result=await runTenantSave({env,key:saved.key,prior:saved.row,context:{actorId:c.identity.person_uid,payloadHash:inputHash,envelope:{event:{eventId,inputHash}}},work:async op=>{
   await op.reserveRevision(['profile',target.id,body.expectedRevision]);
   if(draft){
    const credentials=await prepareManagedCredentials({env,fetchImpl,personUid:c.identity.person_uid,name:fields.name,email:draft.email,sms:draft.phone,pin:draft.pin,requireInputAccess:false,requireExisting:true});
    await credentials.apply(op);
    await op.step('recognition-names',async()=>{const r=await recognitionRows(env,fetchImpl,{uid:c.identity.person_uid});return (r.fields.first_name||'')===draft.first&&(r.fields.last_name||'')===draft.last?r:null;},()=>recognitionRows(env,fetchImpl,{id:c.recognition.id,fields:{first_name:draft.first,last_name:draft.last}}));
   }
   const table=draft?TABLE.people:TABLE.memberships;
   const record=await op.step('profile-record',async()=>{const r=await c.call(table,{id:target.id,fresh:true});return Object.entries(fields).every(([k,v])=>(r.fields[k]??'')===v)?r:null;},()=>c.call(table,{id:target.id,method:'PATCH',fields}));
   if(!draft)await op.step('membership-revision',async()=>{const r=await requireClaimDatabase(env).prepare('SELECT result_json FROM recognize_claims WHERE claim_key=?').bind('profile-membership-revision:'+target.id).first();return r&&JSON.parse(r.result_json).revision===body.expectedRevision+1?{id:target.id}:null;},async()=>{await requireClaimDatabase(env).prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete',?) ON CONFLICT(claim_key) DO UPDATE SET result_json=excluded.result_json").bind('profile-membership-revision:'+target.id,JSON.stringify({revision:body.expectedRevision+1})).run();return {id:target.id};});
   return {record:{id:record.id}};
  }});
  await queueIamRefresh({env,operationId:eventId,executionContext,fetchImpl});
  const fresh=await context(request,env,fetchImpl);return reply({ok:true,record:result.record,state:await stateOf(fresh)});
 }catch(error){return reply({ok:false,error:error.code||'profile_unavailable'},error.status||503);}
}
