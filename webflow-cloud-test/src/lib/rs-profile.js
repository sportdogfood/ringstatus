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
const INVITATIONS='tblOmDd6CDOBkf7Fh';
const photoOf=row=>row.fields.profile_photo?.[0]?.url||row.fields['profile-img']||'';
const ownMemberships=c=>c.memberships.filter(m=>m.fields.Person?.includes(c.person.id));
const equalFields=(actual,expected)=>Object.entries(expected).every(([key,value])=>Array.isArray(value)?JSON.stringify((actual[key]||[]).map(x=>typeof x==='object'?x.id:x))===JSON.stringify(value.map(x=>typeof x==='object'?x.id:x)):(actual[key]??'')===value);
async function ownerTeam(c){
 const own=ownMemberships(c);
 if(own.length!==1||own[0].fields.Person?.length!==1||own[0].fields.Tenant?.length!==1||own[0].fields.Status!=='Enabled'||!c.roles.some(r=>r.fields.rec_name==='owner'&&own[0].fields.Roles?.includes(r.id)))fail('team_owner_required',403);
 const team=await c.call(TABLE.barn,{id:own[0].fields.Tenant[0]});if(team.fields.tenant_active!==true)fail('team_inactive',403);return team;
}
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
 const call=createTenantRest(env,fetchImpl,{profile:true}),matches=await call(TABLE.people,{formula:`{recognition_person_uid} = '${quote(identity.person_uid)}'`});
 if(matches.length!==1)fail('profile_not_found',404);if(!active(matches[0]))fail('profile_inactive',403);
 const person=matches[0];
 const [recognition,memberships,roles]=await Promise.all([recognitionRows(env,fetchImpl,{uid:identity.person_uid}),call(TABLE.memberships),call(TABLE.roles)]);
 if(!active(recognition))fail('profile_inactive',403);
 return {env,callFetch:fetchImpl,identity,person,recognition,call,memberships,roles};
}

async function stateOf(c,view='full'){
 const roster=view==='full'||view==='roster'||view==='team',teamDetails=view==='full'||view==='team';
 const uid=c.identity.person_uid,own=c.person,identity=c.recognition.fields;
 const ownMemberships=c.memberships.filter(m=>m.fields.Person?.includes(own.id));
 const teamConflict=ownMemberships.length>1||ownMemberships.some(m=>m.fields.Person?.length!==1||m.fields.Tenant?.length!==1),teamIds=teamConflict?[]:ownMemberships.filter(m=>m.fields.Status==='Enabled').map(m=>m.fields.Tenant[0]);
 const teams=[];for(const id of teamIds){const row=await c.call(TABLE.barn,{id});if(row.fields.tenant_active===true)teams.push({id:row.id,name:row.fields.tenant_official_name||'',active:true});}
 const ownOwner=!teamConflict&&ownMemberships.length===1&&ownMemberships[0].fields.Status==='Enabled'&&teams.length===1&&c.roles.some(r=>r.fields.rec_name==='owner'&&ownMemberships[0].fields.Roles?.includes(r.id));
 const linked=c.memberships.filter(m=>(roster||m.fields.Person?.includes(own.id))&&m.fields.Person?.length===1&&m.fields.Tenant?.length===1&&teams.some(t=>t.id===m.fields.Tenant[0])&&(ownOwner||m.fields.Status==='Enabled'));
 const memberships=linked.map(m=>({id:m.id,teamId:m.fields.Tenant[0],personRecordId:m.fields.Person[0],role:c.roles.some(r=>r.fields.rec_name==='owner'&&m.fields.Roles?.includes(r.id))?'Owner':'Member',active:m.fields.Status==='Enabled'&&teams.some(t=>t.id===m.fields.Tenant[0]&&t.active)}));
 const profiles=[{id:uid,recordId:own.id,first:identity.first_name||'',last:identity.last_name||'',name:own.fields.name||'',displayName:own.fields['display-name']||'',email:own.fields.email||'',phone:identity.primary_phone_e164||own.fields.phone_e164||'',photo:photoOf(own),tags:tags(own.fields.tags),level:own.fields.user_level||'',hasMembership:ownMemberships.length>0,hasPin:/^\d{4}$/.test(identity.member_pin||''),revision:own.fields.revision||0,created_at:own.createdTime||'',modified_at:own.fields.modified_at||''}];
 const candidates=teamDetails&&ownOwner?(await c.call(TABLE.people)).filter(row=>active(row)&&row.fields.recognition_person_uid&&!c.memberships.some(m=>m.fields.Person?.includes(row.id))):[];
 const peopleIds=new Set([...linked.map(m=>m.fields.Person[0]),...candidates.map(row=>row.id)]);
 for(const id of peopleIds){if(id===own.id)continue;const row=candidates.find(row=>row.id===id)||await c.call(TABLE.people,{id});if(!active(row))continue;const names=row.fields.recognition_person_uid?await recognitionRows(c.env,c.callFetch,{uid:row.fields.recognition_person_uid,allowMissing:true}):null;profiles.push({id:row.fields.recognition_person_uid||row.fields.entity_uid||row.id,recordId:row.id,first:names?.fields.first_name||'',last:names?.fields.last_name||'',name:row.fields.name||'',photo:photoOf(row),tags:tags(row.fields.tags),created_at:row.createdTime||'',modified_at:row.fields.modified_at||''});}
 for(const m of memberships){const row=await requireClaimDatabase(c.env).prepare('SELECT result_json FROM recognize_claims WHERE claim_key=?').bind('profile-membership-revision:'+m.id).first();m.revision=row?JSON.parse(row.result_json).revision:0;}
 for(const m of memberships)m.userId=profiles.find(p=>p.recordId===m.personRecordId)?.id;
 const invitations=[];
 for(const row of teamDetails?await c.call(INVITATIONS):[]){
  const f=row.fields;if(f.Team?.length!==1||f.Recipient?.length!==1||f['Invited by']?.length!==1)continue;
  const recipient=f.Recipient[0]===own.id;if(!recipient&&!(ownOwner&&teams.some(t=>t.id===f.Team[0])))continue;
  const team=await c.call(TABLE.barn,{id:f.Team[0]});if(!team.fields.tenant_active)continue;
  const sender=c.memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===f['Invited by'][0]&&m.fields.Tenant?.length===1&&m.fields.Tenant[0]===team.id&&m.fields.Status==='Enabled');
  if(!sender.some(m=>c.roles.some(r=>r.fields.rec_name==='owner'&&m.fields.Roles?.includes(r.id))))continue;
  invitations.push({id:row.id,teamId:team.id,userId:recipient?uid:profiles.find(p=>p.recordId===f.Recipient[0])?.id,status:String(f.Status||'').toLowerCase(),revision:f.revision||0});
  if(recipient&&!teams.some(t=>t.id===team.id))teams.push({id:team.id,name:team.fields.tenant_official_name||'',active:true});
 }
 return {version:1,identity:{currentId:uid},profiles,teams,memberships:memberships.filter(m=>m.userId),invitations,hidden:[],locations:[],teamConflict,loaded:{profile:true,roster,team:teamDetails}};
}

function profileFields(body,c){
 const f=body.fields;if(!f||typeof f!=='object'||Array.isArray(f))fail('invalid_profile');
 const allowed=['first','last','email','phone','pin','photo','tags'];if(Object.keys(f).some(k=>!allowed.includes(k)))fail('invalid_profile_field');
 const current=c.recognition.fields,first=String(f.first??current.first_name??'').trim(),last=String(f.last??current.last_name??'').trim();
 if(first.length>200||last.length>200)fail('name_required');
 const email=String(f.email??c.person.fields.email??'').trim().toLowerCase();if(email&&!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))fail('invalid_email');
 const photo=String(f.photo??'').trim();
 const photoChanged=Object.hasOwn(f,'photo')&&!(c.person.fields.profile_photo?.length&&photo===photoOf(c.person));let upload=null;
 if(photoChanged&&photo.startsWith('data:')){
  const match=/^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/]+={0,2})$/.exec(photo);if(!match)fail('invalid_photo');
  let bytes;try{bytes=Uint8Array.from(atob(match[2]),c=>c.charCodeAt(0));}catch{fail('invalid_photo');}
  if(!bytes.length||bytes.length>1000000)fail('invalid_photo');
  const signature=match[1]==='image/png'?bytes.slice(0,8).join(',')==='137,80,78,71,13,10,26,10':match[1]==='image/jpeg'?bytes[0]===255&&bytes[1]===216&&bytes[2]===255:String.fromCharCode(...bytes.slice(0,4))==='RIFF'&&String.fromCharCode(...bytes.slice(8,12))==='WEBP';
  if(!signature)fail('invalid_photo');upload={contentType:match[1],file:match[2],extension:match[1].split('/')[1]};
 }else if(photoChanged&&photo){let url;try{url=new URL(photo);}catch{}if(!url||url.protocol!=='https:'||url.username||url.password)fail('https_image_url_required');}
 const selected=f.tags??tags(c.person.fields.tags);if(!Array.isArray(selected)||selected.some(x=>!['trainer','groom','rider','vet','farrier','owner','contact','family'].includes(x)))fail('invalid_tags');
 const phone=String(f.phone??current.primary_phone_e164??'').replace(/\D/g,'');if(!/^(1)?\d{10}$/.test(phone))fail('invalid_phone');
 const pin=String(f.pin||'');if(pin&&!/^\d{4}$/.test(pin))fail('invalid_pin');
 return {first,last,email,photo,photoChanged,upload,tags:[...new Set(selected)],phone:'+'+(phone.length===10?'1':'')+phone,pin};
}

async function saveTeamOperation(c,body,eventId,inputHash,saved){
 const {env}=c;let team,invitation,recipient,role;
 if(body.operation==='team_create'){
  if(c.person.fields.user_level!=='Preferred')fail('preferred_required',403);
  if(ownMemberships(c).length&&!saved.row)fail('team_membership_exists',409);
  if(typeof body.name!=='string'||!body.name.trim()||body.name.trim().length>200)fail('team_name_required');
  role=c.roles.filter(r=>r.fields.rec_name==='owner');
 }else if(body.operation==='invitation_create'){
  team=await ownerTeam(c);
  const matches=(await c.call(TABLE.people)).filter(p=>p.fields.recognition_person_uid===body.userId&&active(p));
  if(matches.length!==1)fail('user_unavailable',404);recipient=matches[0];
  if(c.memberships.some(m=>m.fields.Person?.includes(recipient.id)))fail('team_membership_exists',409);
  const identity=await recognitionRows(env,c.callFetch,{uid:body.userId});if(!active(identity))fail('user_unavailable',404);
 }else{
  if(typeof body.accept!=='boolean'||!/^rec[A-Za-z0-9]+$/.test(body.invitationId||'')||!Number.isSafeInteger(body.expectedRevision)||body.expectedRevision<0)fail('invalid_invitation_response');
  invitation=await c.call(INVITATIONS,{id:body.invitationId});const f=invitation.fields;
  if(f.Recipient?.length!==1||f.Recipient[0]!==c.person.id||f.Team?.length!==1)fail('invitation_unavailable',403);
  if(!saved.row&&(f.Status!=='Pending'||body.expectedRevision!==(f.revision||0)))fail('record_changed',409);
  team=await c.call(TABLE.barn,{id:f.Team[0]});if(team.fields.tenant_active!==true)fail('team_inactive',403);
  const senders=c.memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===f['Invited by']?.[0]&&m.fields.Tenant?.length===1&&m.fields.Tenant[0]===team.id&&m.fields.Status==='Enabled');
  if(!senders.some(m=>c.roles.some(r=>r.fields.rec_name==='owner'&&m.fields.Roles?.includes(r.id))))fail('invitation_unavailable',403);
  if(body.accept&&ownMemberships(c).length&&!saved.row)fail('team_membership_exists',409);
  role=c.roles.filter(r=>r.fields.rec_name==='user');
 }
 if(role&&role.length!==1)fail('role_configuration_required',503);
 return runTenantSave({env,key:saved.key,prior:saved.row,context:{actorId:c.identity.person_uid,payloadHash:inputHash,envelope:{event:{eventId,inputHash}}},work:async op=>{
  if(body.operation==='invitation_create'){
   const previous=(await c.call(INVITATIONS,{fresh:true})).filter(i=>i.fields.Team?.includes(team.id)&&i.fields.Recipient?.includes(recipient.id));
   if(previous.some(i=>i.fields.Status==='Pending'&&i.fields.request_uid!==eventId))fail('invitation_pending',409);
   await op.reserveRevision(['profile-invitation',team.id,recipient.id,previous.filter(i=>i.fields.request_uid!==eventId).length]);
   const fields={'Invitation ID':eventId,Team:[team.id],Recipient:[recipient.id],'Invited by':[c.person.id],Status:'Pending',revision:0,request_uid:eventId,request_hash:inputHash};
   const record=await op.step('invitation',async()=>{const rows=await c.call(INVITATIONS,{fresh:true});return rows.find(r=>equalFields(r.fields,fields))||null;},async()=>(await c.call(INVITATIONS,{method:'POST',fields}))[0]);
   return {record:{id:record.id}};
  }
  await op.reserveRevision(body.operation==='team_create'?['profile-team-create',c.person.id,0]:['profile-invitation-response',invitation.id,body.expectedRevision]);
  if(body.operation==='team_create'||body.accept){
   // The same single slot fences Team creation against acceptance in other tabs.
   const key='profile-team-membership:'+c.person.id;
   await op.step('membership-slot',async()=>{const r=await requireClaimDatabase(env).prepare('SELECT result_json FROM recognize_claims WHERE claim_key=?').bind(key).first();if(r&&JSON.parse(r.result_json).eventId!==eventId)fail('team_membership_exists',409);return r?{id:key}:null;},async()=>{
    await requireClaimDatabase(env).prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete',?) ON CONFLICT(claim_key) DO NOTHING").bind(key,JSON.stringify({eventId})).run();return {id:key};
   });
  }
  if(body.operation==='team_create'){
   const fields={tenant_official_name:body.name.trim(),tenant_active:true,profile_request_uid:eventId};
   team=await op.step('team',async()=>{
    const rows=await c.call(TABLE.barn,{formula:`{profile_request_uid} = '${quote(eventId)}'`,fresh:true});
    if(rows.length>1)fail('ambiguous_team_operation',409);
    if(rows[0]&&!equalFields(rows[0].fields,fields))fail('record_changed',409);
    return rows[0]||null;
   },async()=>(await c.call(TABLE.barn,{method:'POST',fields}))[0]);
  }
  if(body.operation==='team_create'||body.accept){
   const fields={Name:c.person.id+' / '+team.id,Person:[c.person.id],Tenant:[team.id],Roles:[role[0].id],Status:'Enabled'};
   await op.step('membership',async()=>{
    const rows=(await c.call(TABLE.memberships,{fresh:true})).filter(m=>m.fields.Person?.includes(c.person.id));
    if(rows.length>1||rows.length===1&&!equalFields(rows[0].fields,fields))fail('team_membership_exists',409);
    return rows[0]||null;
   },async()=>(await c.call(TABLE.memberships,{method:'POST',fields}))[0]);
  }
  if(invitation){
   const fields={Status:body.accept?'Accepted':'Declined',revision:body.expectedRevision+1,request_uid:eventId,request_hash:inputHash};
   await op.step('invitation-response',async()=>{const r=await c.call(INVITATIONS,{id:invitation.id,fresh:true});return equalFields(r.fields,fields)?r:null;},async()=>(await c.call(INVITATIONS,{id:invitation.id,method:'PATCH',fields}))[0]);
  }
  return {record:{id:invitation?.id||team.id}};
 }});
}

export async function handleProfile(request,env,fetchImpl=fetch,executionContext){
 const reply=(body,status=200)=>Response.json(body,{status,headers:{'Cache-Control':'no-store'}});
 try{
  if(!['GET','POST'].includes(request.method))fail('method_not_allowed',405);
  if(request.method==='POST'&&request.headers.get('Origin')!==new URL(request.url).origin)fail('origin_denied',403);
  const c=await context(request,env,fetchImpl);
  if(request.method==='GET'){const view=new URL(request.url).searchParams.get('view')||'full';if(!['full','profile','roster','team'].includes(view))fail('invalid_profile_view');return reply({ok:true,person_uid:c.identity.person_uid,state:await stateOf(c,view)});}
  const body=await request.json();if(!/^[a-f0-9-]{36}$/i.test(body.requestId||''))fail('invalid_request_id');
  if(!['profile','member_status','team_create','invitation_create','invitation_response'].includes(body.operation))fail('profile_operation_not_connected',409);
  const inputHash=await payloadHash({actor:c.identity.person_uid,body}),eventId=await digest('profile|'+c.identity.person_uid+'|'+body.requestId),saved=await readTenantSave(env,eventId);
  if(saved.row){const old=JSON.parse(saved.row.result_json||'{}');if(old.actorId!==c.identity.person_uid||old.payloadHash!==inputHash)fail('request_id_reused',409);if(saved.row.state==='complete'){await queueIamRefresh({env,operationId:eventId,executionContext,fetchImpl});return reply({ok:true,state:await stateOf(c)});}}
  if(['team_create','invitation_create','invitation_response'].includes(body.operation)){
   const result=await saveTeamOperation(c,body,eventId,inputHash,saved);await queueIamRefresh({env,operationId:eventId,executionContext,fetchImpl});
   return reply({ok:true,record:result.record,state:await stateOf(await context(request,env,fetchImpl))});
  }
  let draft,fields,target;
  if(body.operation==='profile'){
   draft=profileFields(body,c);if(!saved.row&&body.expectedRevision!==(c.person.fields.revision||0))fail('record_changed',409);
   fields={name:(draft.first+' '+draft.last).trim()||c.person.fields.name,email:draft.email,phone_e164:draft.phone,tags:draft.tags.join(','),revision:body.expectedRevision+1,request_uid:eventId,request_hash:inputHash};
   if(draft.photoChanged&&!draft.upload)Object.assign(fields,{'profile-img':draft.photo,profile_photo:[]});target=c.person;
  }else{
   const team=await ownerTeam(c);
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
   if(draft?.upload){
    const filename=eventId+'.'+draft.upload.extension;
    const uploaded=await op.step('profile-photo',async()=>{const r=await c.call(TABLE.people,{id:target.id,fresh:true});return r.fields.profile_photo?.find(a=>a.filename===filename)||null;},async()=>{
     const response=await fetchImpl(`https://content.airtable.com/v0/app7BXcw801n19QKF/${target.id}/fld4TzYY48awDtkF3/uploadAttachment`,{method:'POST',redirect:'manual',signal:AbortSignal.timeout(20000),headers:{Authorization:`Bearer ${env.AIRTABLE_TOKEN}`,'Content-Type':'application/json'},body:JSON.stringify({contentType:draft.upload.contentType,file:draft.upload.file,filename})});
     if(!response.ok){const error=new InputError('write_outcome_unknown',503);error.noWrite=response.status>=400&&response.status<500&&response.status!==408;throw error;}await response.json();return {};
    });
    Object.assign(fields,{'profile-img':'',profile_photo:[{id:uploaded.id}]});
   }
   const table=draft?TABLE.people:TABLE.memberships;
   const record=await op.step('profile-record',async()=>{const r=await c.call(table,{id:target.id,fresh:true});return equalFields(r.fields,fields)?r:null;},async()=>(await c.call(table,{id:target.id,method:'PATCH',fields}))[0]);
   if(!draft)await op.step('membership-revision',async()=>{const r=await requireClaimDatabase(env).prepare('SELECT result_json FROM recognize_claims WHERE claim_key=?').bind('profile-membership-revision:'+target.id).first();return r&&JSON.parse(r.result_json).revision===body.expectedRevision+1?{id:target.id}:null;},async()=>{await requireClaimDatabase(env).prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete',?) ON CONFLICT(claim_key) DO UPDATE SET result_json=excluded.result_json").bind('profile-membership-revision:'+target.id,JSON.stringify({revision:body.expectedRevision+1})).run();return {id:target.id};});
   return {record:{id:record.id}};
  }});
  await queueIamRefresh({env,operationId:eventId,executionContext,fetchImpl});
  const fresh=await context(request,env,fetchImpl);return reply({ok:true,record:result.record,state:await stateOf(fresh)});
 }catch(error){return reply({ok:false,error:error.code||'profile_unavailable'},error.status||503);}
}
