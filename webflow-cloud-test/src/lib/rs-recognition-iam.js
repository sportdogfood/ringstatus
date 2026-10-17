import {requireClaimDatabase,claimOnce,completeClaim} from './rs-recognition-claims.js';
import {accessHash} from './rs-inputs-access.js';

const COOKIE='__Host-rs_recognition_device',TTL=31536000;
const respond=(body,status=200)=>Response.json(body,{status,headers:{'Cache-Control':'no-store','X-RS-Recognition-Store':'cloudflare-d1','X-RS-Airtable-Calls':'0'}});
const fail=(code,status=400)=>{const e=new Error(code);e.code=code;e.status=status;throw e;};
const phone=value=>{const d=String(value||'').replace(/\D/g,'');const n=d.length===10?'1'+d:d;return /^1\d{10}$/.test(n)?'+'+n:'';};
const active=value=>['active','test'].includes(String(value||'').toLowerCase());
const tokenFrom=request=>{const values=(request.headers.get('Cookie')||'').split(';').map(s=>s.trim()).filter(s=>s.startsWith(COOKIE+'='));return values.length===1?values[0].slice(COOKIE.length+1):'';};
const cookie=(token,age=TTL)=>`${COOKIE}=${token}; Path=/; Max-Age=${age}; HttpOnly; Secure; SameSite=Lax`;
export async function iamEnabled(env){
 if(env.RS_RECOGNITION_IAM_MODE==='cloudflare')return true;
 if(!env.RS_RECOGNITION_CONTROL_DB)return false;
 return !!await env.RS_RECOGNITION_CONTROL_DB.prepare("SELECT claim_key FROM recognize_claims WHERE claim_key='rs-slim-iam-active-v1' AND state='complete'").first();
}
async function lookupKey(env,kind,value){
 if(!/^[a-f0-9]{64}$/i.test(env.RS_INPUTS_SESSION_SECRET||''))fail('access_secret_missing',503);
 const key=await crypto.subtle.importKey('raw',Uint8Array.from(env.RS_INPUTS_SESSION_SECRET.match(/../g),b=>parseInt(b,16)),{name:'HMAC',hash:'SHA-256'},false,['sign']);
 return [...new Uint8Array(await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(kind+':'+value)))].map(b=>b.toString(16).padStart(2,'0')).join('');
}
// Explicit data import only: no network, no visitor-triggered refresh or fallback.
export async function refreshIam({env,people,memberships,tenants,roles,identities,aliases=[],devices=[],requiredOwnerUid,activeDeviceToken}){
 const db=requireClaimDatabase(env),commands=[db.prepare('DELETE FROM recognize_iam_keys'),db.prepare('DELETE FROM recognize_iam_people')];
 let ownerConfirmed=false;
 const phoneClaims=new Map(),aliasCounts=new Map(),inviteCounts=new Map();
 const claimPhone=(value,id)=>{const n=phone(value);if(!n)return;if(!phoneClaims.has(n))phoneClaims.set(n,new Set());phoneClaims.get(n).add(id);};
 for(const i of identities){claimPhone(i.fields.primary_phone_e164,i.id);const h=i.fields.input_invite_hash;if(h)inviteCounts.set(h,(inviteCounts.get(h)||0)+1);}
 for(const a of aliases){const n=phone(a.fields.alias_phone_e164);aliasCounts.set(n,(aliasCounts.get(n)||0)+1);claimPhone(n,a.fields.person?.length===1?a.fields.person[0]:'invalid-alias');}
 for(const identity of identities){
  const f=identity.fields,uid=f.person_uid,matches=people.filter(p=>p.fields.recognition_person_uid===uid);
  if(!/^[A-Za-z0-9_-]{1,128}$/.test(uid||'')||!active(f.status)||!['invited','approved'].includes(f.input_access)||matches.length!==1||!active(matches[0].fields.status))continue;
  const p=matches[0],all=memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===p.id&&m.fields.Tenant?.length===1);
  const enabled=all.filter(m=>m.fields.Status==='Enabled'&&tenants.some(t=>t.id===m.fields.Tenant[0]&&t.fields.tenant_active===true)&&all.filter(other=>other.fields.Tenant[0]===m.fields.Tenant[0]).length===1);
  const owner=enabled.some(m=>roles.some(r=>r.fields.rec_name==='owner'&&m.fields.Roles?.includes(r.id))),registeredPhone=phone(p.fields.phone_e164);
  if(!enabled.length||(!owner&&!registeredPhone))continue;
  const profile={person_uid:uid,person_name:f.person_name||p.fields.name||'',first_name:f.first_name||'',email:f.email||'',person_record_id:identity.id,barnIds:enabled.map(m=>m.fields.Tenant[0]),owner,roles:[...new Set(enabled.flatMap(m=>m.fields.Roles||[]))]};
  if(uid===requiredOwnerUid&&owner)ownerConfirmed=true;
  commands.push(db.prepare('INSERT INTO recognize_iam_people(person_uid,profile_json) VALUES (?,?)').bind(uid,JSON.stringify(profile)));
  const add=async(kind,value,expires=null)=>commands.push(db.prepare('INSERT OR IGNORE INTO recognize_iam_keys(kind,lookup_key,person_uid,expires_at) VALUES (?,?,?,?)').bind(kind,await lookupKey(env,kind,value),uid,expires));
  const acceptedPhones=new Set([phone(f.primary_phone_e164),...aliases.filter(a=>a.fields.person?.length===1&&a.fields.person[0]===identity.id).map(a=>phone(a.fields.alias_phone_e164))]);
  if(registeredPhone&&acceptedPhones.has(registeredPhone)&&phoneClaims.get(registeredPhone)?.size===1&&(aliasCounts.get(registeredPhone)||0)<2)await add('phone',registeredPhone);
  if(/^\d{4}$/.test(f.member_pin||''))await add('pin',f.member_pin);
  if(owner&&/^[a-f0-9]{32}$/.test(f.input_session_version||'')&&inviteCounts.get(f.input_invite_hash)===1&&/^[a-f0-9]{64}$/.test(f.input_invite_hash||'')&&Date.parse(f.input_invite_expires_at)>Date.now())commands.push(db.prepare('INSERT INTO recognize_iam_keys(kind,lookup_key,person_uid,expires_at) VALUES (?,?,?,?)').bind('invite',f.input_invite_hash,uid,Date.parse(f.input_invite_expires_at)));
 }
 // PIN ambiguity includes inactive/nonmember identities, as in the existing lookup.
 for(const i of identities)if(/^\d{4}$/.test(i.fields.member_pin||''))commands.push(db.prepare('INSERT OR IGNORE INTO recognize_iam_keys(kind,lookup_key,person_uid,expires_at) VALUES (?,?,?,NULL)').bind('pin',await lookupKey(env,'pin',i.fields.member_pin),i.fields.person_uid));
 for(const d of devices){
  const f=d.fields,token=f.device_token;
  if(typeof token!=='string'||!token)continue;
  if(!active(f.status)||f.person?.length!==1||devices.filter(other=>other.fields.device_token===token).length!==1){commands.push(db.prepare('UPDATE recognize_iam_devices SET retired=1 WHERE token_hash=?').bind(await accessHash(token)));continue;}
  const identity=identities.find(i=>i.id===f.person[0]);if(!identity)continue;
  commands.push(db.prepare('INSERT INTO recognize_iam_devices(token_hash,person_uid,expires_at,retired) SELECT ?,person_uid,?,0 FROM recognize_iam_people WHERE person_uid=? ON CONFLICT(token_hash) DO NOTHING').bind(await accessHash(token),Date.now()+TTL*1000,identity.fields.person_uid));
 }
 if(requiredOwnerUid){
  if(!ownerConfirmed||!activeDeviceToken)fail('owner_projection_missing',409);
  commands.push(db.prepare('INSERT INTO recognize_iam_devices(token_hash,person_uid,expires_at,retired) VALUES (?,?,?,0) ON CONFLICT(token_hash) DO NOTHING').bind(await accessHash(activeDeviceToken),requiredOwnerUid,Date.now()+TTL*1000));
  commands.push(db.prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES ('rs-slim-iam-active-v1','complete','{}') ON CONFLICT(claim_key) DO NOTHING"));
 }
 await db.batch(commands);
}
async function remembered(request,env){
 const token=tokenFrom(request);if(!token)return null;
 const row=await requireClaimDatabase(env).prepare('SELECT p.profile_json FROM recognize_iam_devices d JOIN recognize_iam_people p ON p.person_uid=d.person_uid WHERE d.token_hash=? AND d.retired=0 AND d.expires_at>?').bind(await accessHash(token),Date.now()).first();
 return row?JSON.parse(row.profile_json):null;
}
export async function iamActor(request,env){const p=await remembered(request,env);if(!p)fail('authentication_required',401);return {id:p.person_uid,barnIds:p.barnIds,permissions:['inputs:read','inputs:write'],profile:{personUid:p.person_uid,name:p.person_name,email:p.email}};}
function audit(env,ctx,body){
 // Existing durable claim table is the outbox; Airtable export is separate.
 const work=claimOnce(env,'iam-audit',[body.event_uid]).then(c=>c.won?completeClaim(env,c,{...body,exported:false}):c.result).catch(()=>console.error('[rs-recognition] IAM audit unavailable'));
 if(ctx?.waitUntil)ctx.waitUntil(work);
 return body.event_uid;
}
export async function handleIamRecognition(request,env,ctx=null){
 try{
  const url=new URL(request.url),operation=url.searchParams.get('operation'),db=requireClaimDatabase(env);
  if(request.method!=='POST'||request.headers.get('Origin')!==url.origin)fail('origin_denied',403);
  const body=await request.json().catch(()=>null);if(!body||Array.isArray(body))fail('invalid_request');
  if(operation==='recognize'){
   const p=await remembered(request,env);const history_event_uid=audit(env,ctx,{event_uid:crypto.randomUUID(),event_type:'recognition',person_uid:p?.person_uid||'',person_record_id:p?.person_record_id||'',device_token:tokenFrom(request)||body.device_token||'',session_uid:body.session_uid||'',page_path:body.page_path||'',at:new Date().toISOString()});
   return respond({ok:true,history_event_uid,recognized:!!p,...(p?{person_uid:p.person_uid,person_name:p.person_name,first_name:p.first_name}:{})});
  }
  if(operation!=='action')fail('unsupported_operation');
  if(body.action==='retire_device'){
   const token=tokenFrom(request);
   const p=await remembered(request,env);
   if(token)await db.prepare('UPDATE recognize_iam_devices SET retired=1 WHERE token_hash=?').bind(await accessHash(token)).run();
   const history_event_uid=audit(env,ctx,{event_uid:body.session_event_uid||crypto.randomUUID(),event_type:'device_retired',person_uid:p?.person_uid||'',person_record_id:p?.person_record_id||'',device_token:token,session_uid:body.session_uid||'',at:new Date().toISOString()});
   const response=respond({ok:true,history_event_uid,retired:true,recognized:false});response.headers.append('Set-Cookie',cookie('',0));response.headers.append('Set-Cookie','__Secure-rs_input_access=; Path=/test/rs-inputs; Max-Age=0; HttpOnly; Secure; SameSite=Strict');return response;
  }
  if(body.action!=='credential')fail('unsupported_operation');
  const {kind,value,request_id}=body;
  if(!/^[a-f0-9-]{36}$/i.test(request_id||'')||typeof value!=='string'||!['phone','pin','invite'].includes(kind))fail('invalid_credential');
  const normalized=kind==='phone'?phone(value):value;
  if(!normalized||(kind==='pin'&&!/^\d{4}$/.test(value))||(kind==='invite'&&!/^\d{7}$/.test(value)))fail('invalid_credential');
  const key=kind==='invite'?await accessHash(value):await lookupKey(env,kind,normalized);
  const claim=await claimOnce(env,'iam-credential',[request_id,kind,key]);
  if(!claim.won){if(!claim.result)fail('credential_outcome_unknown',409);if(claim.result.verified){const p=await remembered(new Request(url,{headers:{Cookie:COOKIE+'='+claim.result.device_token}}),env);if(!p)return respond({ok:true,verified:false,reason:'not_matched'});}const response=respond(claim.result);if(claim.result.verified)response.headers.set('Set-Cookie',cookie(claim.result.device_token));return response;}
  const bucket=await accessHash('credential-rate:'+String(request.headers.get('cf-connecting-ip')||'unknown')+':'+Math.floor(Date.now()/600000));
  const rate=await db.prepare(`INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete','{"count":1}') ON CONFLICT(claim_key) DO UPDATE SET result_json=json_set(result_json,'$.count',json_extract(result_json,'$.count')+1) WHERE json_extract(result_json,'$.count')<5 RETURNING result_json`).bind(bucket).first();
  if(!rate)return respond(await completeClaim(env,claim,{ok:true,verified:false,reason:'retry_limit'}));
  let rows=(await db.prepare('SELECT p.profile_json,k.expires_at FROM recognize_iam_keys k LEFT JOIN recognize_iam_people p ON p.person_uid=k.person_uid WHERE k.kind=? AND k.lookup_key=?').bind(kind,key).all()).results;
  if(rows.length>1&&kind==='pin'){const p=await remembered(request,env);rows=p?rows.filter(r=>r.profile_json&&JSON.parse(r.profile_json).person_uid===p.person_uid):rows;}
  if(rows.length!==1||!rows[0].profile_json||(kind==='invite'&&rows[0].expires_at<=Date.now()))return respond(await completeClaim(env,claim,{ok:true,verified:false,reason:kind==='pin'&&rows.length>1?'phone_required':'not_matched'}));
  const p=JSON.parse(rows[0].profile_json);
  if(kind==='invite'){const invitation=await claimOnce(env,'iam-invitation',[p.person_uid,key,rows[0].expires_at]);if(!invitation.won)return respond(await completeClaim(env,claim,{ok:true,verified:false,reason:'invalid_invitation'}));}
  const token=crypto.randomUUID();await db.prepare('INSERT INTO recognize_iam_devices(token_hash,person_uid,expires_at,retired) VALUES (?,?,?,0)').bind(await accessHash(token),p.person_uid,Date.now()+TTL*1000).run();
  const result=await completeClaim(env,claim,{ok:true,verified:true,device_token:token,history_event_uid:request_id});
  await db.prepare(`UPDATE recognize_claims SET result_json=json_set(result_json,'$.count',MAX(0,json_extract(result_json,'$.count')-1)) WHERE claim_key=?`).bind(bucket).run();
  audit(env,ctx,{event_uid:request_id,event_type:'login',person_uid:p.person_uid,person_record_id:p.person_record_id,device_token:token,session_uid:body.session_uid||'',matched_by:kind,at:new Date().toISOString()});
  const response=respond(result);response.headers.set('Set-Cookie',cookie(token));return response;
 }catch(error){return respond({ok:false,error:error.code||'recognition_unavailable'},error.status||503);}
}
