import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {refreshIam,handleIamRecognition} from '../src/lib/rs-recognition-iam.js';
import {handleProfile} from '../src/lib/rs-profile.js';
import {accessHash} from '../src/lib/rs-inputs-access.js';
const origin='https://ringstatus.com',base='app7BXcw801n19QKF',recognize='app9kOZdIaGyKk5uG';
const ids={person:'recUserOwner00001',other:'recUserOther00001',identity:'recIdentityOwn001',otherIdentity:'recIdentityUsr001',team:'recTeam0000000001',ownMembership:'recMemberOwn00001',membership:'recMemberUsr00001',ownerRole:'recRoleOwner00001',memberRole:'recRoleUser000001'};
const table={people:'tblyTBgrgdhT3Tcg7',team:'tbled1saw01Thffs1',memberships:'tblFf4aWQYA2RRsWo',roles:'tblRqRbjmqmNfJtIP',identities:'tbly1PM5iFYqVzKSm',aliases:'tblgDWKi0Bb6OcoqS',invitations:'tblOmDd6CDOBkf7Fh'};
function database(){const sql=new DatabaseSync(':memory:');for(const file of ['0001_claims.sql','0003_iam.sql'])sql.exec(readFileSync(new URL('../migrations/recognize-control/'+file,import.meta.url),'utf8'));return {prepare(text){let args=[];return {bind(...v){args=v;return this;},async run(){return {meta:{changes:Number(sql.prepare(text).run(...args).changes)}};},async first(){return sql.prepare(text).get(...args)||null;},async all(){return {results:sql.prepare(text).all(...args)};}};},async batch(rows){sql.exec('BEGIN');try{const out=[];for(const r of rows)out.push(await r.run());sql.exec('COMMIT');return out;}catch(e){sql.exec('ROLLBACK');throw e;}},close(){sql.close();}};}
const row=(id,fields)=>({id,createdTime:'2026-01-01T00:00:00.000Z',fields});
async function fixture(withTeam=false){
 const db=database(),env={RS_RECOGNITION_CONTROL_DB:db,RS_INPUTS_SESSION_SECRET:'1'.repeat(64),AIRTABLE_TOKEN:'local-fixture-only',RS_INPUTS_WRITE_MODE:'isolated-trial',AIRTABLE_RS_RECOGNITION_BASE_ID:recognize};
 const records={
  [base+'/'+table.invitations]:[],
  [base+'/tbluecPFIorOytASP']:[],
  [base+'/'+table.people]:[row(ids.person,{entity_uid:'user_owner',recognition_person_uid:'user_owner',name:'First Owner',email:'owner@example.test',phone_e164:'+12025550101',status:'Active',revision:0}),row(ids.other,{entity_uid:'user_other',recognition_person_uid:'user_other',name:'Other User',email:'private@example.test',phone_e164:'+12025550102',status:'Active',revision:0})],
  [base+'/'+table.team]:[row(ids.team,{tenant_active:true,tenant_official_name:'Existing Team',tenant_id:7})],
  [base+'/'+table.memberships]:withTeam?[row(ids.ownMembership,{Person:[ids.person],Tenant:[ids.team],Roles:[ids.ownerRole],Status:'Enabled'}),row(ids.membership,{Person:[ids.other],Tenant:[ids.team],Roles:[ids.memberRole],Status:'Enabled'})]:[],
  [base+'/'+table.roles]:[row(ids.ownerRole,{rec_name:'owner'}),row(ids.memberRole,{rec_name:'user'})],
  [recognize+'/'+table.identities]:[row(ids.identity,{person_uid:'user_owner',first_name:'First',last_name:'Owner',person_name:'First Owner',email:'owner@example.test',primary_phone_e164:'+12025550101',status:'Active',input_access:''}),row(ids.otherIdentity,{person_uid:'user_other',first_name:'Other',last_name:'User',person_name:'Other User',primary_phone_e164:'+12025550102',status:'Active',input_access:''})],
  [recognize+'/'+table.aliases]:[row('recAliasOwn000001',{alias_phone_e164:'+12025550101',person:[ids.identity],status:'Active'}),row('recAliasUsr000001',{alias_phone_e164:'+12025550102',person:[ids.otherIdentity],status:'Active'})]
 };
 await refreshIam({env,people:records[base+'/'+table.people],identities:records[recognize+'/'+table.identities],memberships:records[base+'/'+table.memberships],tenants:records[base+'/'+table.team],roles:records[base+'/'+table.roles]});
 const writes=[],reads=[];let sequence=0;
 const fetchImpl=async(input,options={})=>{const url=new URL(input),[,version,b,t,id]=url.pathname.split('/');if(url.hostname==='content.airtable.com'){const payload=JSON.parse(options.body),person=records[base+'/'+table.people].find(r=>r.id===t);const attachment={id:'attFixture'+(++sequence),filename:payload.filename,url:'https://attachments.example.test/'+payload.filename};person.fields.profile_photo=[...(person.fields.profile_photo||[]),attachment];writes.push({table:'upload',id:person.id,fieldNames:['profile_photo']});return Response.json({id:person.id,fields:{profile_photo:person.fields.profile_photo}});}assert.equal(url.hostname,'api.airtable.com');assert.equal(version,'v0');let rows=records[b+'/'+t];if(!rows)throw Error('Unexpected fixture table '+t);if(options.method==='PATCH'||options.method==='POST'){const payload=JSON.parse(options.body);const result=payload.records.map(update=>{let r=rows.find(r=>r.id===update.id);if(!r){r=row(update.id||'recFixture'+String(++sequence).padStart(7,'0'),{});rows.push(r);}if(update.fields.profile_photo)update.fields.profile_photo=update.fields.profile_photo.map(a=>a.id?(r.fields.profile_photo||[]).find(old=>old.id===a.id)||a:a);Object.assign(r.fields,update.fields);if(t===table.people)r.fields.modified_at=new Date().toISOString();writes.push({table:t,id:r.id,fieldNames:Object.keys(update.fields)});return structuredClone(r);});return Response.json({records:result});}reads.push(t);if(id)return Response.json(structuredClone(rows.find(r=>r.id===id)));const formula=url.searchParams.get('filterByFormula')||'';if(formula.includes('{event_uid}'))rows=rows.filter(r=>formula.includes("'"+r.fields.event_uid+"'"));else if(formula.includes('profile_request_uid'))rows=rows.filter(r=>formula.includes("'"+r.fields.profile_request_uid+"'"));else if(formula.includes('recognition_person_uid'))rows=rows.filter(r=>formula.includes("'"+r.fields.recognition_person_uid+"'"));else if(formula.includes('{person_uid}'))rows=rows.filter(r=>formula.includes("'"+r.fields.person_uid+"'"));else if(formula.includes('primary_phone_e164'))rows=rows.filter(r=>formula.includes(String(r.fields.primary_phone_e164||'').replace(/^\+/,''))||formula.includes(r.fields.primary_phone_e164));else if(formula.includes('alias_phone_e164'))rows=rows.filter(r=>formula.includes(String(r.fields.alias_phone_e164||'').replace(/^\+/,''))||formula.includes(r.fields.alias_phone_e164));return Response.json({records:structuredClone(rows)});};
 const credential=await handleIamRecognition(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({action:'credential',kind:'phone',value:'2025550101',request_id:crypto.randomUUID()})}),env);const login=await credential.json();assert.equal(login.verified,true);
 return {db,env,records,writes,reads,fetchImpl,token:login.device_token,cookie:'__Host-rs_recognition_device='+login.device_token};
}
async function call(f,body,cookie=f.cookie){const request=new Request(origin+'/test/rs-profile/data',{method:body?'POST':'GET',headers:{Cookie:cookie,Origin:origin,'X-RS-Actor-Scope':'user_owner','Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});const response=await handleProfile(request,f.env,f.fetchImpl);return {response,body:await response.json()};}
test('Profile validates Recognize before accessing records; independent User does not require a Team',async()=>{const f=await fixture();try{assert.equal((await call(f,null,'')).response.status,401);assert.equal(f.reads.length,0);const r=await call(f);assert.equal(r.response.status,200,JSON.stringify(r.body));assert.equal(r.body.person_uid,'user_owner');assert.equal(r.body.state.profiles[0].name,'First Owner');assert.equal(r.body.state.teams.length,0);assert.equal(r.body.state.profiles[0].hasPin,false);assert.ok(!JSON.stringify(r.body).includes('member_pin'));assert.ok(!JSON.stringify(r.body).includes('credentials'));}finally{f.db.close();}});
test('real Team links use record IDs; roster omits another User private email and phone',async()=>{const f=await fixture(true);try{const r=await call(f);assert.equal(r.body.state.teams[0].id,ids.team);assert.equal(r.body.state.teams[0].name,'Existing Team');assert.equal(r.body.state.memberships.find(m=>m.userId==='user_owner').role,'Owner');const other=r.body.state.profiles.find(p=>p.id==='user_other');assert.equal(other.name,'Other User');assert.equal(other.first,'Other');assert.equal(other.last,'User');assert.equal(other.email,undefined);assert.equal(other.phone,undefined);}finally{f.db.close();}});
test('own User saves real canonical/recognition fields, retains roles, and same request replays without rewriting',async()=>{const f=await fixture();try{const body={operation:'profile',requestId:crypto.randomUUID(),expectedRevision:0,fields:{first:'First',last:'Owner',email:'changed@example.test',phone:'2025550101',photo:'',pin:'0007',tags:['rider']}};const r=await call(f,body);assert.equal(r.response.status,200,JSON.stringify(r.body));assert.equal(f.records[base+'/'+table.people][0].fields.email,'changed@example.test');assert.equal(f.records[recognize+'/'+table.identities][0].fields.email,'changed@example.test');assert.equal(r.body.state.profiles[0].revision,1);assert.equal(r.body.state.profiles[0].hasPin,true);assert.ok(!JSON.stringify(r.body).includes('0007'));const count=f.writes.length;assert.equal((await call(f,body)).response.status,200);assert.equal(f.writes.length,count);assert.equal((await call(f,{...body,fields:{...body.fields,email:'different@example.test'}})).response.status,409);assert.equal(f.records[base+'/'+table.people][1].fields.email,'private@example.test');}finally{f.db.close();}});
test('GeoIP identity does not authorize private Profile data',async()=>{const f=await fixture();try{await f.db.prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete','{}')").bind('iam-identity-only:'+await accessHash(f.token)).run();const r=await call(f);assert.equal(r.response.status,401);assert.equal(r.body.error,'credential_confirmation_required');assert.equal(f.reads.length,0);}finally{f.db.close();}});
test('owner changes membership status only; revision supports repeated toggles; owner cannot be disabled',async()=>{const f=await fixture(true);try{for(const [expectedRevision,active] of [[0,false],[1,true],[2,false]]){const r=await call(f,{operation:'member_status',membershipId:ids.membership,active,expectedRevision,requestId:crypto.randomUUID()});assert.equal(r.response.status,200,JSON.stringify(r.body));assert.equal(r.body.state.memberships.find(m=>m.id===ids.membership).active,active);}assert.equal((await call(f,{operation:'member_status',membershipId:ids.ownMembership,active:false,expectedRevision:0,requestId:crypto.randomUUID()})).response.status,403);for(const w of f.writes)assert.deepEqual(w.fieldNames,['Status']);}finally{f.db.close();}});
test('account switch rejects the displayed actor before records are read or written',async()=>{const f=await fixture();try{const login=await handleIamRecognition(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({action:'credential',kind:'phone',value:'2025550102',request_id:crypto.randomUUID()})}),f.env);const other=await login.json();const r=await call(f,{operation:'profile',fields:{email:'wrong@example.test'},requestId:crypto.randomUUID(),expectedRevision:0},'__Host-rs_recognition_device='+other.device_token);assert.equal(r.response.status,409);assert.equal(r.body.error,'actor_changed');assert.equal(f.writes.length,0);assert.equal(f.reads.length,0);}finally{f.db.close();}});
test('disabled membership and inactive Team do not expose Team or roster data',async()=>{for(const variant of ['disabled','inactive']){const f=await fixture(true);try{if(variant==='disabled')f.records[base+'/'+table.memberships][0].fields.Status='Disabled';else f.records[base+'/'+table.team][0].fields.tenant_active=false;const r=await call(f);assert.equal(r.response.status,200);assert.equal(r.body.state.teams.length,0);assert.equal(r.body.state.profiles.length,1);assert.equal(r.body.state.profiles[0].id,'user_owner');}finally{f.db.close();}}});
test('omitted name fields read back as empty rather than causing a permanent save failure',async()=>{const f=await fixture();try{delete f.records[recognize+'/'+table.identities][0].fields.last_name;const r=await call(f,{operation:'profile',requestId:crypto.randomUUID(),expectedRevision:0,fields:{email:'missing-name@example.test'}});assert.equal(r.response.status,200,JSON.stringify(r.body));assert.equal(r.body.state.profiles[0].last,'');}finally{f.db.close();}});
test('uncertain canonical write is read back on same-request retry without a duplicate write',async()=>{const f=await fixture();try{const actual=f.fetchImpl;let lost=true;f.fetchImpl=async(url,options)=>{const response=await actual(url,options);if(lost&&options?.method==='PATCH'&&String(url).includes(table.people)){lost=false;throw Error('fixture lost acknowledgement');}return response;};const body={operation:'profile',requestId:crypto.randomUUID(),expectedRevision:0,fields:{email:'ack@example.test'}};assert.equal((await call(f,body)).response.status,503);const writes=f.writes.length;const retry=await call(f,body);assert.equal(retry.response.status,200,JSON.stringify(retry.body));assert.equal(f.writes.length,writes);}finally{f.db.close();}});

async function otherCall(f,body){
 const response=await handleIamRecognition(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({action:'credential',kind:'phone',value:'2025550102',request_id:crypto.randomUUID()})}),f.env);
 const login=await response.json();assert.equal(login.verified,true);
 const result=await handleProfile(new Request(origin+'/test/rs-profile/data',{method:body?'POST':'GET',headers:{Cookie:'__Host-rs_recognition_device='+login.device_token,Origin:origin,'X-RS-Actor-Scope':'user_other','Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})}),f.env,f.fetchImpl);
 return {response:result,body:await result.json()};
}
test('level is explicit/read-only; Preferred creation persists one Owner membership and replays once',async()=>{
 const f=await fixture();try{
  assert.equal((await call(f)).body.state.profiles[0].level,'');
  const body={operation:'team_create',name:'My Team',requestId:crypto.randomUUID()};
  assert.equal((await call(f,body)).response.status,403);
  f.records[base+'/'+table.people][0].fields.user_level='Preferred';
  const made=await call(f,body);assert.equal(made.response.status,200,JSON.stringify(made.body));
  assert.equal(made.body.state.profiles[0].level,'Preferred');assert.equal(made.body.state.memberships[0].role,'Owner');assert.equal(made.body.state.teams[0].name,'My Team');
  const count=f.writes.length;assert.equal((await call(f,body)).response.status,200);assert.equal(f.writes.length,count);
  assert.equal((await call(f,{...body,requestId:crypto.randomUUID()})).response.status,409);
  f.records[base+'/'+table.memberships][0].fields.Status='Disabled';
  assert.equal((await call(f,{...body,requestId:crypto.randomUUID()})).response.status,409);
  assert.equal((await call(f,{operation:'profile',expectedRevision:0,requestId:crypto.randomUUID(),fields:{level:'Preferred'}})).response.status,400);
 }finally{f.db.close();}
});
test('Owner invites existing User; recipient accepts explicitly; repeated response creates no duplicate',async()=>{
 const f=await fixture(true);try{
  f.records[base+'/'+table.memberships].pop();
  const state=(await call(f)).body.state;assert.ok(state.profiles.some(p=>p.id==='user_other'));assert.equal(state.profiles.find(p=>p.id==='user_other').phone,undefined);
  const sent=await call(f,{operation:'invitation_create',userId:'user_other',requestId:crypto.randomUUID()});assert.equal(sent.response.status,200,JSON.stringify(sent.body));
  const invitation=f.records[base+'/'+table.invitations][0];assert.equal(invitation.fields.Status,'Pending');
  const recipient=await otherCall(f);assert.equal(recipient.body.state.memberships.length,0);assert.equal(recipient.body.state.invitations[0].teamId,ids.team);assert.equal(recipient.body.state.profiles.length,1);
  const body={operation:'invitation_response',invitationId:invitation.id,accept:true,expectedRevision:0,requestId:crypto.randomUUID()};
  assert.equal((await call(f,body)).response.status,403);
  const accepted=await otherCall(f,body);assert.equal(accepted.response.status,200,JSON.stringify(accepted.body));assert.equal(invitation.fields.Status,'Accepted');assert.equal(accepted.body.state.memberships.find(m=>m.userId==='user_other').role,'Member');
  const count=f.writes.length;assert.equal((await otherCall(f,body)).response.status,200);assert.equal(f.writes.length,count);assert.equal(f.records[base+'/'+table.memberships].filter(m=>m.fields.Person.includes(ids.other)).length,1);
 }finally{f.db.close();}
});
test('decline preserves no membership and permits a new invitation; inactive Team cannot accept',async()=>{
 const f=await fixture(true);try{
  f.records[base+'/'+table.memberships].pop();
  const invite=()=>call(f,{operation:'invitation_create',userId:'user_other',requestId:crypto.randomUUID()});
  assert.equal((await invite()).response.status,200);assert.equal((await invite()).response.status,409);
  const invitation=f.records[base+'/'+table.invitations][0];
  const declined=await otherCall(f,{operation:'invitation_response',invitationId:invitation.id,accept:false,expectedRevision:0,requestId:crypto.randomUUID()});assert.equal(declined.response.status,200,JSON.stringify(declined.body));assert.equal(invitation.fields.Status,'Declined');assert.equal(f.records[base+'/'+table.memberships].length,1);
  assert.equal((await invite()).response.status,200);
  f.records[base+'/'+table.team][0].fields.tenant_active=false;
  assert.equal((await otherCall(f,{operation:'invitation_response',invitationId:f.records[base+'/'+table.invitations][1].id,accept:true,expectedRevision:0,requestId:crypto.randomUUID()})).response.status,403);
 }finally{f.db.close();}
});
test('concurrent creation and invitation acceptance cannot create two memberships',async()=>{
 const f=await fixture(true);try{
  f.records[base+'/'+table.people][0].fields.user_level='Preferred';
  f.records[base+'/'+table.memberships].shift();f.records[base+'/'+table.memberships][0].fields.Roles=[ids.ownerRole];
  const invitation=row('recInvitation001',{Team:[ids.team],Recipient:[ids.person],'Invited by':[ids.other],Status:'Pending',revision:0});f.records[base+'/'+table.invitations].push(invitation);
  const results=await Promise.all([call(f,{operation:'team_create',name:'Concurrent Team',requestId:crypto.randomUUID()}),call(f,{operation:'invitation_response',invitationId:invitation.id,accept:true,expectedRevision:0,requestId:crypto.randomUUID()})]);
  assert.equal(results.filter(r=>r.response.status===200).length,1,JSON.stringify(results.map(r=>r.body)));assert.equal(f.records[base+'/'+table.memberships].filter(m=>m.fields.Person.includes(ids.person)).length,1);
 }finally{f.db.close();}
});
test('uploaded photo is validated, read back, replayed once, retained on unrelated edits and removed',async()=>{
 const f=await fixture();try{
  const photo='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a9X8AAAAASUVORK5CYII=';
  const body={operation:'profile',expectedRevision:0,requestId:crypto.randomUUID(),fields:{photo}};
  const uploaded=await call(f,body);assert.equal(uploaded.response.status,200,JSON.stringify(uploaded.body));assert.match(uploaded.body.state.profiles[0].photo,/^https:\/\/attachments/);assert.equal(f.writes.filter(w=>w.table==='upload').length,1);
  assert.equal((await call(f,body)).response.status,200);assert.equal(f.writes.filter(w=>w.table==='upload').length,1);
  assert.equal((await call(f,{operation:'profile',expectedRevision:1,requestId:crypto.randomUUID(),fields:{tags:['rider']}})).response.status,200);assert.equal(f.records[base+'/'+table.people][0].fields.profile_photo.length,1);
  const cleared=await call(f,{operation:'profile',expectedRevision:2,requestId:crypto.randomUUID(),fields:{photo:''}});assert.equal(cleared.response.status,200);assert.equal(cleared.body.state.profiles[0].photo,'');assert.ok(cleared.body.state.profiles[0].modified_at);
  assert.equal((await call(f,{operation:'profile',expectedRevision:3,requestId:crypto.randomUUID(),fields:{photo:'data:image/png;base64,YmFk'}})).response.status,400);
 }finally{f.db.close();}
});

test('saving the displayed uploaded-photo URL preserves durable attachment backing',async()=>{
 const f=await fixture();try{
  const photo='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a9X8AAAAASUVORK5CYII=';
  const uploaded=await call(f,{operation:'profile',expectedRevision:0,requestId:crypto.randomUUID(),fields:{photo}});
  assert.equal(uploaded.response.status,200,JSON.stringify(uploaded.body));
  const original=structuredClone(f.records[base+'/'+table.people][0].fields.profile_photo);
  const saved=await call(f,{operation:'profile',expectedRevision:1,requestId:crypto.randomUUID(),fields:{photo:uploaded.body.state.profiles[0].photo}});
  assert.equal(saved.response.status,200,JSON.stringify(saved.body));
  assert.deepEqual(f.records[base+'/'+table.people][0].fields.profile_photo,original);
  assert.equal(f.records[base+'/'+table.people][0].fields['profile-img'],'');
  assert.match(readFileSync(new URL('../src/assets/rs-profile/client.js',import.meta.url),'utf8'),/if\(key==='photo'&&fields\.photo===p\(\)\.photo\)delete fields\.photo/);
 }finally{f.db.close();}
});

test('lost Team-create reply recovers the same Team by operation ID and finishes one Owner membership',async()=>{
 const f=await fixture();try{
  f.records[base+'/'+table.people][0].fields.user_level='Preferred';
  const actual=f.fetchImpl;let lost=true;
  f.fetchImpl=async(url,options)=>{const response=await actual(url,options);if(lost&&options?.method==='POST'&&new URL(url).pathname.endsWith('/'+table.team)){lost=false;throw Error('lost Team acknowledgement');}return response;};
  const body={operation:'team_create',name:'Recovered Team',requestId:crypto.randomUUID()};
  assert.equal((await call(f,body)).response.status,503);
  assert.equal(f.records[base+'/'+table.team].length,2);assert.equal(f.records[base+'/'+table.memberships].length,0);
  const recovered=await call(f,body);assert.equal(recovered.response.status,200,JSON.stringify(recovered.body));
  assert.equal(f.records[base+'/'+table.team].length,2);assert.equal(f.records[base+'/'+table.memberships].length,1);assert.equal(recovered.body.state.memberships[0].role,'Owner');
  const count=f.writes.length;assert.equal((await call(f,body)).response.status,200);assert.equal(f.writes.length,count);
 }finally{f.db.close();}
});

async function scopedCall(f,view){const response=await handleProfile(new Request(origin+'/test/rs-profile/data?view='+view,{headers:{Cookie:f.cookie,'X-RS-Actor-Scope':'user_owner'}}),f.env,f.fetchImpl);return {response,body:await response.json()};}
test('conditional refresh sends only changed packages and still checks identity',async()=>{const f=await fixture(true);try{const initial=await scopedCall(f,'team'),etag=initial.response.headers.get('etag');assert.ok(etag);const request=(cookie=f.cookie)=>new Request(origin+'/test/rs-profile/data?view=team',{headers:{Cookie:cookie,'X-RS-Actor-Scope':'user_owner','If-None-Match':etag}});const unchanged=await handleProfile(request(),f.env,f.fetchImpl);assert.equal(unchanged.status,304);assert.equal(await unchanged.text(),'');assert.notEqual((await handleProfile(request(''),f.env,f.fetchImpl)).status,304);const save=await call(f,{operation:'profile',requestId:crypto.randomUUID(),expectedRevision:0,fields:{email:'changed@example.test'}});assert.equal(save.response.status,200);const changed=await handleProfile(request(),f.env,f.fetchImpl);assert.equal(changed.status,200);assert.notEqual(changed.headers.get('etag'),etag);}finally{f.db.close();}});
test('initial Profile scope omits roster enrichment, candidates and invitations',async()=>{const f=await fixture(true);try{f.reads.length=0;const r=await scopedCall(f,'profile');assert.equal(r.response.status,200);assert.deepEqual(r.body.state.loaded,{profile:true,roster:false,team:false});assert.deepEqual(r.body.state.profiles.map(p=>p.id),['user_owner']);assert.equal(r.body.state.memberships.length,1);assert.equal(r.body.state.memberships[0].role,'Owner');assert.equal(f.reads.filter(t=>t===table.identities).length,1);assert.equal(f.reads.filter(t=>t===table.people).length,1);assert.ok(!f.reads.includes(table.invitations));assert.equal(f.writes.length,0);}finally{f.db.close();}});
test('roster and Team scopes defer only their corresponding existing records',async()=>{const f=await fixture(true);try{f.reads.length=0;const roster=await scopedCall(f,'roster');assert.equal(roster.response.status,200);assert.equal(roster.body.state.loaded.roster,true);assert.equal(roster.body.state.loaded.team,false);assert.ok(roster.body.state.profiles.some(p=>p.id==='user_other'));assert.ok(!f.reads.includes(table.invitations));const team=await scopedCall(f,'team');assert.equal(team.body.state.loaded.team,true);assert.ok(f.reads.includes(table.invitations));assert.equal(f.writes.length,0);}finally{f.db.close();}});


test('horse package uses authenticated Team trainer scope and rejects unrelated trainer tokens',async()=>{
 const f=await fixture(true);try{
 f.records[base+'/'+table.team][0].fields.tenant_id=39138;
 f.records[base+'/tblROAbIzrkNMRGVd']=[row('recHorseAllowed01',{'horse-id':141765,'horse-name':'RED C','w-trainers':'39138'}),row('recHorseShared001',{'horse-id':2,'horse-name':'Shared','w-trainers':'222, 39138'}),row('recHorseDenied01',{'horse-id':3,'horse-name':'Private','w-trainers':'139138'})];
 const request=(cookie=f.cookie)=>new Request(origin+'/test/rs-profile/data?view=horses&trainer=139138',{headers:{Cookie:cookie,'X-RS-Actor-Scope':'user_owner'}});
 let r=await handleProfile(request(''),f.env,f.fetchImpl);assert.equal(r.status,401);assert.equal(f.reads.length,0);
 r=await handleProfile(request(),f.env,f.fetchImpl);assert.equal(r.status,200);const data=await r.json();assert.deepEqual(data.state.horses.map(h=>h.name),['RED C','Shared']);assert.equal(data.state.horseTeamId,ids.team);assert.equal(data.state.loaded.horses,true);assert.equal(f.writes.length,0);
 const conditional=request();conditional.headers.set('If-None-Match',r.headers.get('ETag'));assert.equal((await handleProfile(conditional,f.env,f.fetchImpl)).status,304);
 f.records[base+'/'+table.memberships][0].fields.Status='Disabled';const before=f.reads.filter(t=>t==='tblROAbIzrkNMRGVd').length;r=await handleProfile(request(),f.env,f.fetchImpl);assert.deepEqual((await r.json()).state.horses,[]);assert.equal(f.reads.filter(t=>t==='tblROAbIzrkNMRGVd').length,before);
 }finally{f.db.close();}
});


test('last location requires recognized actor and only reads the Airtable log',async()=>{
 const f=await fixture();try{
  let lookups=0;const fetchGeo=async()=>{lookups++;return Response.json({latitude:'26.6',longitude:'-80.2'});};
  const request=(cookie,actor='user_owner')=>new Request(origin+'/test/rs-profile/data?view=geoip',{headers:{Cookie:cookie,'X-RS-Actor-Scope':actor,'X-Forwarded-For':'203.0.113.1'}});
  assert.equal((await handleProfile(request(''),f.env,fetchGeo)).status,401);assert.equal(lookups,0);
  assert.equal((await handleProfile(request(f.cookie,'another'),f.env,fetchGeo)).status,409);assert.equal(lookups,0);
  const response=await handleProfile(request(f.cookie),f.env,f.fetchImpl),value=await response.json();assert.equal(response.status,200);assert.equal(value.person_uid,'user_owner');assert.equal(value.geoip,null);assert.equal(lookups,0);assert.deepEqual(f.reads,['tbluecPFIorOytASP']);assert.equal(f.writes.length,0);assert.equal(response.headers.get('cache-control'),'no-store');
 }finally{f.db.close();}
});


test('location POST saves only the authenticated Profile coordinates; GET stays read-only',async()=>{
 const f=await fixture();try{
  const request=(position,headers={})=>new Request(origin+'/test/rs-profile/data?view=geoip',{method:'POST',headers:{Cookie:f.cookie,Origin:origin,'X-RS-Actor-Scope':'user_owner','Content-Type':'application/json',...headers},body:JSON.stringify({requestId:crypto.randomUUID(),...position})});
  const denied=await handleProfile(request({latitude:10,longitude:20},{'X-RS-Actor-Scope':'user_other'}),f.env,f.fetchImpl);assert.equal(denied.status,409);assert.equal(f.writes.length,0);
  const gpsBody={requestId:crypto.randomUUID(),latitude:10,longitude:20};const response=await handleProfile(request(gpsBody),f.env,f.fetchImpl);const body=await response.json();assert.equal(response.status,200,JSON.stringify(body));assert.equal(body.saved,true);
  const events=f.records[base+'/tbluecPFIorOytASP'];assert.equal(events.length,1);const beforeReplay=f.writes.length;assert.equal((await handleProfile(request(gpsBody),f.env,f.fetchImpl)).status,200);assert.equal(f.writes.length,beforeReplay);assert.equal(events.length,1);assert.equal(JSON.parse(events[0].fields.result_json).source,'gps');const person=f.records[base+'/'+table.people][0];assert.equal(person.fields.location_latitude,10);assert.equal(person.fields.location_longitude,20);assert.equal(person.fields.location_source,'gps');assert.ok(person.fields.location_observed_at);assert.equal(person.fields.revision,0);assert.equal(f.records[base+'/'+table.people][1].fields.location_source,undefined);
  const fallback=request({});Object.defineProperty(fallback,'cf',{value:{latitude:30,longitude:40}});assert.equal((await handleProfile(fallback,f.env,f.fetchImpl)).status,200);assert.equal(person.fields.location_source,'cloudflare');assert.equal(events.length,2);
  const unavailable=await handleProfile(request({}),f.env,f.fetchImpl);assert.equal(unavailable.status,200);assert.equal(events.length,3);assert.equal(events[2].fields.outcome,'location_unavailable');assert.equal((await unavailable.json()).geoip.source,'cloudflare');
  const replay=await handleProfile(request(gpsBody),f.env,f.fetchImpl);assert.equal((await replay.json()).geoip.source,'cloudflare');assert.equal(events.length,3);
  const count=f.writes.length;const read=new Request(origin+'/test/rs-profile/data?view=geoip',{headers:{Cookie:f.cookie,'X-RS-Actor-Scope':'user_owner'}});Object.defineProperty(read,'cf',{value:{latitude:1,longitude:2}});await handleProfile(read,f.env,f.fetchImpl);assert.equal(f.writes.length,count);
 }finally{f.db.close();}
});


test('lost location acknowledgement replays the same log without another event',async()=>{
 const f=await fixture();try{
  const body={requestId:crypto.randomUUID(),latitude:10,longitude:20};
  const req=()=>new Request(origin+'/test/rs-profile/data?view=geoip',{method:'POST',headers:{Cookie:f.cookie,Origin:origin,'X-RS-Actor-Scope':'user_owner','Content-Type':'application/json'},body:JSON.stringify(body)});
  const actual=f.fetchImpl;let lose=true;
  const unreliable=async(url,options)=>{const result=await actual(url,options);if(lose&&options?.method==='POST'&&String(url).includes('tbluecPFIorOytASP')){lose=false;throw Error('fixture lost acknowledgement');}return result;};
  assert.equal((await handleProfile(req(),f.env,unreliable)).status,503);
  assert.equal(f.records[base+'/tbluecPFIorOytASP'].length,1);
  assert.equal((await handleProfile(req(),f.env,actual)).status,200);
  assert.equal(f.records[base+'/tbluecPFIorOytASP'].length,1);
 }finally{f.db.close();}
});
