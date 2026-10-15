// Airtable automation: normal record-created runner, never a manual data repair.
// Reuses the existing signed admission -> one-winner claim -> readback pattern.
const T={sub:'tblldMTDEC697hPwE',barn:'tbled1saw01Thffs1',riders:'tbllUlTlcEMgZiW3h',sources:'tblRswgKjA9dt3PED',events:'tbluecPFIorOytASP',people:'tblyTBgrgdhT3Tcg7',memberships:'tblFf4aWQYA2RRsWo',roster:'tblwcczj8lFhVcbyt'};
const endpoint='https://ringstatus.com/test/rs-inputs/tenant-barn-process';
const source='rs-inputs-native';
const rolesTable='tblRqRbjmqmNfJtIP';
const cell=(r,k)=>r.getCellValue(k),choice=v=>v?.name||v||'';
const fail=code=>{throw Error(code)};
async function rows(table,fields){return (await base.getTable(table).selectRecordsAsync({fields})).records;}
function unique(records,predicate){const found=records.filter(predicate);if(found.length>1)fail('ambiguous_tenant_mapping');return found[0]||null;}
async function mapping(kind,id){return unique(await rows(T.sources,['mapping_uid','external_id','source_scope']),r=>cell(r,'mapping_uid')===`${source}|${kind}|${id}`);}
async function saveMap(kind,record,target){const prior=await mapping(kind,record.id);if(prior){if(cell(prior,'external_id')!==target)fail('mapping_changed');return;}
 await base.getTable(T.sources).createRecordAsync({mapping_uid:`${source}|${kind}|${record.id}`,entity_type:{name:kind==='barn'?'Barn':'Rider'},entity_uid:record.id,source_system:source,source_entity_type:kind,source_scope:record.barnId,external_id:target,record_mode:{name:'Test'}});
}
async function membership(actor,tenantId,allowCreate){
 const owner=unique(await rows(rolesTable,['rec_name']),r=>cell(r,'rec_name')==='owner');
 if(!owner)fail('tenant_owner_role_required');
 const ownerRole=owner.id;
 let person=unique(await rows(T.people,['recognition_person_uid','entity_uid']),r=>cell(r,'recognition_person_uid')===actor.id);
 if(!person&&allowCreate){const id=await base.getTable(T.people).createRecordAsync({entity_uid:`recognize_${actor.id}`,recognition_person_uid:actor.id,name:actor.profile.name,email:actor.profile.email||'',status:{name:'Active'},record_mode:{name:'Test'}});person=await base.getTable(T.people).selectRecordAsync(id);}
 if(!person)fail('tenant_membership_required');
 const memberships=await rows(T.memberships,['Person','Tenant','Status','Roles']);
 const found=unique(memberships,r=>cell(r,'Person')?.some(x=>x.id===person.id)&&cell(r,'Tenant')?.some(x=>x.id===tenantId));
 if(found){if(choice(cell(found,'Status'))!=='Enabled'||!cell(found,'Roles')?.some(x=>x.id===ownerRole))fail('tenant_membership_required');return found.id;}
 if(!allowCreate)fail('tenant_membership_required');
 return base.getTable(T.memberships).createRecordAsync({Name:`${actor.profile.name} · owner`,Person:[{id:person.id}],Tenant:[{id:tenantId}],Roles:[{id:ownerRole}],Status:{name:'Enabled'}});
}
async function run(){
 const {recordId}=input.config(),sub=base.getTable(T.sub),row=await sub.selectRecordAsync(recordId);
 if(!row)return;let raw;try{raw=JSON.parse(cell(row,'Submitted changes'));}catch{return;}
 if(raw.kind!=='barn-rider')return;
 let won=false;
 const send=async action=>{const res=await fetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,submissionId:recordId,signature:raw.signature})});const value=await res.json();if(action==='claim'&&value.processingClaimed===true)won=true;if(!res.ok||value.ok!==true)fail(value.error||'claim_failed');return value;};
 try{
  const claimed=await send('claim');if(claimed.state==='Applied')return;won=true;
  const e=claimed.envelope,r=e.record,kind=e.entityKind;
  if(!['barn','riders'].includes(kind)||r.ownerUid!==e.actor.id)fail('invalid_scope');
  const map=await mapping(kind,r.id);if((map?cell(map,'external_id'):'')!==e.targetId)fail('mapping_changed');
  let tenantId=kind==='barn'?e.targetId:cell(await mapping('barn',r.barnId)||{getCellValue:()=>null},'external_id');
  if(kind==='riders'&&!tenantId)fail('save_barn_first');
  if(tenantId)await membership(e.actor,tenantId,false);
  const target=base.getTable(T[kind]),nameField=kind==='barn'?'tenant_official_name':'name';
  let targetId=e.targetId;
  if(targetId){const current=await target.selectRecordAsync(targetId);if(!current||cell(current,nameField)!==e.previous?.name)fail('record_changed');}
  else{
   // Existing imported/shared records require an explicit source mapping. Never
   // create a second row just because an imported row lacks our application ID.
   const matching=(await rows(T[kind],[nameField])).filter(x=>String(cell(x,nameField)||'').trim()&&[r.name,e.previous?.name].filter(Boolean).some(name=>String(cell(x,nameField)||'').trim().toLowerCase()===name.trim().toLowerCase()));
   if(matching.length)fail('existing_record_mapping_required');
  }
  let roster=null;
  if(kind==='riders'&&targetId){roster=unique(await rows(T.roster,['Tenant','Rider','Horse','Status']),x=>cell(x,'Tenant')?.some(v=>v.id===tenantId)&&cell(x,'Rider')?.some(v=>v.id===targetId));if(!roster||choice(cell(roster,'Status'))!=='Enabled'||cell(roster,'Horse')?.length)fail('roster_not_enabled');}
  const fields={[nameField]:r.name};
  if(kind==='riders'){fields.people=[];if(r.userId){const person=unique(await rows(T.people,['entity_uid','recognition_person_uid']),x=>cell(x,'entity_uid')===r.userId||(e.linkedUser?.recognitionPersonId&&cell(x,'recognition_person_uid')===e.linkedUser.recognitionPersonId));if(!person)fail('person_mapping_required');fields.people=[{id:person.id}];}}
  if(targetId)await target.updateRecordAsync(targetId,fields);else targetId=await target.createRecordAsync(fields);
  if(kind==='barn')tenantId=targetId;
  const memberId=await membership(e.actor,tenantId,kind==='barn'&&!e.targetId);
  if(kind==='riders'){
   roster=roster||unique(await rows(T.roster,['Tenant','Rider','Horse','Status']),x=>cell(x,'Tenant')?.some(v=>v.id===tenantId)&&cell(x,'Rider')?.some(v=>v.id===targetId));
   if(roster){if(choice(cell(roster,'Status'))!=='Enabled'||cell(roster,'Horse')?.length)fail('roster_not_enabled');}
   else await base.getTable(T.roster).createRecordAsync({Name:r.name,Tenant:[{id:tenantId}],Rider:[{id:targetId}],Status:{name:'Enabled'}});
  }
  await saveMap(kind,r,targetId);
  const current=await target.selectRecordAsync(targetId);if(cell(current,nameField)!==r.name)fail('target_readback_mismatch');
  const prior=unique(await rows(T.events,['event_uid']),x=>cell(x,'event_uid')===e.event.eventId);if(prior)fail('duplicate_event');
  await base.getTable(T.events).createRecordAsync({event_uid:e.event.eventId,actor_uid:e.actor.id,entity_uid:r.id,barn_uid:r.barnId,action:e.event.action,kind,outcome:'committed',request_uid:e.payload.requestId,input_hash:e.event.inputHash,result_json:JSON.stringify(r),occurred_at:e.acceptedAt,record_mode:{name:'Test'}});
  await sub.updateRecordAsync(recordId,{'Target record ID':targetId,Membership:[{id:memberId}],'Processing status':{name:'Applied'},'Error / review note':''});
  // Final server readback is a separate verification of the automation's work.
  await send('complete');output.set('result','Applied');
 }catch(error){if(won){const latest=await sub.selectRecordAsync(recordId);if(choice(cell(latest,'Processing status'))!=='Applied')await sub.updateRecordAsync(recordId,{'Processing status':{name:'Needs review'},'Error / review note':error.message||'unconfirmed_save'});}throw error;}
}
await run();
