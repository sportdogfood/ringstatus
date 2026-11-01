import {remembered} from './rs-recognition-iam.js';

// Private Inputs eligibility is separate from recognizing a person.
export function projectIamAccess({identity,people,memberships,tenants,roles}) {
 const matches=people.filter(p=>p.fields.recognition_person_uid===identity.fields.person_uid);
 if(matches.length!==1||!['active','test'].includes(String(matches[0].fields.status).toLowerCase()))return {inputsAllowed:false,barnIds:[],owner:false,roles:[]};
 const p=matches[0],all=memberships.filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===p.id&&m.fields.Tenant?.length===1);
 const enabled=all.filter(m=>m.fields.Status==='Enabled'&&tenants.some(t=>t.id===m.fields.Tenant[0]&&t.fields.tenant_active===true)&&all.filter(other=>other.fields.Tenant[0]===m.fields.Tenant[0]).length===1);
 const owner=enabled.some(m=>roles.some(r=>r.fields.rec_name==='owner'&&m.fields.Roles?.includes(r.id)));
 const digits=String(p.fields.phone_e164||'').replace(/\D/g,'');
 const registeredPhone=/^1\d{10}$/.test(digits.length===10?'1'+digits:digits);
 return {inputsAllowed:['invited','approved'].includes(identity.fields.input_access)&&enabled.length>0&&(owner||registeredPhone),barnIds:enabled.map(m=>m.fields.Tenant[0]),owner,roles:[...new Set(enabled.flatMap(m=>m.fields.Roles||[]))]};
}

export async function iamActor(request,env) {
 const p=await remembered(request,env);
 // Pre-existing snapshots contain only eligible identities. New snapshots carry
 // the explicit eligibility decision; GeoIP-only devices never authorize Inputs.
 if(!p||p.identity_only||p.inputsAllowed===false){const error=new Error('authentication_required');error.code='authentication_required';error.status=401;throw error;}
 return {id:p.person_uid,barnIds:p.barnIds,permissions:['inputs:read','inputs:write'],profile:{personUid:p.person_uid,name:p.person_name,email:p.email}};
}
