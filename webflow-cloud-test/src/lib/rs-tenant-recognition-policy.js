// Read the existing People, memberships and roles. No second identity index.
import {createTenantRest,TABLE} from './rs-tenant-barn.js';

const phone=value=>{const digits=String(value||'').replace(/\D/g,'');return /^1?\d{10}$/.test(digits)?'+'+(digits.length===10?'1':'')+digits:'';};
export function createTenantRecognitionPolicy({env,fetchImpl=fetch}){
 const call=createTenantRest(env,fetchImpl);
 return async function eligibility(identity){
  const uid=identity?.fields?.person_uid;
  if(!/^[A-Za-z0-9_-]{1,128}$/.test(uid||''))return {allowed:false,owner:false};
  const people=await call(TABLE.people,{formula:`{recognition_person_uid} = '${uid}'`});
  if(people.length!==1||!['Active','Test'].includes(people[0].fields.status))return {allowed:false,owner:false};
  const person=people[0],catalog=await call(TABLE.roles);
  const memberships=(await call(TABLE.memberships)).filter(m=>m.fields.Person?.length===1&&m.fields.Person[0]===person.id&&m.fields.Tenant?.length===1);
  // Duplicate membership rows must not turn a disabled membership into access.
  const enabled=memberships.filter(m=>m.fields.Status==='Enabled'&&memberships.filter(other=>other.fields.Tenant[0]===m.fields.Tenant[0]).length===1&&catalog.some(r=>m.fields.Roles?.includes(r.id)));
  const owner=enabled.some(m=>catalog.some(r=>r.fields.rec_name==='owner'&&m.fields.Roles?.includes(r.id)));
  const registeredPhone=phone(person.fields.phone_e164);
  // Owners can accept their pre-established invitation before adding a phone.
  // Every User requires the owner-established phone, including PIN/cookie entry.
  // A stable device/PIN identity survives an interrupted phone synchronization.
  // Phone entry separately matches the canonical registered phone.
  return {allowed:enabled.length>0&&(owner||!!registeredPhone),owner,phone:registeredPhone,personId:person.id};
 };
}
