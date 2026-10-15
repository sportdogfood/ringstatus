import assert from 'node:assert/strict';
import {recognizeTenantsFixture} from './recognize-tenants-fixture.mjs';
import {TABLE,SOURCE} from '../src/lib/rs-tenant-barn.js';
const origin='https://ringstatus.com';
export async function profileFixture(){
 const f=await recognizeTenantsFixture();
 const ownerRole=f.insert(TABLE.roles,{rec_name:'owner'}),userRole=f.insert(TABLE.roles,{rec_name:'user'}),staffRole=f.insert(TABLE.roles,{rec_name:'staff'});
 const person=f.insert(TABLE.people,{entity_uid:'canonical-owner',recognition_person_uid:f.actor,name:'Fixture Owner',email:'',phone_e164:'+16318752160',status:'Active',record_mode:'Test'});
 const tenant=f.insert(TABLE.barn,{tenant_official_name:'Fixture Barn',tenant_active:true});
 const membership=f.insert(TABLE.memberships,{Name:'Owner',Person:[person.id],Tenant:[tenant.id],Roles:[ownerRole.id],Status:'Enabled'});
 f.insert(TABLE.sources,{mapping_uid:`${SOURCE}|barn|${f.barnId}`,entity_type:'Barn',entity_uid:f.barnId,source_system:SOURCE,source_entity_type:'barn',source_scope:f.barnId,external_id:tenant.id});
 f.insert(TABLE.events,{event_uid:'fixture-barn',entity_uid:f.barnId,kind:'barn',outcome:'committed',result_json:JSON.stringify({id:f.barnId,barnId:f.barnId,name:'Fixture Barn',revision:1,ownerUid:f.actor})});
 async function recognize(kind,value,extra={}){const response=await f.dispatch(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({action:'credential',kind,value,request_id:crypto.randomUUID(),...extra})}));return {status:response.status,...await response.json()};}
 function client(identity,actor=f.actor){return async(path,body)=>{const response=await f.dispatch(new Request(origin+'/test/rs-inputs/'+path,{method:body?'POST':'GET',headers:{Origin:origin,'Content-Type':'application/json',Cookie:`__Host-rs_recognition_device=${identity.device_token}`,'X-RS-Tenants':'barn-rider-v1','X-RS-Actor':actor},...(body?{body:JSON.stringify(body)}:{})}));return {status:response.status,...await response.json()};};}
 const identity=await recognize('phone','6318752160');assert.equal(identity.verified,true);
 return {...f,ownerRole,userRole,staffRole,person,tenant,membership,identity,recognize,client,request:client(identity)};
}
