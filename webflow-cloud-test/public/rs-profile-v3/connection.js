// Presentation adapter only. Identity, authorization and persistence remain v2's.
export const EMPTY = () => ({recognized:false,currentUserId:null,tenant:{id:null,name:'',ownerUserId:null},users:[]});
export function membership(data,id=data?.identity?.currentId){return data?.memberships?.find(m=>m.userId===id);}
export function isOwner(data){const m=membership(data);return !!(m?.active&&m.role==='Owner'&&!data?.teamConflict);}
export function presentation(data){
 if(!data?.identity?.currentId)return EMPTY();
 const id=data.identity.currentId,m=membership(data),team=data.teams?.find(t=>t.id===m?.teamId);
 if(!data.profiles?.some(p=>p.id===id))return EMPTY();
 return {recognized:true,currentUserId:id,tenant:{id:team?.id||null,name:team?.name||'',ownerUserId:isOwner(data)?id:null},users:data.profiles.map(p=>{const member=membership(data,p.id);return {id:p.id,tenant_id:member?.teamId||null,first_name:p.first||'',last_name:p.last||'',email:p.email||'',sms:p.phone||'',profile_img:p.photo||'',tags:p.tags||[],role:member?.role||'',level:p.level||'',active:member?!!member.active:true,revision:p.revision,created_at:p.created_at,modified_at:p.modified_at};})};
}
export function roster(data){
 const own=membership(data);if(!own?.active||data.teamConflict)return [];
 const hidden=data.hidden||[];
 return presentation(data).users.filter(p=>{const m=membership(data,p.id);return m?.teamId===own.teamId&&m.active&&!hidden.some(x=>x[0]===data.identity.currentId&&x[1]===own.teamId&&x[2]===p.id);});
}
export function canRead(data,id){return id===data?.identity?.currentId||roster(data).some(p=>p.id===id);}
export function profileFields(fd,photo){
 const fields={first:String(fd.get('first_name')||'').trim(),last:String(fd.get('last_name')||'').trim(),email:String(fd.get('email')||'').trim(),phone:String(fd.get('sms')||'').trim(),tags:fd.getAll('tags'),photo:String(fd.get('photo_url')||'').trim()||photo||''};
 if(fields.first.length>200||fields.last.length>200)throw Error('Names must be 200 characters or fewer.');
 const pin=String(fd.get('pin')||'').trim();if(pin){if(!/^\d{4}$/.test(pin))throw Error('Enter a four-digit PIN.');fields.pin=pin;}
 return fields;
}
export function createConnection(service,recognize,onState){
 let data=null,epoch=0,identityWork=null;
 const accept=(next,error)=>{data=next;onState(next,error);};
 service.subscribeProfile(accept);
 return {
  get data(){return data;},
  async start(){if(identityWork)return identityWork;const generation=epoch;const work=service.loadProfile().then(next=>{if(generation===epoch)accept(next);return next;}).catch(error=>{if(generation===epoch)accept(service.profileState(),error);throw error;});identityWork=work;try{return await work;}finally{if(identityWork===work)identityWork=null;}},
  async login(value){const generation=epoch,uid=await recognize.recognize(value);if(generation!==epoch)return null;if(!uid)throw Error('That phone or PIN was not recognized.');const next=await service.loadProfile(uid);if(generation===epoch)accept(next);return next;},
  async signOut(){epoch++;identityWork=null;service.clearProfile();accept(null);await recognize.signOut();},
  load(view){return service.loadProfileView(view);},
  save(operation,values){return service.saveProfile(operation,values);},
  hide(id,hide){const own=membership(data);if(!own?.active||id===data.identity.currentId)return;const key=[data.identity.currentId,own.teamId,id],hidden=(data.hidden||[]).filter(x=>JSON.stringify(x)!==JSON.stringify(key));if(hide)hidden.push(key);service.saveViewPreferences(hidden);accept(service.profileState());}
 };
}
