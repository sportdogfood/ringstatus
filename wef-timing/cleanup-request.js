'use strict';
// Coordinator-to-runner handoff only. This module cannot invoke cleanup or delete data.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const digest=value=>crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
function normalize(event,now){
 const ms=Date.parse(now),at=Date.parse(event?.discovered_at);
 if(!Number.isFinite(ms)||!Number.isFinite(at)||at>ms)throw Error('Original discovery time missing or future');
 if(!event||typeof event.event_id!=='string'||!event.event_id.trim()||typeof event.source_ref!=='string'||!event.source_ref.trim())throw Error('Discovery event identity/source reference missing');
 if(![event.customer,event.old_show,event.new_show].every(x=>Number.isSafeInteger(x)&&x>0)||event.old_show===event.new_show)throw Error('Invalid new-show identity');
 if(event.coordinator!==undefined&&event.coordinator!=='wef-timing-coordinator')throw Error('Unexpected coordinator');
 return{version:1,coordinator:'wef-timing-coordinator',event_id:event.event_id,customer:event.customer,old_show:event.old_show,new_show:event.new_show,discovered_at:event.discovered_at,source_ref:event.source_ref};
}
function read({runtime,now=new Date().toISOString()}){
 const file=path.join(runtime,'cleanup-request.json');let bytes;
 try{bytes=fs.readFileSync(file,'utf8');}catch(e){if(e.code==='ENOENT')return null;throw e;}
 const raw=JSON.parse(bytes),event=normalize(raw,now);
 if(raw.version!==1||raw.coordinator!==event.coordinator||raw.request_hash!==digest(event))throw Error('Cleanup request identity/hash mismatch');
 return{...event,request_hash:raw.request_hash};
}
function publish({runtime,event,now=new Date().toISOString()}){
 const normalized=normalize(event,now),request={...normalized,request_hash:digest(normalized)};
 fs.mkdirSync(runtime,{recursive:true});
 const previous=read({runtime,now});
 if(previous){if(previous.request_hash===request.request_hash)return{status:'already-pending',request:previous};throw Error('A cleanup request already exists; preserve it for runner reconciliation');}
 const file=path.join(runtime,'cleanup-request.json'),temp=file+'.'+crypto.randomUUID()+'.tmp';
 let fd;
 try{
  fd=fs.openSync(temp,'wx');fs.writeFileSync(fd,JSON.stringify(request,null,2));fs.fsyncSync(fd);fs.closeSync(fd);fd=undefined;
  // Atomic publication without replacing another pending request.
  fs.linkSync(temp,file);
 }finally{if(fd!==undefined)fs.closeSync(fd);if(fs.existsSync(temp))fs.unlinkSync(temp);}
 const saved=read({runtime,now});if(saved.request_hash!==request.request_hash)throw Error('Cleanup request readback mismatch');
 return{status:'pending',request:saved};
}
module.exports={normalize,read,publish};
