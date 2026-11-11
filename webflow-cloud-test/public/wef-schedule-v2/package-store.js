import {diffRecords} from './data-diff.js';
// Reuses the existing rs-inputs persistence transaction pattern, in a separate show store.
// Profile, drafts, comments and recognition storage are never opened here.
export function packageStore(indexedDB=globalThis.indexedDB){
 let opened;
 const open=()=>opened ||= new Promise((resolve,reject)=>{
  if(!indexedDB){reject(Error('device_storage_unavailable'));return;}
  const request=indexedDB.open('rs-schedule-packages-v1',1);
  request.onupgradeneeded=()=>{
   request.result.createObjectStore('packages');
   request.result.createObjectStore('records',{keyPath:'key'}).createIndex('package','package');
  };
  request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);request.onblocked=()=>reject(Error('device_storage_blocked'));
 });
 async function access(showId,lane,incoming){
  const db=await open(),scope=JSON.stringify([showId,lane]);
  return new Promise((resolve,reject)=>{
   const tx=db.transaction(['packages','records'],incoming?'readwrite':'readonly'),packages=tx.objectStore('packages'),records=tx.objectStore('records');
   let result=null,failure;
   const manifestRequest=packages.get(scope);
   manifestRequest.onsuccess=()=>{
    const manifest=manifestRequest.result,rowsRequest=records.index('package').getAll(scope);
    rowsRequest.onsuccess=()=>{try{
     const map=new Map(rowsRequest.result.map(r=>[r.value.key,r.value]));
     const old=manifest?{...manifest,records:manifest.order.map(id=>map.get(id))}:null;
     if(!incoming){if(old)delete old.order;result=old;return;}
     if(old&&old.metadata.appliedBaseVersion===incoming.metadata.appliedBaseVersion&&Date.parse(old.metadata.package.startedAt)>Date.parse(incoming.metadata.package.startedAt)){result=old;return;}
     const diff=diffRecords(old?.records||[],incoming.records,'key');
     for(const id of [...diff.added,...diff.updated]){
      const value=diff.records.find(r=>r.key===id);records.put({key:JSON.stringify([scope,id]),package:scope,value});
     }
     for(const id of diff.removed)records.delete(JSON.stringify([scope,id]));
     const {records:ignored,...header}=incoming;
     packages.put({...header,order:incoming.records.map(r=>r.key)},scope);
     result={...incoming,records:diff.records};
    }catch(error){failure=error;tx.abort();}};
   };
   tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(failure||tx.error||Error('device_storage_failed'));tx.onabort=()=>reject(failure||tx.error||Error('device_storage_failed'));
  });
 }
 return {read:(show,lane)=>access(show,lane),commit:(show,lane,data)=>access(show,lane,data)};
}
