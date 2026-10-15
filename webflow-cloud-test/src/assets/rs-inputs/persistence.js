// Device state is a copy only. Callers must revalidate server access before reads/replay.
const copy=value=>JSON.parse(JSON.stringify(value));
export function indexedDBStorage(indexedDB=globalThis.indexedDB){
 let opened;
 function open(){return opened ||= new Promise((resolve,reject)=>{
  if(!indexedDB){reject(Error('device_storage_unavailable'));return;}
  const request=indexedDB.open('rs-inputs-device-v1',1);
  request.onupgradeneeded=()=>request.result.createObjectStore('state');
  request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);request.onblocked=()=>reject(Error('device_storage_blocked'));
 });}
 async function read(key){const db=await open();return new Promise((resolve,reject)=>{
  const tx=db.transaction('state','readonly'),request=tx.objectStore('state').get(key);let result;
  request.onsuccess=()=>{result=request.result};tx.oncomplete=()=>resolve(result||null);
  tx.onerror=()=>reject(tx.error||Error('device_storage_failed'));tx.onabort=()=>reject(tx.error||Error('device_storage_failed'));
 });}
 // A single readwrite transaction serializes this store across connections/tabs.
 // The mutation callback must be synchronous, so the transaction stays active.
 async function mutate(key,fn){const db=await open();return new Promise((resolve,reject)=>{
  const tx=db.transaction('state','readwrite'),store=tx.objectStore('state'),request=store.get(key);let result,failure;
  request.onsuccess=()=>{
   try{const changed=fn(request.result||null);result=changed.result;store.put(changed.state,key);}
   catch(error){failure=error;tx.abort();}
  };
  tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(failure||tx.error||Error('device_storage_failed'));tx.onabort=()=>reject(failure||tx.error||Error('device_storage_failed'));
 });}
 return {read,mutate};
}
export function createPersistence({scope,storage=indexedDBStorage(),now=()=>Date.now()}){
 if(!scope?.actorId||!scope?.resource||!scope?.tenantId)throw Error('verified_scope_required');
 if(typeof storage?.read!=='function'||typeof storage?.mutate!=='function')throw Error('atomic_storage_required');
 const key=JSON.stringify([scope.resource,scope.tenantId,scope.actorId]);
 const empty=()=>({generation:0,draft:null,dirty:false,confirmed:null,pending:null});
 let chain=Promise.resolve();
 const serial=fn=>{const next=chain.then(fn);chain=next.catch(()=>{});return next};
 const read=async()=>copy(await storage.read(key)||empty());
 const change=fn=>serial(()=>storage.mutate(key,current=>{const state=copy(current||empty());const result=fn(state);return {state,result:copy(result===undefined?state:result)}}));
 const operations=state=>[state.pending,...(state.retainedPending||[])].filter(Boolean);
 const matching=(state,op)=>operations(state).find(item=>item.body.requestId===op?.requestId&&item.generation===op?.generation);
 const matches=(state,op)=>!!matching(state,op);
 const fingerprint=value=>Array.isArray(value)?`[${value.map(fingerprint).join(',')}]`:value&&typeof value==='object'?`{${Object.keys(value).filter(k=>k!=='requestId').sort().map(k=>JSON.stringify(k)+':'+fingerprint(value[k])).join(',')}}`:JSON.stringify(value);
 const target=body=>JSON.stringify([body.kind,body.barnId,body.draft?.id||String(body.draft?.name||'').trim().toLowerCase()]);
 function removeOperation(state,op){
  if(state.pending?.requestId===op.requestId)state.pending=null;
  state.retainedPending=(state.retainedPending||[]).filter(item=>item.requestId!==op.requestId);
  if(!state.pending)state.pending=state.retainedPending.shift()||null;
 }

 return {
  load:()=>serial(read),
  edit:data=>change(state=>{state.generation++;state.draft=copy(data);state.dirty=true;delete state.restoredPendingId;}),
  refresh:(data,revision)=>change(state=>{state.confirmed={data:copy(data),revision,updatedAt:now()};if(!state.dirty&&!state.pending)state.draft=copy(data);}),
  prepare:body=>change(state=>{
   if(state.pending){if(JSON.stringify(state.pending.body)!==JSON.stringify(body))throw Error('pending_operation_unresolved');return state.pending;}
   if(!body?.requestId)throw Error('request_id_required');
   state.pending={body:copy(body),requestId:body.requestId,generation:state.generation,attempts:0,error:null};return state.pending;
  }),
  // WEC failedActions: keep independent failed records, match retries by payload,
  // and remove only the operation whose server result was confirmed.
  prepareIndependent:body=>change(state=>{
   if(!body?.requestId)throw Error('request_id_required');
   const all=operations(state),same=all.find(item=>fingerprint(item.body)===fingerprint(body));
   if(same)return same;
   if(all.some(item=>target(item.body)===target(body)))throw Error('pending_record_unresolved');
   state.generation++;state.draft=copy(body);state.dirty=true;delete state.restoredPendingId;
   const op={body:copy(body),requestId:body.requestId,generation:state.generation,attempts:0,error:null};
   if(state.pending)(state.retainedPending ||= []).push(op);else state.pending=op;
   return op;
  }),
  confirm:(op,data,revision)=>change(state=>{
   if(!matches(state,op))return false;
   state.confirmed={data:copy(data),revision,updatedAt:now()};removeOperation(state,op);
   if(state.generation===op.generation||state.restoredPendingId===op.requestId){state.draft=copy(state.pending?.body||data);state.dirty=!!state.pending;if(state.pending)state.restoredPendingId=state.pending.requestId;else delete state.restoredPendingId;}return true;
  }),
  fail:(op,error)=>change(state=>{const item=matching(state,op);if(item){item.attempts++;item.error=error;} }),
  // Only a proven pre-admission revision rejection may be replaced. Keep the
  // user's draft; unknown outcomes and admitted work retain their exact request.
  rejectRevision:op=>change(state=>{
   const item=matching(state,op);if(!item||item.error!=='intake_record_changed')return false;
   removeOperation(state,op);return true;
  })
 };
}
