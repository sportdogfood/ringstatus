// Uses the shared native controller across the extension/page boundary.
// No fetch, credential logging, cookie access or URL-based success inference.
globalThis.RecognizeTestAdapter=function(document){
 const root=()=>document.getElementById('rs-native-recognize');
 function snapshot(){try{return JSON.parse(root()?.getAttribute('data-rs-recognition-state')||'null')||{stage:'unavailable',busy:false,halted:false,allowedActions:[]};}catch{return {stage:'unavailable',busy:false,halted:false,allowedActions:[]};}}
 async function perform(action,value){
  const node=root(),state=snapshot();if(!node||state.busy||state.halted)throw new Error('native_unavailable_or_busy');
  return new Promise((resolve,reject)=>{
   const id=crypto.randomUUID();
   const done=event=>{if(event.detail?.id!==id)return;cleanup();event.detail.error?reject(new Error(event.detail.error)):resolve(event.detail.result);};
   const timer=setTimeout(()=>{cleanup();reject(new Error('native_outcome_unconfirmed'));},40000);
   function cleanup(){clearTimeout(timer);node.removeEventListener('rs-recognition-result',done);}
   node.addEventListener('rs-recognition-result',done);
   node.dispatchEvent(new CustomEvent('rs-recognition-command',{detail:{id,action,value}}));
  });
 }
 return {snapshot,perform};
};
