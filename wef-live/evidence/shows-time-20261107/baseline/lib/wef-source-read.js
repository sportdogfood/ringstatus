"use strict";
// One bounded retry inside a scheduled collection; never starts another run.
async function readSource(url, {request, decoded=false, pause=ms=>new Promise(r=>setTimeout(r,ms)), onRetry=(event)=>console.log(JSON.stringify(event))}={}) {
 const read=request|| (async target=>{const response=await fetch(target,{headers:{Accept:'application/json','Cache-Control':'no-cache'},signal:AbortSignal.timeout(30000)});return {response,text:await response.text()};});
 for(let attempt=0;attempt<2;attempt++) {
  let result;
  try { result=await read(url); }
  catch(error) {
   const transient=['TimeoutError','AbortError'].includes(error.name)||['ETIMEDOUT','ECONNRESET','UND_ERR_CONNECT_TIMEOUT','UND_ERR_SOCKET'].includes(error.cause?.code);
   if(!transient||attempt===1)throw error;
   onRetry({event:'source_retry',path:new URL(url).pathname,reason:'transport timeout/reset',attempt:2});await pause(500);continue;
  }
  const {response,text}=result;
  if(!response.ok){
   if(attempt===0&&[502,503,504].includes(response.status)) {onRetry({event:'source_retry',path:new URL(url).pathname,status:response.status,attempt:2});await pause(500);continue;}
   throw Error('Source HTTP '+response.status+' at '+new URL(url).pathname);
  }
  if(decoded&&response.headers?.get('x-rs-decoded')!=='1')throw Error('Source decoding not confirmed');
  let data;try{data=JSON.parse(text);}catch{throw Error('Unreadable source JSON');}
  if(!data||typeof data!=='object'||data._t||(decoded&&Array.isArray(data)))throw Error('Invalid source payload');
  return data;
 }
}
module.exports={readSource};
