"use strict";
// Bounded recovery of the existing read only. No fallback host or workflow retry.
const networkCodes=new Set(['ECONNRESET','ECONNREFUSED','ETIMEDOUT','ENOTFOUND','EAI_AGAIN','UND_ERR_CONNECT_TIMEOUT','UND_ERR_HEADERS_TIMEOUT','UND_ERR_BODY_TIMEOUT','UND_ERR_SOCKET']);
function codes(error,depth=0){
 if(!error||depth>4)return[];
 const value=String(error.code||error.name||'Error');
 return [/^[A-Za-z0-9_]{1,64}$/.test(value)?value:'Error',...codes(error.cause,depth+1),...(Array.isArray(error.errors)?error.errors:[]).flatMap(e=>codes(e,depth+1))];
}
async function fetchSourceText(url,{request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 const parsed=new URL(url);
 if(parsed.origin!=='https://wef-schedules-decoded.gombcg.workers.dev'||!['/ring','/schedule'].includes(parsed.pathname))throw Error('Unapproved show source');
 let last;
 for(let attempt=1;attempt<=3;attempt++){
  try{
   const response=await request(url,{method:'GET',signal:AbortSignal.timeout(30000),headers:{Accept:'application/json'}});
   if(!response.ok){await response.body?.cancel().catch(()=>{});const error=Error('HTTP '+response.status);error.httpStatus=response.status;error.retryable=response.status===429||(response.status>=500&&response.status<=599);throw error;}
   return{response,text:await response.text()};
  }catch(error){
   last=error;const reasons=codes(error);
   const retryable=error.retryable??reasons.some(c=>networkCodes.has(c)||['AbortError','TimeoutError'].includes(c));
   if(!retryable||attempt===3){
    // Never echo response bodies, request credentials or query strings.
    const reason=error.httpStatus?'HTTP '+error.httpStatus:'request failed';
    throw Error(`Show source GET ${parsed.pathname} failed after ${attempt} attempts: ${reason} [${[...new Set(reasons)].join(' > ')}]`,{cause:last});
   }
  }
  await pause(attempt*500);
 }
}
module.exports={fetchSourceText};
