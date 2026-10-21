"use strict";
// GET-only recovery. Creation/update/delete behavior remains in its existing adapters.
function transient(error){
 if(error?.retryable!==undefined)return error.retryable;
 const codes=new Set(['ECONNRESET','ECONNREFUSED','ETIMEDOUT','ENOTFOUND','EAI_AGAIN','UND_ERR_CONNECT_TIMEOUT','UND_ERR_HEADERS_TIMEOUT','UND_ERR_BODY_TIMEOUT','UND_ERR_SOCKET']);
 return ['AbortError','TimeoutError'].includes(error?.name)||codes.has(error?.code)||/fetch failed|timeout/i.test(error?.message||'')||transientCause(error?.cause)||error?.errors?.some(transient)||false;
}
const transientCause=error=>error?transient(error):false;
function causes(error,depth=0){
 if(!error||depth>4)return[];
 return [error.code||error.name||'Error',...causes(error.cause,depth+1),...(error.errors||[]).flatMap(e=>causes(e,depth+1))];
}
async function readAirtable(url,{headers={},request=globalThis.fetch,pause=ms=>new Promise(r=>setTimeout(r,ms)),timeoutMs=20000,attempts=3,baseMs=400,maxMs=2000}={}){
 const limit=Number.isFinite(Number(attempts))?Math.max(1,Math.floor(Number(attempts))):3;
 const parsed=new URL(url);if(parsed.protocol!=='https:'||parsed.hostname!=='api.airtable.com')throw Error('Unapproved Airtable read host');
 const scope=parsed.origin+parsed.pathname;let last,used=0;
 for(let attempt=1;attempt<=limit;attempt++){
  used=attempt;const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),timeoutMs);
  try{
   const response=await request(url,{method:'GET',headers,signal:controller.signal});
   if(!response.ok){await response.body?.cancel().catch(()=>{});const error=Error('HTTP '+response.status);error.safeReason=error.message;error.retryable=response.status===429||response.status>=500&&response.status<=599;throw error;}
   let value;try{value=await response.json();}catch(error){if(error instanceof SyntaxError){const invalid=Error('Invalid JSON response');invalid.safeReason=invalid.message;invalid.retryable=false;throw invalid;}throw error;}
   if(!Array.isArray(value?.records)){const invalid=Error('Invalid record list');invalid.safeReason=invalid.message;invalid.retryable=false;throw invalid;}
   return value;
  }catch(error){last=error;if(!transient(error)||attempt===limit)break;}
  finally{clearTimeout(timer);}
  await pause(Math.min(maxMs,baseMs*attempt+Math.floor(Math.random()*200)));
 }
 // Never echo headers, query offsets, credentials or response bodies.
 const reason=last?.safeReason||(/fetch failed/i.test(last?.message||'')?'fetch failed':'request failed');
 throw Error(`Airtable GET ${scope} failed after ${used} attempts: ${reason} [${[...new Set(causes(last))].join(' > ')}]`,{cause:last});
}
module.exports={readAirtable};
