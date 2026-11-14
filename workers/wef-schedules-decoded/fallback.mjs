import {recordRenewal} from './health-records.mjs';
import decoder from './worker.js';
import {tcpFetch} from './tcp-fetch.mjs';
import {publicRoute,fetchPublic} from './public-live.mjs';

let cached,renewal;
class TokenRenewalError extends Error {
 constructor(reason,status=null){super(reason);this.status=status;}
}
function redactedConfig(value,env){
 if(Array.isArray(value))return value.map(item=>redactedConfig(item,env));
 if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,/^(?:_t|token|access_token|refresh_token|cookie|set-cookie|authorization|password|secret|api[_-]?key)$/i.test(key)?'[redacted]':redactedConfig(item,env)]));
 if(typeof value!=='string')return value;
 for(const secret of [env.SGL_TOKEN,env.SGL_COOKIE,env.SGL_RETURNED_TOKEN,env.RUNNER_KEY,env.WEF_SHOWS_AIRTABLE_TOKEN])if(secret)value=value.split(secret).join('[redacted]');
 return value.replace(/((?:set-cookie|cookie|authorization)\s*:)[^\r\n]*/gi,'$1 [redacted]').replace(/Bearer\s+[^\s"'<>]+/gi,'Bearer [redacted]').replace(/\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g,'[redacted]').replace(/(["']?(?:_t|token|access_token|refresh_token|cookie|authorization|password|secret|api[_-]?key)["']?\s*[:=]\s*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;}<>]+)/gi,'$1"[redacted]"');
}
async function readAnonymousToken(response,env){
 const raw=await response.text();let data;
 try{data=JSON.parse(raw);}catch{}
 const fail=reason=>{const error=new TokenRenewalError(reason,response.status);error.configResponse=redactedConfig(data===undefined?raw:data,env);throw error;};
 if(!response.ok)fail('Config request returned an HTTP error');
 if(data===undefined)fail('Config response was not valid JSON');
 const token=data?._t;
 if(token==null||token==='')fail('Config response is missing _t');
 let claims;
 try{
  claims=JSON.parse(atob(token.split('.')[1].replaceAll('-','+').replaceAll('_','/')));
 }catch{fail('Config token is malformed');}
 if(claims?.anon!==true)fail('Config token is not anonymous');
 if(!Number.isFinite(claims.exp))fail('Config token expiry is missing or invalid');
 if(claims.exp*1000<=Date.now())fail('Config token is expired');
 return {token,expires:claims.exp*1000};
}
async function sourceToken(env){
 if(cached&&cached.expires>Date.now()+86400000&&cached.cookie===env.SGL_COOKIE)return cached.token;
 if(!env.SGL_COOKIE)throw new TokenRenewalError('Fallback cookie not configured');
 renewal??=(async()=>{
  const profiles=[['original-header','Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1'],['windows-clean','Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36 Edg/158.0.0.0'],['android-clean','Mozilla/5.0 (Linux; Android 16; Pixel 10) AppleWebKit/537.36 (KHTML, like Gecko) Edg/157.0.0.0 Mobile Safari/537.36']];
  for(const [index,[profile,userAgent]] of profiles.entries()){
  if(index)await new Promise(resolve=>setTimeout(resolve,1000));
  const started=new Date().toISOString(),id=crypto.randomUUID();let status=null;
  try{
  const response=await tcpFetch('https://sglapi.wellingtoninternational.com/utilities/get-config',{headers:{
   Accept:'application/json, text/plain, */*',Cookie:env.SGL_COOKIE,
   'User-Agent':userAgent,
   Origin:'https://www.wellingtoninternational.com',Referer:'https://www.wellingtoninternational.com/',
   'sgl-request-origin':'SGL-API','Cache-Control':'no-cache','Accept-Language':'en-US,en;q=0.9','Sec-Fetch-Site':'same-site'
  }});
  status=response.status;
  const {token,expires}=await readAnonymousToken(response,env);
  cached={token,expires,cookie:env.SGL_COOKIE};
  await recordRenewal(env,{'refresh-id':id,'started-at':started,'completed-at':new Date().toISOString(),lane:'cloudflare-fallback',endpoint:'https://sglapi.wellingtoninternational.com/utilities/get-config',reason:(env.REFRESH_REASON||'missing-or-near-expiry')+'; profile='+profile,result:'Succeeded','http-status':status,'token-expires-at':new Date(cached.expires).toISOString()});
  await new Promise(resolve=>setTimeout(resolve,1000));
  return token;
 }catch(error){
  const failure=error instanceof TokenRenewalError?error:new TokenRenewalError('Config transport failed before a response was available: '+decoder.diagnostic(error,env),status);
  await recordRenewal(env,{'refresh-id':id,'started-at':started,'completed-at':new Date().toISOString(),lane:'cloudflare-fallback',endpoint:'https://sglapi.wellingtoninternational.com/utilities/get-config',reason:(env.REFRESH_REASON||'missing-or-near-expiry')+'; profile='+profile,result:'Failed','http-status':failure.status,error:failure.message+(failure.configResponse!==undefined?'\nConfig response (redacted): '+JSON.stringify(failure.configResponse):'')});
  if(status!==200||failure.message!=='Config response is missing _t'||index===profiles.length-1)throw failure;
 }
 }
 })().finally(()=>{renewal=undefined;});
 return renewal;
}
export async function renewFallbackToken(env,reason,report){
 cached=undefined;
 try{return await sourceToken({...env,REFRESH_REASON:reason});}
 catch(error){
  if(report&&error instanceof TokenRenewalError)report({reason:error.message,token_http_status:error.status});
  throw error;
 }
}
export async function fetchFallback(request,env,{profile,report}={}){
 try{
  const url=new URL(request.url),feed=publicRoute(url);
  if(feed){
   if(!feed.valid)throw Error('Invalid public source parameters');
   const response=await fetchPublic(url,feed.origin,tcpFetch);
   if(response.status>=300&&response.status<400)throw Error('Public source redirect blocked');
   return new Response(response.body,{status:response.status,headers:{'Content-Type':response.headers.get('content-type')||'application/json','Cache-Control':'no-store'}});
  }
  const token=await sourceToken(env);
  let sourceStatus=null;
  const sourceFetch=report?async(...args)=>{
   report({stage:'schedule-request'});
   const response=await tcpFetch(...args);
   sourceStatus=response.status;
   return response;
  }:tcpFetch;
  const response=await decoder.fetch(request,{SGL_TOKEN:token,SGL_COOKIE:env.SGL_COOKIE,SGL_FETCH:sourceFetch,SGL_HEADER_PROFILE:profile,
   SGL_ON_TOKEN:(next,expires)=>{cached={token:next,expires,cookie:env.SGL_COOKIE};}});
  if(report&&!response.ok){
   const detail=await response.clone().json().catch(()=>null);
   report({schedule_upstream_status:sourceStatus,...(detail?.stage?{stage:detail.stage}:{}),reason:detail?.reason||detail?.message||(sourceStatus===null?'Schedule request failed before a response was available':sourceStatus>=300&&sourceStatus<400?'Schedule redirect was blocked':sourceStatus>=400?'Schedule returned an HTTP error':'Schedule response could not be decoded')});
  }
  return response;
 }catch(error){
  if(report)report(error instanceof TokenRenewalError
   ?{stage:'token-before-schedule',reason:error.message,token_http_status:error.status}
   :{stage:'fallback',reason:'Fallback failed'});
  console.error(JSON.stringify({message:'Direct source fallback failed',path:new URL(request.url).pathname}));
  return Response.json({message:'Source fallback unavailable',statusCode:502,...(error instanceof TokenRenewalError?{stage:'token-before-schedule',reason:error.message,tokenHttpStatus:error.status}:{})},{status:502,headers:{'Cache-Control':'no-store'}});
 }
}
