import {recordRenewal} from './health-records.mjs';
import decoder from './worker.js';
import {tcpFetch} from './tcp-fetch.mjs';
import {publicRoute,fetchPublic} from './public-live.mjs';

let cached,renewal;
class TokenRenewalError extends Error {
 constructor(reason,status=null){super(reason);this.status=status;}
}
async function readAnonymousToken(response){
 let data;
 try{data=await response.json();}catch{throw new TokenRenewalError('Config response was not valid JSON',response.status);}
 const token=data?._t;
 if(token==null||token==='')throw new TokenRenewalError('Config response is missing _t',response.status);
 let claims;
 try{
  claims=JSON.parse(atob(token.split('.')[1].replaceAll('-','+').replaceAll('_','/')));
 }catch{throw new TokenRenewalError('Config token is malformed',response.status);}
 if(claims?.anon!==true)throw new TokenRenewalError('Config token is not anonymous',response.status);
 if(!Number.isFinite(claims.exp))throw new TokenRenewalError('Config token expiry is missing or invalid',response.status);
 if(claims.exp*1000<=Date.now())throw new TokenRenewalError('Config token is expired',response.status);
 return {token,expires:claims.exp*1000};
}
async function sourceToken(env){
 if(cached&&cached.expires>Date.now()+86400000&&cached.cookie===env.SGL_COOKIE)return cached.token;
 if(!env.SGL_COOKIE)throw new TokenRenewalError('Fallback cookie not configured');
 renewal??=(async()=>{
  const started=new Date().toISOString(),id=crypto.randomUUID();let status=null;
  try{
  const response=await tcpFetch('https://sglapi.wellingtoninternational.com/utilities/get-config',{headers:{
   Accept:'application/json, text/plain, */*',Cookie:env.SGL_COOKIE,
   'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1',
   Origin:'https://www.wellingtoninternational.com',Referer:'https://www.wellingtoninternational.com/',
   'sgl-request-origin':'SGL-API','Cache-Control':'no-cache','Accept-Language':'en-US,en;q=0.9','Sec-Fetch-Site':'same-site'
  }});
  status=response.status;
  if(!response.ok)throw new TokenRenewalError('Config request returned an HTTP error',status);
  const {token,expires}=await readAnonymousToken(response);
  cached={token,expires,cookie:env.SGL_COOKIE};
  await recordRenewal(env,{'refresh-id':id,'started-at':started,'completed-at':new Date().toISOString(),lane:'cloudflare-fallback',endpoint:'https://sglapi.wellingtoninternational.com/utilities/get-config',reason:env.REFRESH_REASON||'missing-or-near-expiry',result:'Succeeded','http-status':status,'token-expires-at':new Date(cached.expires).toISOString()});
  await new Promise(resolve=>setTimeout(resolve,1000));
  return token;
 }catch(error){
  const failure=error instanceof TokenRenewalError?error:new TokenRenewalError('Config transport failed before a response was available',status);
  await recordRenewal(env,{'refresh-id':id,'started-at':started,'completed-at':new Date().toISOString(),lane:'cloudflare-fallback',endpoint:'https://sglapi.wellingtoninternational.com/utilities/get-config',reason:env.REFRESH_REASON||'missing-or-near-expiry',result:'Failed','http-status':failure.status,error:failure.message});
  throw failure;
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
  if(report&&!response.ok)report({schedule_upstream_status:sourceStatus,reason:sourceStatus===null?'Schedule request failed before a response was available':sourceStatus>=300&&sourceStatus<400?'Schedule redirect was blocked':sourceStatus>=400?'Schedule returned an HTTP error':'Schedule response could not be decoded'});
  return response;
 }catch(error){
  if(report)report(error instanceof TokenRenewalError
   ?{stage:'token-before-schedule',reason:error.message,token_http_status:error.status}
   :{stage:'fallback',reason:'Fallback failed'});
  console.error(JSON.stringify({message:'Direct source fallback failed',path:new URL(request.url).pathname}));
  return Response.json({message:'Source fallback unavailable',statusCode:502},{status:502,headers:{'Cache-Control':'no-store'}});
 }
}
