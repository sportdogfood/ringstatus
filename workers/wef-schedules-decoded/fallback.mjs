import decoder from './worker.js';
import {tcpFetch} from './tcp-fetch.mjs';
import {publicRoute,fetchPublic} from './public-live.mjs';

let cached,renewal;
async function sourceToken(env){
 if(cached&&cached.expires>Date.now()+86400000&&cached.cookie===env.SGL_COOKIE)return cached.token;
 if(!env.SGL_COOKIE)throw Error('Fallback cookie not configured');
 renewal??=(async()=>{
  const response=await tcpFetch('https://sglapi.wellingtoninternational.com/utilities/get-config',{headers:{
   Accept:'application/json, text/plain, */*',Cookie:env.SGL_COOKIE,
   'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1',
   Origin:'https://www.wellingtoninternational.com',Referer:'https://www.wellingtoninternational.com/',
   'sgl-request-origin':'SGL-API','Cache-Control':'no-cache','Accept-Language':'en-US,en;q=0.9'
  }});
  if(!response.ok)throw Error('Fallback token renewal failed');
  const {_t:token}=await response.json();
  const claims=JSON.parse(atob(token.split('.')[1].replaceAll('-','+').replaceAll('_','/')));
  if(claims.anon!==true||!Number.isFinite(claims.exp)||claims.exp*1000<=Date.now())throw Error('Invalid fallback anonymous token');
  cached={token,expires:claims.exp*1000,cookie:env.SGL_COOKIE};
  await new Promise(resolve=>setTimeout(resolve,1000));
  return token;
 })().finally(()=>{renewal=undefined;});
 return renewal;
}
export async function fetchFallback(request,env){
 try{
  const url=new URL(request.url),feed=publicRoute(url);
  if(feed){
   if(!feed.valid)throw Error('Invalid public source parameters');
   const response=await fetchPublic(url,feed.origin,tcpFetch);
   if(response.status>=300&&response.status<400)throw Error('Public source redirect blocked');
   return new Response(response.body,{status:response.status,headers:{'Content-Type':response.headers.get('content-type')||'application/json','Cache-Control':'no-store'}});
  }
  const token=await sourceToken(env);
  return await decoder.fetch(request,{SGL_TOKEN:token,SGL_COOKIE:env.SGL_COOKIE,SGL_FETCH:tcpFetch});
 }catch{
  console.error(JSON.stringify({message:'Direct source fallback failed',path:new URL(request.url).pathname}));
  return Response.json({message:'Source fallback unavailable',statusCode:502},{status:502,headers:{'Cache-Control':'no-store'}});
 }
}
