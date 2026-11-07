import decoder from './decoder.mjs';
import {tcpFetch} from './tcp-fetch.mjs';
const ua='Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1';
let cached,renewals=0;
const headers=env=>({Accept:'application/json, text/plain, */*','User-Agent':ua,Cookie:env.SGL_COOKIE,Origin:'https://www.wellingtoninternational.com',Referer:'https://www.wellingtoninternational.com/','sgl-request-origin':'SGL-API','Cache-Control':'no-cache','Accept-Language':'en-US,en;q=0.9'});
export default{async fetch(request,env){
 const url=new URL(request.url);
 try{
  if(url.pathname==='/wire'){
   const h=headers(env);h.Cookie='diagnostic=placeholder';const r=await tcpFetch('https://tls.peet.ws/api/all',{headers:h});const p=await r.json();const received=p.http1?.headers||p.http2?.sent_frames?.find(f=>f.frame_type==='HEADERS')?.headers||[];
   return Response.json({status:r.status,http:p.http_version,receivedCFWorker:received.filter(h=>/^cf-worker:/i.test(h)),realCredentialsSent:false});
  }
  if(url.pathname==='/renew'){
   const r=await tcpFetch('https://sglapi.wellingtoninternational.com/utilities/get-config',{headers:headers(env)});if(!r.ok)throw Error('Token source status '+r.status);const p=await r.json();const c=JSON.parse(atob(p._t.split('.')[1].replaceAll('-','+').replaceAll('_','/')));if(c.anon!==true||!Number.isFinite(c.exp)||c.exp*1000<=Date.now())throw Error('Invalid anonymous token');cached={token:p._t,exp:c.exp};renewals++;
   return Response.json({ok:true,renewals,anonymous:true,issued:new Date(c.iat*1000).toISOString(),expires:new Date(c.exp*1000).toISOString(),renewedInsideCloudflare:true});
  }
  if(!['/schedule','/classes/200031233'].includes(url.pathname))return new Response('not found',{status:404});
  if(!cached||cached.exp*1000<=Date.now()){
   const renewed=await this.fetch(new Request(new URL('/renew',url)),env);if(!renewed.ok)return renewed;
   await new Promise(resolve=>setTimeout(resolve,1000));
  }
  const r=await decoder.fetch(request,{SGL_TOKEN:cached.token,SGL_COOKIE:env.SGL_COOKIE});const body=await r.text();let p;try{p=JSON.parse(body)}catch{}
  return Response.json({status:r.status,decoded:r.headers.get('x-rs-decoded'),bytes:new TextEncoder().encode(body).length,showId:p?.show?.show_id??null,date:p?.show_date??null,rings:p?.rings?.length??null,classId:p?.class?.class_id??null,trips:p?.trips?.length??null,entryIds:p?.trips?.filter(t=>Number(t.entry_id)>0).length??null,resolvedClassIds:p?.rings?.flatMap(r=>r.classes||[]).filter(c=>Number(c.class_id)>0).length??null,empty:body.trim()==='{}',renewals,error:r.ok?undefined:p?.message});
 }catch(e){return Response.json({stage:'independent-preview',error:e.message},{status:502});}
}};
