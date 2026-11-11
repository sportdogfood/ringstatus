import { publicRoute } from './public-live.mjs';
import { fetchFallback } from './fallback.mjs';
import { DurableObject } from 'cloudflare:workers';
import { createPacer } from './sgl-pacing.mjs';
const reply = (data, status=200, extra={}) => new Response(JSON.stringify(data), {
  status, headers:{"Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store",...extra}
});
export const sourceHandler = {
  async fetch(request, env) {
    const url=new URL(request.url), pathname=url.pathname.replace(/\/+$/, "") || "/";
    if(request.method!=="GET") return reply({message:"GET only"},405);
    if(pathname==="/health"){
      const show = env.HEALTH_SHOW_ID;
      if (!/^\d+$/.test(show || ""))
        return reply({ok:false,worker:"wef-schedules-decoded",message:"Health check is not configured"},503);
      const ping = new URL("/schedule", url);
      ping.search = "?date=00/00/00&show_id=" + show + "&customer_id=15";
      const result = await this.fetch(new Request(ping),env);
      const data = await result.json().catch(() => null);
      const hasRings = Array.isArray(data?.rings);
      const ok = result.ok && result.headers.get("x-rs-decoded") === "1" &&
        String(data?.show?.show_id) === show && Boolean(data?.show_date) && hasRings;
      return reply({ok,worker:"wef-schedules-decoded",show_id:show,rings:hasRings ? data.rings.length : 0,
        schedule_status:result.status},ok ? 200 : 503);
    }
    const publicFeed = publicRoute(url);
    if (publicFeed && !publicFeed.valid) return reply({message:"Invalid source parameters"},400);
    if (!publicFeed) {
      const isRingDetail = /^\/ring\/[1-9]\d*$/.test(pathname);
      const isPeople=/^\/people\/\d+$/.test(pathname);
      const isClasses=/^\/classes(?:\/\d+)?$/.test(pathname);
      const isEntries=pathname==="/entries";
      if(!["/ring","/schedule"].includes(pathname)&&!isRingDetail&&!isPeople&&!isClasses&&!isEntries)
        return reply({message:"Route not found"},404);
      const allowed=isRingDetail?["show_date","show_id","customer_id"]:pathname==="/ring"?["customer_id"]:pathname==="/schedule"?["date","show_id","customer_id"]:isPeople?["pid","show_id","customer_id"]:isEntries?["show_id","customer_id","page","sort_on","sort_type","search_text"]:["show_id","customer_id","cgid"];
      if([...url.searchParams.keys()].some(k=>!allowed.includes(k)) ||
         url.searchParams.get("customer_id")!=="15" ||
         (pathname!=="/ring"&&!/^\d+$/.test(url.searchParams.get("show_id")||"")) ||
         (isRingDetail&&!/^\d{4}-\d{2}-\d{2}$/.test(url.searchParams.get("show_date")||"")) ||
         (isPeople&&url.searchParams.get("pid")!==pathname.split("/")[2]) ||
         (isEntries&&(!/^[1-9]\d*$/.test(url.searchParams.get("page")||"") ||
            (url.searchParams.has("sort_on")&&url.searchParams.get("sort_on")!=="number") ||
            (url.searchParams.has("sort_type")&&url.searchParams.get("sort_type")!=="asc") ||
            (url.searchParams.has("search_text")&&url.searchParams.get("search_text")!==""))) ||
         (pathname==="/schedule"&&!/^(?:\d{4}-\d{2}-\d{2}|00\/00\/00)$/.test(url.searchParams.get("date")||"")))
        return reply({message:"Invalid source parameters"},400);
    }
    const fallbackEligible=true; // All source routes have already passed validation.
    const fallback=async reason=>{
      console.error(JSON.stringify({message:'Using direct source fallback',path:pathname,reason}));
      const response=await fetchFallback(request,env);
      const headers=new Headers(response.headers);headers.set('X-RS-Source-Lane','cloudflare-fallback');
      return new Response(response.body,{status:response.status,headers});
    };
    if(fallbackEligible&&env.RUNNER_KEY&&request.headers.get('X-RS-Fallback-Test')===env.RUNNER_KEY)return fallback('authorized-verification');
    try {
      const target = new URL(env.RUNNER_ORIGIN);
      if (target.protocol !== "https:") throw Error("Invalid runner origin");
      target.pathname = pathname;
      target.search = url.search;
      const response = await fetch(target, {
        headers: {Authorization: "Bearer " + env.RUNNER_KEY},
        redirect: "manual", cache: "no-store", signal: AbortSignal.timeout(30000)
      });
      if(fallbackEligible){
        if(!response.ok){await response.body?.cancel();return fallback('runner-http-'+response.status);}
        if(!publicFeed){
          if(response.headers.get('x-rs-decoded')!=='1'){await response.body?.cancel();return fallback('runner-undecoded');}
          const payload=await response.clone().json().catch(()=>null);
          if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).length===0){await response.body?.cancel();return fallback('runner-empty-or-invalid');}
        }
      }
      if (response.status >= 300 && response.status < 400) return reply({message:"Runner redirect blocked"},502);
      if (!publicFeed && response.ok && response.headers.get("x-rs-decoded") !== "1") return reply({message:"Undecoded response"},502);
      return new Response(response.body,{status:response.status,headers:{
        "Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store",
        ...(fallbackEligible?{"X-RS-Source-Lane":"runner"}:{}),
        ...(!publicFeed && response.ok ? {"X-RS-Decoded":"1"} : {})
      }});
    } catch {return fallbackEligible?fallback('runner-unavailable'):reply({message:"Runner unavailable"},502);}
  }
};

export class SglPacer extends DurableObject {
  constructor(ctx,env){
    super(ctx,env);
    this.ready=ctx.blockConcurrencyWhile(async()=>{
      const notBefore=(await ctx.storage.get('nextAllowed'))??0;
      this.enqueue=createPacer({notBefore,persist:value=>ctx.storage.put('nextAllowed',value)});
    });
  }
  async fetch(request){
    await this.ready;
    return this.enqueue(async({startedAt})=>{
      const response=await sourceHandler.fetch(request,this.env);
      const body=response.body===null?null:await response.arrayBuffer();
      const headers=new Headers(response.headers);
      headers.set('X-RS-Queue-Start',String(startedAt));
      headers.set('X-RS-Queue-End',String(Date.now()));
      return new Response(body,{status:response.status,headers});
    });
  }
}

export default {
  async fetch(request,env){
    if(request.method!=='GET')return sourceHandler.fetch(request,env);
    try{
      if(!env.SGL_PACER)throw Error('Shared pacing not configured');
      return await env.SGL_PACER.get(env.SGL_PACER.idFromName('sgl-source')).fetch(request);
    }catch{
      console.error(JSON.stringify({message:'Shared source queue unavailable'}));
      return reply({message:'Source queue unavailable',statusCode:503},503);
    }
  }
};
