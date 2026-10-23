// Matches the public SGL React bundle inspected 2026-10-21. This is public
// key-derivation material, not a credential. The upstream bearer stays here.
const SGL_BUNDLE_SALT = "5874a2ec29e1d30258abe7a00220fd5d";

async function decodeSglPayload(body, authorization, salt, keepBlobs = false) {
  const payload = JSON.parse(body);
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw Error("Invalid payload");
  // A token refresh requires a fresh authenticated response, not a guessed key.
  if (payload._t) throw Error("Upstream token refresh required");
  const keys = new Map();
  async function visit(value) {
    if (!value || typeof value !== "object") return;
    if (Array.isArray(value)) {
      for (const row of value) await visit(row);
      return;
    }
    if (Object.hasOwn(value, "_")) {
      if (!authorization || typeof value._ !== "string") throw Error("Missing authentication or blob");
      const bytes = Uint8Array.from(atob(value._), c => c.charCodeAt(0));
      if (bytes.length < 36) throw Error("Invalid blob length");
      const minute = new TextDecoder().decode(bytes.slice(0, 8));
      if (!/^\d{8}$/.test(minute)) throw Error("Invalid blob minute");
      if (!keys.has(minute)) {
        const material = new TextEncoder().encode(salt + "|" + authorization + "|" + minute);
        const digest = await crypto.subtle.digest("SHA-256", material);
        keys.set(minute, await crypto.subtle.importKey("raw", digest, {name:"AES-GCM"}, false, ["decrypt"]));
      }
      const plaintext = await crypto.subtle.decrypt({name:"AES-GCM", iv:bytes.slice(8,20)}, keys.get(minute), bytes.slice(20));
      const fields = JSON.parse(new TextDecoder("utf-8", {fatal:true}).decode(plaintext));
      if (!fields || typeof fields !== "object" || Array.isArray(fields)) throw Error("Invalid decoded fields");
      for (const [key, field] of Object.entries(fields)) {
        if (["__proto__", "constructor", "prototype", "_", "_t"].includes(key)) throw Error("Invalid decoded key");
        value[key] = field;
      }
      if (!keepBlobs) delete value._;
    }
    for (const child of Object.values(value)) await visit(child);
  }
  await visit(payload);
  return JSON.stringify(payload);
}


const reply = (data, status=200, extra={}) => new Response(JSON.stringify(data), {
  status, headers:{"Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store",...extra}
});
export default {
  async fetch(request, env) {
    const url=new URL(request.url), pathname=url.pathname.replace(/\/+$/, "") || "/";
    if(request.method!=="GET") return reply({message:"GET only"},405);
    if(pathname==="/health") return reply({ok:true,worker:"wef-schedules-decoded"});
    const isPeople=/^\/people\/\d+$/.test(pathname);
    const isClasses=/^\/classes(?:\/\d+)?$/.test(pathname);
    if(!["/ring","/schedule"].includes(pathname)&&!isPeople&&!isClasses)
      return reply({message:"Route not found"},404);
    const allowed=pathname==="/ring"?["customer_id"]:pathname==="/schedule"?["date","show_id","customer_id"]:isPeople?["pid","show_id","customer_id"]:["show_id","customer_id","cgid"];
    if([...url.searchParams.keys()].some(k=>!allowed.includes(k)) ||
       url.searchParams.get("customer_id")!=="15" ||
       (pathname!=="/ring"&&!/^\d+$/.test(url.searchParams.get("show_id")||"")) ||
       (isPeople&&url.searchParams.get("pid")!==pathname.split("/")[2]) ||
       (pathname==="/schedule"&&!/^(?:\d{4}-\d{2}-\d{2}|00\/00\/00)$/.test(url.searchParams.get("date")||"")))
      return reply({message:"Invalid source parameters"},400);
    if(!env.SGL_TOKEN) return reply({message:"Source authentication not configured"},503);
    const upstream=new URL(pathname,"https://sglapi.wellingtoninternational.com");
    upstream.search=url.search;
    const authorization="Bearer "+env.SGL_TOKEN;
    const headers={
      Accept:"application/json, text/plain, */*","User-Agent":"Mozilla/5.0",
      "Cache-Control":"no-cache",Pragma:"no-cache",Expires:"0",
      Authorization:authorization,"sgl-request-origin":"SGL-API",
      Origin:"https://www.wellingtoninternational.com",Referer:"https://www.wellingtoninternational.com/"
    };
    if(env.SGL_COOKIE) headers.Cookie=env.SGL_COOKIE;
    try {
      const response=await fetch(upstream.toString(),{method:"GET",headers,redirect:"manual",signal:AbortSignal.timeout(25000),cf:{cacheTtl:0,cacheEverything:false}});
      if(response.status>=300&&response.status<400) return reply({message:"Source redirect blocked",statusCode:502},502);
      if(!response.ok) return reply({message:"Source request failed",statusCode:response.status},response.status);
      const body=await decodeSglPayload(await response.text(),authorization,env.SGL_BUNDLE_SALT||SGL_BUNDLE_SALT,pathname==="/schedule");
      return new Response(body,{headers:{"Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store","X-RS-Decoded":"1"}});
    } catch {
      return reply({message:"Source fetch or protected response decoding failed",statusCode:502},502);
    }
  }
};

