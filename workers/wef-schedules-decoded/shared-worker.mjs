const reply = (data, status=200, extra={}) => new Response(JSON.stringify(data), {
  status, headers:{"Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store",...extra}
});
export default {
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
    const isPeople=/^\/people\/\d+$/.test(pathname);
    const isClasses=/^\/classes(?:\/\d+)?$/.test(pathname);
    const isEntries=pathname==="/entries";
    if(!["/ring","/schedule"].includes(pathname)&&!isPeople&&!isClasses&&!isEntries)
      return reply({message:"Route not found"},404);
    const allowed=pathname==="/ring"?["customer_id"]:pathname==="/schedule"?["date","show_id","customer_id"]:isPeople?["pid","show_id","customer_id"]:isEntries?["show_id","customer_id","page","sort_on","sort_type","search_text"]:["show_id","customer_id","cgid"];
    if([...url.searchParams.keys()].some(k=>!allowed.includes(k)) ||
       url.searchParams.get("customer_id")!=="15" ||
       (pathname!=="/ring"&&!/^\d+$/.test(url.searchParams.get("show_id")||"")) ||
       (isPeople&&url.searchParams.get("pid")!==pathname.split("/")[2]) ||
       (isEntries&&(!/^[1-9]\d*$/.test(url.searchParams.get("page")||"") ||
          (url.searchParams.has("sort_on")&&url.searchParams.get("sort_on")!=="number") ||
          (url.searchParams.has("sort_type")&&url.searchParams.get("sort_type")!=="asc") ||
          (url.searchParams.has("search_text")&&url.searchParams.get("search_text")!==""))) ||
       (pathname==="/schedule"&&!/^(?:\d{4}-\d{2}-\d{2}|00\/00\/00)$/.test(url.searchParams.get("date")||"")))
      return reply({message:"Invalid source parameters"},400);
    try {
      const target = new URL(env.RUNNER_ORIGIN);
      if (target.protocol !== "https:") throw Error("Invalid runner origin");
      target.pathname = pathname;
      target.search = url.search;
      const response = await fetch(target, {
        headers: {Authorization: "Bearer " + env.RUNNER_KEY},
        redirect: "manual", cache: "no-store", signal: AbortSignal.timeout(30000)
      });
      if (response.status >= 300 && response.status < 400) return reply({message:"Runner redirect blocked"},502);
      if (response.ok && response.headers.get("x-rs-decoded") !== "1") return reply({message:"Undecoded response"},502);
      return new Response(response.body,{status:response.status,headers:{
        "Content-Type":"application/json; charset=utf-8","Cache-Control":"no-store",
        ...(response.ok ? {"X-RS-Decoded":"1"} : {})
      }});
    } catch {return reply({message:"Runner unavailable"},502);}
  }
};
