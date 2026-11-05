import {isIP} from 'node:net';

function coordinates(latitude,longitude,source,observedAt){
 if(latitude===null||latitude===undefined||String(latitude).trim()===''||longitude===null||longitude===undefined||String(longitude).trim()==='')return null;
 const lat=Number(latitude),lon=Number(longitude);
 return Number.isFinite(lat)&&Number.isFinite(lon)&&Math.abs(lat)<=90&&Math.abs(lon)<=180?{latitude:lat,longitude:lon,source,observedAt}:null;
}
// Same server-side GeoJS fallback already used for regional geography. No IP is returned or stored.
export async function profileGeoip(request,fetchImpl=fetch){
 const observedAt=new Date().toISOString(),cf=request.cf||{};
 const supplied=coordinates(cf.latitude,cf.longitude,'cloudflare',observedAt);if(supplied)return supplied;
 const ip=[request.headers.get('CF-Connecting-IP'),(request.headers.get('X-Forwarded-For')||'').split(',')[0]].map(v=>(v||'').trim()).find(v=>isIP(v));
 if(!ip)return null;
 try{
  const response=await fetchImpl(`https://get.geojs.io/v1/ip/geo/${encodeURIComponent(ip)}.json`,{signal:AbortSignal.timeout(1500)});
  if(!response.ok)return null;
  const value=await response.json();return coordinates(value.latitude,value.longitude,'geoip',observedAt);
 }catch{return null;}
}
