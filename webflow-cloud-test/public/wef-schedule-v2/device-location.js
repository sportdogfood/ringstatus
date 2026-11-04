// Session memory only. Separate from show data, Profile, comments and refresh cadence.
export let deviceLocation=null;
export async function pingLocation(endpoint,fetchImpl=fetch){
 try{
  const response=await fetchImpl(endpoint,{cache:'no-store',signal:AbortSignal.timeout(2500)});
  if(!response.ok)return null;
  const value=await response.json();
  if(value.available!==true||value.approximate!==true||value.source!=='geoip'||!Number.isFinite(value.latitude)||Math.abs(value.latitude)>90||!Number.isFinite(value.longitude)||Math.abs(value.longitude)>180)return null;
  deviceLocation={latitude:value.latitude,longitude:value.longitude,approximate:true,source:'geoip'};
  return deviceLocation;
 }catch{return null;}
}
