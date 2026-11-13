// Test utility only. No route, check-in UI, map provider or new dependency.
// Haversine distance follows https://turfjs.org/docs/api/distance.
export function estimateWalk(device,target,metresPerSecond=1.4){
 for(const point of [device,target])if(!point||!Number.isFinite(point.latitude)||!Number.isFinite(point.longitude)||Math.abs(point.latitude)>90||Math.abs(point.longitude)>180)throw new RangeError('Invalid coordinates');
 if(!Number.isFinite(metresPerSecond)||metresPerSecond<=0)throw new RangeError('Invalid walking speed');
 const rad=value=>value*Math.PI/180;
 const dlat=rad(target.latitude-device.latitude),dlon=rad(target.longitude-device.longitude);
 const a=Math.sin(dlat/2)**2+Math.cos(rad(device.latitude))*Math.cos(rad(target.latitude))*Math.sin(dlon/2)**2;
 const metres=6371008.8*2*Math.atan2(Math.sqrt(Math.min(1,a)),Math.sqrt(Math.max(0,1-a)));
 return {metres,minutes:device.source==='gps'?metres/metresPerSecond/60:null,metresPerSecond,basis:'straight-line',approximate:true,source:device.source};
}

// Test policy parameters are supplied explicitly; no production radius is chosen.
export function withinRing(device,target,{radiusFeet,maxAgeMs,now}){
 if(!Number.isFinite(radiusFeet)||radiusFeet<=0||!Number.isFinite(maxAgeMs)||maxAgeMs<=0||!Number.isFinite(now))throw new RangeError('Explicit radius and freshness required');
 const age=now-Date.parse(device.observedAt);
 if(device.source!=='gps'||!Number.isFinite(device.accuracy)||device.accuracy<0||!Number.isFinite(age)||age<0||age>maxAgeMs)return {qualified:false,reason:'fresh-accurate-gps-required'};
 const {metres}=estimateWalk(device,target);
 return {qualified:metres+device.accuracy<=radiusFeet*0.3048,metres,accuracy:device.accuracy,radiusFeet};
}
