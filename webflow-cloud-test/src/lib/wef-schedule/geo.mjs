// Approximate network location only. Do not infer GPS precision or retain raw IP.
export function approximateLocation(request){
 const cf=request?.cf;
 const coordinate=(value,limit)=>{
  if(value===null||value===undefined||String(value).trim()==='')return null;
  const number=Number(value);return Number.isFinite(number)&&Math.abs(number)<=limit?number:null;
 };
 const latitude=coordinate(cf?.latitude,90),longitude=coordinate(cf?.longitude,180);
 if(latitude===null||longitude===null)return {available:false,source:'geoip',approximate:true,latitude:null,longitude:null};
 return {available:true,source:'geoip',approximate:true,latitude,longitude};
}
