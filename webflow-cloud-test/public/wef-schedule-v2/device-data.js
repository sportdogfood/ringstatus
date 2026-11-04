import {diffRecords,fingerprint} from './data-diff.js';
import {packageStore} from './package-store.js';
export const REFRESH_MS=180000;
export const FULL_REFRESH_MS=900000;
export function validDataset(data,showId,lane){
 const scope=data?.metadata?.package;
 if(!data||data.metadata?.showId!==showId||!scope||scope.lane!==lane||scope.complete!==true||!Number.isFinite(Date.parse(scope.startedAt))||!Number.isFinite(Date.parse(data.metadata.refreshedAt||data.metadata.capturedAt)))return false;
 if(!Array.isArray(data.days)||!data.days.length||!Array.isArray(data.records)||!data.records.length||!data.ringDetails)return false;
 if(lane==='day0'&&(data.days.length!==1||data.days[0].date!==data.metadata.defaultDate))return false;
 if(fingerprint([...scope.dates||[]].sort())!==fingerprint(data.days.map(d=>d.date).sort()))return false;
 try{diffRecords([],data.records,'key');}catch{return false;}
 return data.records.every(r=>data.days.some(d=>d.date===r.date)&&Array.isArray(r.entries)&&data.ringDetails[r.ring]);
}
export function assemblePackages(full,day0){
 if(!full)return day0;if(!day0)return full;
 const date=day0.days[0].date;
 const records=[...full.records.filter(r=>r.date!==date),...day0.records].sort((a,b)=>a.date.localeCompare(b.date)||(day0.ringDetails[a.ring]||full.ringDetails[a.ring]).number-(day0.ringDetails[b.ring]||full.ringDetails[b.ring]).number||a.position-b.position||a.key.localeCompare(b.key));
 const days=[...full.days.filter(d=>d.date!==date),...day0.days].sort((a,b)=>a.date.localeCompare(b.date));
 return {metadata:{...full.metadata,defaultDate:date,refreshedAt:day0.metadata.refreshedAt,day0CheckedAt:day0.metadata.refreshedAt,fullCheckedAt:full.metadata.refreshedAt},days,records,ringDetails:{...full.ringDetails,...day0.ringDetails}};
}
export function trainerOptions(records,day){
 const trainers=new Map();
 for(const row of records.filter(r=>r.date===day&&r.entryCount!==0))for(const e of row.entries)if(e.trainerId&&e.trainer)trainers.set(e.trainerId,e.trainer);
 return [...trainers].sort((a,b)=>a[1].localeCompare(b[1]));
}
export function filterTrainer(records,trainerId){
 return trainerId?records.map(r=>({...r,entries:r.entries.filter(e=>e.trainerId===trainerId)})).filter(r=>r.entries.length):records;
}
export function createDeviceData({showId,endpoint,storage=packageStore(),seed=null,fetchImpl=fetch,onData,onError=()=>{},onChecked=()=>{}}){
 const packages={day0:null,full:null},inFlight={day0:null,full:null},timers={day0:null,full:null};
 let current=seed,stopped=true,generation=0;
 const timestamp=d=>Date.parse(d.metadata.package.startedAt);
 function emit(){
  const next=assemblePackages(packages.full||seed,packages.day0);if(!next)return;
  const diff=diffRecords(current?.records||[],next.records,'key');
  const changed=diff.changed||fingerprint(current?.days)!==fingerprint(next.days)||fingerprint(current?.ringDetails)!==fingerprint(next.ringDetails)||current?.metadata.defaultDate!==next.metadata.defaultDate;
  current={...next,records:diff.records};if(changed)onData(current,diff);
 }
 async function refresh(lane='day0'){
  if(stopped||!endpoint)return;if(!['day0','full'].includes(lane))throw Error('invalid_lane');
  if(inFlight[lane])return inFlight[lane];const run=generation;
  inFlight[lane]=(async()=>{try{
   const url=new URL(endpoint);url.searchParams.set('show',showId);url.searchParams.set('lane',lane);
   const response=await fetchImpl(url,{cache:'no-store',signal:AbortSignal.timeout(120000)});
   if(!response.ok)throw Error('schedule_refresh_failed');const data=await response.json();
   if(stopped||run!==generation)return;
   if(!validDataset(data,showId,lane))throw Error('invalid_schedule_package');
   if(packages[lane]&&timestamp(data)<timestamp(packages[lane]))return;
   // Commit a complete package only; unchanged records are not rewritten.
   let accepted=data;try{accepted=await storage.commit(showId,lane,data);}catch(error){onError(error,current,lane);}
   if(stopped||run!==generation)return;
   packages[lane]=accepted;emit();onChecked(current,lane);
  }catch(error){if(!stopped&&run===generation)onError(error,current,lane);}finally{inFlight[lane]=null;}})();
  return inFlight[lane];
 }
 async function initialize(run){
  for(const lane of ['day0','full']){
   try{const data=await storage.read(showId,lane);if(stopped||run!==generation)return;if(validDataset(data,showId,lane))packages[lane]=data;}catch{}
  }
  if(stopped||run!==generation)return;emit();
  await refresh('day0');if(stopped||run!==generation)return;
  // Start the fast lane independently; the full load cannot hold it up.
  timers.day0=setInterval(()=>void refresh('day0'),REFRESH_MS);
  await refresh('full');if(stopped||run!==generation)return;
  timers.full=setInterval(()=>void refresh('full'),FULL_REFRESH_MS);
 }
 return {get current(){return current;},refresh,start(){
  stopped=false;const run=++generation;const ready=initialize(run);
  const stop=()=>{stopped=true;generation++;Object.values(timers).forEach(clearInterval);};stop.ready=ready;return stop;
 }};
}
