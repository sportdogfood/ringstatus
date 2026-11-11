import {packageStore} from './package-store.js';
import {assemblePackages,validDataset} from './device-data.js';
export let records=[],days=[],metadata={timezone:'America/New_York'};
export const ringDetails={};
let sharedRings={};
export function selectRingDetails(date){
 for(const key of Object.keys(ringDetails))delete ringDetails[key];
 Object.assign(ringDetails,days.find(day=>day.date===date)?.ringDetails??sharedRings);
}
export function replaceData(data){
 records=data.records;days=data.days;metadata=data.metadata;
 sharedRings=data.ringDetails;
 selectRingDetails(data.metadata.defaultDate);
}
const endpoint=document.body.dataset.scheduleEndpoint;
const scopeKey='rs-schedule-app-current-scope-v1';
if(endpoint){
 // Prepare confirmed device data before making the scope request.
 try{
  const scope=JSON.parse(localStorage.getItem(scopeKey));
  if(scope?.showId){
   const storage=packageStore(),packages={};
   for(const lane of ['day0','full']){const data=await storage.read(scope.showId,lane);if(validDataset(data,scope.showId,lane))packages[lane]=data;}
   const cached=assemblePackages(packages.full,packages.day0);if(cached)replaceData(cached);
  }
 }catch{}
}else{replaceData(await import('./data.js?v=entry-counts-1'));}
export const scopeReady=endpoint?(async()=>{
 try{
  const response=await fetch(new URL('../wef-schedule/scope',location.href),{cache:'no-store'});
  if(!response.ok)throw Error('scope_unavailable');
  const scope=await response.json();
  if(!/^rec[A-Za-z0-9]{14}$/.test(scope.showId||''))throw Error('invalid_scope');
  try{localStorage.setItem(scopeKey,JSON.stringify(scope));}catch{}
  return scope;
 }catch{
  document.querySelector('.rings-shell-footer').textContent='Refresh unavailable - showing last loaded data';
  return metadata.showId?metadata:null;
 }
})():Promise.resolve(metadata);
