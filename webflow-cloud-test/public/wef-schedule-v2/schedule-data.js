import * as seed from './data.js?v=entry-counts-1';
export let records=seed.records,days=seed.days,metadata=seed.metadata;
export const ringDetails={...seed.ringDetails};
export function replaceData(data){
 records=data.records;days=data.days;metadata=data.metadata;
 for(const key of Object.keys(ringDetails))delete ringDetails[key];
 Object.assign(ringDetails,data.ringDetails);
}
