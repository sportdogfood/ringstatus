'use strict';
const schema=require('./candidate/schema');
const names=['live-trips','live-trip-results','live-trip-messages','live-trip-comments'];
const plan=names.map(name=>({name,description:name==='live-trips'?'Day-0 source class trips. Source identity per class/entry/UUID; legacy live-entries retained.':name==='live-trip-results'?'Trip-specific source result facts; verification status preserved. No claim of final results from placeholders.':name==='live-trip-messages'?'Messages linked directly to an individual trip. Authoring/delivery belongs to downstream messaging.':'Comments linked directly to an individual trip. Not synthesized by the collector.',fields:schema.tables[name].map(field=>{
 if(name==='live-trips')return schema.field(field);
 if(field==='show-id')return{name:field,type:'number',options:{precision:0}};
 if(['body','source-json'].includes(field))return{name:field,type:'multilineText'};
 if(field==='created-at')return{name:field,type:'singleLineText'};
 return{name:field,type:'singleLineText'};
})}));
console.log(JSON.stringify(plan));
