'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const model=require('../model');
const row={'show-id':76,date:'2026-10-29','class-id':7,'ring-number':3};
const candidate={key:'unchanged-data-key',recordId:'recData',customerId:15,showId:76,date:row.date,classId:7,ringNumber:3,kind:'class',sourceAsOf:'2026-10-29T12:00:00Z',projectionAsOf:'2026-10-29T12:01:00Z',sourceEvidence:{collections:[{key:'original'}]}};
test('Data occurrence association preserves its key and ID as JSON only',()=>{
 const result=model.dataAssociation(row,{customer_id:15,show_id:76},[candidate]);
 assert.equal(result.status,'linked');assert.equal(result.key,candidate.key);assert.equal(result.recordId,'recData');assert.equal(result.sourceAsOf,candidate.sourceAsOf);assert.equal(result.schedules,undefined);
});
test('Data lane conflicts do not arbitrarily choose an occurrence or cross scope',()=>{
 assert.equal(model.dataAssociation(row,{customer_id:15,show_id:76},[candidate,{...candidate,key:'another'}]).status,'Conflict');
 for(const field of ['showId','customerId','ringNumber','classId','date'])assert.equal(model.dataAssociation(row,{customer_id:15,show_id:76},[{...candidate,[field]:'wrong'}]).status,'unresolved');
});
