'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {prepare}=require('./class-worker');
function fixture(){
 const day='2026-10-29',at='2026-10-29T14:00:00Z',identity={customer:15,show:76,date:day,ring:2,group:3};
 const inv={observed_at:at,inventory_complete:true,refs:[{base:'appdl16HJ5ETm8N69',table:'collections',key:'part',sha256:'a'.repeat(64),record:'rec1'}],groups:[{identity,rows:[{'class-id':4,'stage-key':'15|76|2026-10-29|2|123','start-time':'10:00:00','end-time':'10:20:00','total-trips':10,type:'class'}]}]};
 return{data:{source_lane:'data_live',context:{'customer-id':15,'show-id':76,'show-key':'15|76','start-date':day,'end-date':day,'selected-date':day},proof:{'show-key':'15|76','run-key':'run','state-sha256':'d'.repeat(64)},inventories:{[day]:inv}},liveClasses:new Map(),answers:[],now:'2026-10-29T14:05:00Z',runId:'first'};
}
test('writes changed class snapshots and reuses unchanged current evidence',()=>{
 const f=fixture(),a=prepare(f);assert.equal(a.outputs.length,1);assert.equal(a.writes.length,1);
 const rows=a.writes.map((fields,i)=>({id:'r'+i,fields}));const b=prepare({...f,answers:rows,runId:'second',now:'2026-10-29T14:06:00Z'});
 assert.equal(b.outputs.length,1);assert.equal(b.writes.length,0);
});
test('a return to prior content creates a new revision instead of reviving history',()=>{
 const f=fixture(),a=prepare(f);const aa=a.writes[0];f.data.inventories['2026-10-29'].groups[0].rows[0]['total-trips']=11;
 const b=prepare({...f,answers:[{id:'a',fields:aa}],runId:'second',now:'2026-10-29T14:06:00Z'});const bb=b.writes[0];
 f.data.inventories['2026-10-29'].groups[0].rows[0]['total-trips']=10;
 const c=prepare({...f,answers:[{id:'a',fields:aa},{id:'b',fields:bb}],runId:'third',now:'2026-10-29T14:07:00Z'});
 assert.equal(c.writes.length,1);assert.equal(c.writes[0]['content-hash'],aa['content-hash']);assert.notEqual(c.writes[0]['answer-key'],aa['answer-key']);
});
