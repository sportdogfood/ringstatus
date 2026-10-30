'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {validateCoverage}=require('./verify-data'),{evaluate}=require('./pace'),{adapt}=require('./class-adapter');
function fixture(){
 const day='2026-10-30',at='2026-10-30T14:00:00Z',now='2026-10-30T14:01:00Z';
 const identity={customer:15,show:76,date:day,ring:1,group:2};
 const item={'stage-key':'15|76|2026-10-30|1|123','class-id':3,'start-time':'10:00:00','end-time':'10:20:00','total-trips':10,type:'class'};
 const group={identity,rows:[item]},refs=[{base:'appdl16HJ5ETm8N69',table:'collections',key:'obs',sha256:'a'.repeat(64),record:'rec'}];
 const inventory={observed_at:at,inventory_complete:true,refs,groups:[group]};
 const data={source_lane:'data_live',context:{'customer-id':15,'show-id':76,'show-key':'15|76','start-date':day,'end-date':day,'selected-date':day},proof:{'show-key':'15|76','run-key':'run','state-sha256':'d'.repeat(64)},inventories:{[day]:inventory}};
 const paceRefs=[{base:'appCaSmL819KK7UFJ',table:'shows',record:'show',observed_at:at},...refs.map(r=>({...r,observed_at:at}))];
 const pace=evaluate({identity,rows:[item],observedAt:at,inputRefs:paceRefs,now,sourceLane:'data_live'}).fields;
 const classResult=adapt({lane:'data_live',now,data,paceRows:[],liveClasses:new Map()})[0].fields;
 const expected=new Map([[pace['scope-key'],{kind:'ring-pace',inventory,group}],[classResult['scope-key'],{kind:'class-timing',inventory,group}]]);
 return{expected,answers:[{fields:pace},{fields:classResult}]};
}
test('acceptance checks every expected scope, including unchanged answers',()=>{
 const {expected,answers}=fixture();assert.deepEqual(validateCoverage(expected,answers),{groups:1,classes:1,validated:2});
 assert.throws(()=>validateCoverage(expected,answers.slice(0,1)),/Missing expected class-timing/);
});
test('acceptance rejects corrupted unchanged answer and changed provenance',()=>{
 const {expected,answers}=fixture(),bad=structuredClone(answers);let p=JSON.parse(bad[1].fields['payload-json']);p.duration_minutes=999;bad[1].fields['payload-json']=JSON.stringify(p);
 assert.throws(()=>validateCoverage(expected,bad),/Class snapshot hash/);
 const other=structuredClone(answers),refs=JSON.parse(other[0].fields['input-refs-json']);refs[1].sha256='b'.repeat(64);other[0].fields['input-refs-json']=JSON.stringify(refs);
 assert.throws(()=>validateCoverage(expected,other),/Data evidence mismatch/);
});
test('valid unavailable stale class is accepted without fabricated data provenance',()=>{
 const {expected,answers}=fixture();const entry=[...expected.entries()].find(([,v])=>v.kind==='class-timing');const required=entry[1];
 const {evaluateClassTiming}=require('./class-timing'),old=JSON.parse(answers[1].fields['payload-json']);
 const stale=evaluateClassTiming({identity:old.identity,sourceLane:'data_live',timezone:'America/New_York',now:'2026-10-30T14:31:00Z',data:{identity:old.identity,source_lane:'data_live',observed_at:'2026-10-30T14:00:00Z',valid_until:'2026-10-30T14:30:00Z',ref:{source_key:'obs',revision:'a'.repeat(64)}}});
 const latest=[answers[0],{fields:stale.fields}];assert.doesNotThrow(()=>validateCoverage(expected,latest));
});
