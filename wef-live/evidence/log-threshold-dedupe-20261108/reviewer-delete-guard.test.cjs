'use strict';
globalThis.fetch=async()=>{throw Error('Offline network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict');
test('review: actual adapter still deletes eligible historical errs-log while creation remains disabled',async()=>{
 const rows=[{id:'recOldError',createdTime:'2026-11-01T00:00:00Z',fields:{'error-key':'old|1','run-id':'old','occurred-at':'2026-11-01T00:00:00Z',message:'historical'}}];
 let current=structuredClone(rows);const calls=[];
 const request=async(url,options)=>{calls.push({url,method:options.method});
  if(url.includes('/shows?'))return{ok:true,json:async()=>({records:[{id:'recShow',fields:{customer_id:15,show_id:77,is_default:true}}]})};
  assert(url.includes('/errs-log?'));if(options.method==='DELETE'){current=[];return{ok:true,json:async()=>({records:[{id:'recOldError',deleted:true}]})};}
  assert.equal(options.method,'GET');return{ok:true,json:async()=>({records:structuredClone(current)})};
 };
 const api=require('./candidate/wef-live/airtable').airtable({token:'offline',pause:async()=>{},request});let deleted=0;
 await api.retireCleanup('errs-log',rows,{show:77,trigger:'quick-cleanup',deletedIds:new Set(),protectedRuns:new Set()},ids=>deleted+=ids.length);
 assert.equal(deleted,1);assert.equal(calls.filter(x=>x.method==='DELETE').length,1);assert.equal(current.length,0);
 await assert.rejects(api.call('appgzosUcVqA4vZdf','errs-log','POST',{records:[]}),/disabled by owner/);assert(!calls.some(c=>c.method==='POST'));
});
