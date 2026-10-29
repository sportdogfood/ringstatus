const test=require('node:test'),assert=require('node:assert/strict');
const {createAudit}=require('../audit');
test('repeat error persistence is idempotent; previous source success survives a later failed attempt',async()=>{
 const tables={sources:[],'err-logs':[]};const api={upsert:async(t,k,values)=>values.map(f=>{let row=tables[t].find(r=>r.fields[k]===f[k]);if(!row){row={id:t+tables[t].length,fields:{}};tables[t].push(row);}Object.assign(row.fields,f);return row;})};
 const first=createAudit({api,runId:'run1',clock:()=> '2026-10-29T12:00:00Z'});await first.observe('default-ring',async()=>({}));await first.saveSources();
 const second=createAudit({api,runId:'run2',clock:()=> '2026-10-29T12:05:00Z'});try{await second.observe('default-ring',async()=>{throw Error('Failed fixture');});}catch(e){second.capture(e);}await second.saveSources();await second.saveErrors('recRun2');await second.saveErrors('recRun2');
 assert.equal(tables.sources.length,1);assert.equal(tables.sources[0].fields['last-success-at'],'2026-10-29T12:00:00Z');assert.equal(tables['err-logs'].length,1);assert.deepEqual(tables['err-logs'][0].fields.run,['recRun2']);
});
