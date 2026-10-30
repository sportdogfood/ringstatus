'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {retireOld}=require('../retirement');
function fixture(){
 const db=Object.fromEntries(['live-entries','live-classes','live-groups','live-rings','collections'].map(t=>[t,[]]));
 for(const t of Object.keys(db))db[t]=[{id:t,fields:{'show-id':74,date:'2026-10-22','source-kind':'class',entry:['original-link']}}];
 const deleted=[],snapshots=[];
 const api={list:async t=>structuredClone(db[t]),upsert:async(t,k,rs)=>{for(const f of rs){let r=db[t].find(r=>r.fields[k]===f[k]);if(!r){r={id:'catalog'+db[t].length,fields:{}};db[t].push(r);}Object.assign(r.fields,f);}return [];},retire:async(t,rs)=>{assert.equal(snapshots.length,5);assert.equal(db.collections.filter(r=>r.fields['source-kind']==='archive-catalog').length,5);deleted.push(t);db[t]=db[t].filter(r=>!rs.some(x=>x.id===r.id));}};
 const args={api,scope:{show:75,date:'2026-10-23'},now:'2026-10-23T15:00:00Z',report:{run_id:'test',archives:[]},archive:async(k,rs,scope)=>{snapshots.push({k,rs:structuredClone(rs),scope});return {file:k,sha256:k,kind:k,records:rs.length};}};
 return {db,deleted,snapshots,args};
}
test('all snapshots and catalogs precede child-first retirement, catalog survives repeat',async()=>{
 const f=fixture();await retireOld(f.args);assert.deepEqual(f.deleted,['live-entries','live-classes','live-groups','live-rings','collections']);assert(f.snapshots.every(s=>s.rs[0].fields.entry[0]==='original-link'&&s.scope.shows[0]===74));assert.equal(f.db.collections.length,5);assert(f.db.collections.every(r=>r.fields.status==='Retired'));await retireOld(f.args);assert.equal(f.deleted.length,5);
});
for(const failure of ['archive','catalog','readback'])test(failure+' failure prevents every deletion',async()=>{
 const f=fixture();if(failure==='archive')f.args.archive=async()=>{throw Error('bad archive')};
 if(failure==='catalog')f.args.api.upsert=async()=>{throw Error('bad catalog')};
 if(failure==='readback')f.args.api.upsert=async()=>[];
 await assert.rejects(()=>retireOld(f.args));assert.equal(f.deleted.length,0);
});
test('uncertain deletion stops immediately, records uncertainty, no retry',async()=>{
 const f=fixture();let calls=0;f.args.api.retire=async()=>{calls++;throw Error('unknown delete')};await assert.rejects(()=>retireOld(f.args));assert.equal(calls,1);assert(f.db.collections.some(r=>r.fields.status==='Uncertain'));
});
test('upcoming canonical date protects today and future even for other shows',async()=>{
 const f=fixture();for(const t of Object.keys(f.db))for(const date of ['2026-10-23','2026-10-24'])f.db[t].push({id:date,fields:{'show-id':74,date,'source-kind':'class'}});f.args.scope.date='2026-10-24';await retireOld(f.args);for(const t of Object.keys(f.db))assert(f.db[t].some(r=>r.id==='2026-10-23')&&f.db[t].some(r=>r.id==='2026-10-24'));
});
