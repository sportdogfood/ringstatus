const test=require('node:test'),assert=require('node:assert/strict');
const model=require('./model'),{linkRetained}=require('./collector');
const fs=require('node:fs'),path=require('node:path');
function fixture(name='partial'){
 const data=JSON.parse(fs.readFileSync(path.resolve(__dirname,'../../../tests/fixtures/narrow-matcher',name+'.json')));
 const date=data.payload.class_related_data.date.slice(0,10),now=date+'T12:00:00Z';
 const show={'show-key':'15|'+data.payload.show_id,show_id:data.payload.show_id,customer_id:15,start_date:date,end_date:date,selected_date:date,last_checked:now,is_default:true};
 return{shows:[{id:'canonicalShow',fields:show}],sourceSchedules:data.schedules||[],localSchedules:data.schedules||[],classes:[{payload:data.payload,observedAt:now,sourceEvidence:{revision:'offline'},inventory:{revision:'offline',complete:true,showId:show.show_id,customerId:15,groups:data.groups}}],now};
}
test('canonical scope has no tenant dependency',()=>{const f=fixture();assert.equal(model.scope(f.shows,f.now).showRecord,'canonicalShow');assert.equal(model.owners,undefined);assert.equal(model.selections,undefined);});
test('uncertain group retains completed class rows and ordinary schedule links',async()=>{const r=await linkRetained(fixture());assert.equal(r.rows.length,1);assert.equal(r.unresolved.length,0);assert.equal(r.completedScopes.length,1);assert.equal(r.rows[0]['schedule-link-basis'],'class-date');assert.equal(r.linkIssues.length,1);assert.equal(r.linkIssues[0].kind,'group');});
test('incomplete local schedule sync does not discard source entity facts',async()=>{const f=fixture();f.localSchedules=[];const r=await linkRetained(f);assert.equal(r.rows.length,1);assert.equal(r.rows[0].schedules,undefined);assert(r.linkIssues.some(x=>x.kind==='schedule-reference'));});
test('one invalid source scope does not stop another or require optional consumers',async()=>{const f=fixture();f.classes.push({...f.classes[0],payload:{...f.classes[0].payload,show_id:999}});const r=await linkRetained(f);assert.equal(r.rows.length,1);assert.equal(r.unresolved.length,1);assert.deepEqual(r.optionalConnections,{messages:'developing',results:'developing',waze:'developing'});});
test('valid empty scope completes without group or schedule dependencies',async()=>{const f=fixture();f.classes[0].payload.trips=[];f.classes[0].payload.total_entry_trips=0;f.sourceSchedules=[];f.localSchedules=[];const r=await linkRetained(f);assert.deepEqual(r.rows,[]);assert.equal(r.completedScopes[0].count,0);assert.deepEqual(r.linkIssues,[]);assert.deepEqual(r.unresolved,[]);});
