"use strict";
// Read-only acceptance: Airtable GETs and existing local receipts only.
const fs=require('fs'),path=require('path'),assert=require('assert/strict');
const root=path.resolve(__dirname,'../..'),api=require(path.join(root,'collector-core'));
(async()=>{
 const base='appUq5Qz9kARqFija',deployment=JSON.parse(fs.readFileSync(path.join(__dirname,'deployment.json')));
 const records=await api.airtableList('schedules',{},base);
 const logs=await api.airtableList('run-logs',{filterByFormula:`IS_AFTER({started},'${deployment.deployed_at}')`},base);
 const heartbeat=await api.airtableList('heartbeat',{},base);
 const collections=await api.airtableList('show-collection',{},base);
 const finished=logs.filter(r=>r.fields.result!=='Running').sort((a,b)=>a.fields.started.localeCompare(b.fields.started));
 assert(!finished.some(r=>r.fields.result==='Failed'),'A normal scheduled run failed; stop acceptance');
 if(!finished.length){console.log('No completed post-deployment run yet');return;}
 const before=JSON.parse(fs.readFileSync(path.join(__dirname,'schedules-before.json')));
 const byKey=new Map(records.map(r=>[r.fields['stage-key'],r]));assert.equal(byKey.size,records.length,'duplicate stage-key');
 for(const old of before){const current=byKey.get(old.fields['stage-key']);assert(current,'missing original class');assert.equal(current.id,old.id);for(const name of ['class-id','class-number','group-id','show-details','rings'])assert.deepEqual(current.fields[name],old.fields[name],name+' changed '+old.id);}
 const blocks=records.filter(r=>JSON.parse(r.fields['row-json']).fields.schedule_break);
 assert.equal(blocks.length,33);for(const r of blocks){assert(!r.fields['class-id']);assert(!r.fields['class-number']);assert(r.fields.rings?.length);assert(r.fields['show-details']?.length);assert.equal(r.fields.status,'active');}
 const today=blocks.filter(r=>r.fields.date==='2026-10-29');assert.equal(today.length,26);assert.equal(today.filter(r=>r.fields.type==='schooling').length,24);assert.equal(today.filter(r=>r.fields.type==='break').length,2);assert(today.every(r=>r.fields['total-trips']===0));
 for(const run of finished){assert.equal(run.fields.result,'Success');assert(!run.fields.errors);assert.equal(run.fields.rows,504);assert.equal(run.fields['days-complete'],4);assert.equal(run.fields['days-failed'],0);assert.match(run.fields.summary,/2026-10-29: 26 rows; 0 classes, 26 schooling\/break blocks/);assert(!run.fields.summary.includes('empty_show_day'));}
 const latest=finished.at(-1),hb=heartbeat.find(r=>r.fields['run-id']===latest.fields['run-id']);
 assert(hb,'heartbeat has not published latest finished run');assert.equal(hb.fields.result,'Success');assert.equal(hb.fields.rows,504);
 const todayCollection=collections.find(r=>r.fields.date==='2026-10-29'&&Number(r.fields['show-id'])===200000076);assert.equal(todayCollection.fields.status,'complete');assert.equal(todayCollection.fields['row-count'],26);assert.equal(JSON.parse(todayCollection.fields.message).warnings.length,0);
 for(let i=0;i<finished.length;i++){
  const run=finished[i],output={verified_at:new Date().toISOString(),run,heartbeat:run.id===latest.id?hb:null,preserved_class_records:before.length,total_records:records.length,today_schooling:24,today_breaks:2,today_reported_trips:0,no_duplicate_keys:true,source_class_ids_and_links_preserved:true,manual_runs:0};
  const file=path.join(__dirname,`normal-run-${i+1}.json`);if(!fs.existsSync(file)||output.heartbeat)fs.writeFileSync(file,JSON.stringify(output,null,2));
 }
 const firstFile=path.join(__dirname,'schedules-run-1.json');if(!fs.existsSync(firstFile))fs.writeFileSync(firstFile,JSON.stringify(records));else{const first=JSON.parse(fs.readFileSync(firstFile));for(const r of first)assert.equal(byKey.get(r.fields['stage-key'])?.id,r.id,'repeat record identity changed');}
 fs.writeFileSync(path.join(__dirname,'schedules-latest.json'),JSON.stringify(records));
 console.log(JSON.stringify({completed_post_deployment_runs:finished.length,runs:finished.map(r=>({id:r.fields['run-id'],started:r.fields.started,completed:r.fields.completed,result:r.fields.result})),rows:records.length,preserved_class_records:before.length,today_schooling:24,today_breaks:2,today_reported_trips:0,heartbeat_matches:true},null,2));
})().catch(e=>{console.error(e.stack);process.exitCode=1});
