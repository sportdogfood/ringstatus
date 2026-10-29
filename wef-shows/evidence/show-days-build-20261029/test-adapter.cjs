const test=require('node:test'),assert=require('node:assert/strict'),Module=require('node:module'),fs=require('node:fs'),path=require('node:path');
const filename=path.resolve(__dirname,'../../airtable.js'),m=new Module(filename,module);m.filename=filename;m.paths=Module._nodeModulePaths(path.dirname(filename));m._compile(fs.readFileSync(path.join(__dirname,'staged/airtable.js'),'utf8'),filename);
test('Airtable timestamp readback accepts equivalent source offsets without repeated writes',async()=>{
 const rows=[],writes=[];const request=async(url,{method,body})=>{if(method==='POST'){const data=JSON.parse(body);for(const r of data.records){rows.push({id:'rec1',fields:{...r.fields,'count-observed-at':new Date(r.fields['count-observed-at']).toISOString()}});}writes.push(method);}return{ok:true,json:async()=>({records:structuredClone(rows)})};};
 const api=m.exports.airtable({request,pause:async()=>{}}),value={'show-day-key':'15|76|2026-10-29','count-observed-at':'2026-10-29T13:00:00+00:00'};
 await api.upsert('show-days','show-day-key',[value]);await api.upsert('show-days','show-day-key',[value]);assert.deepEqual(writes,['POST']);
 await assert.rejects(api.call('appdl16HJ5ETm8N69','runs','PATCH',{}),/forbidden/);await assert.rejects(api.call('appdl16HJ5ETm8N69','entries'),/Unapproved/);
});
