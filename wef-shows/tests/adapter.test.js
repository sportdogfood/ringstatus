const test=require('node:test'),assert=require('node:assert/strict'),{airtable}=require('../airtable');
test('Airtable timestamp readback accepts equivalent source offsets without repeated writes',async()=>{
 const rows=[],writes=[];const request=async(url,{method,body})=>{if(method==='POST'){const data=JSON.parse(body);for(const r of data.records){rows.push({id:'rec1',fields:{...r.fields,'count-observed-at':new Date(r.fields['count-observed-at']).toISOString()}});}writes.push(method);}return{ok:true,json:async()=>({records:structuredClone(rows)})};};
 const api=airtable({request,pause:async()=>{}}),value={'show-day-key':'15|76|2026-10-29','count-observed-at':'2026-10-29T13:00:00+00:00'};
 await api.upsert('show-days','show-day-key',[value]);await api.upsert('show-days','show-day-key',[value]);assert.deepEqual(writes,['POST']);
 await assert.rejects(api.call('appdl16HJ5ETm8N69','runs','PATCH',{}),/forbidden/);await assert.rejects(api.call('appdl16HJ5ETm8N69','entries'),/Unapproved/);
});
