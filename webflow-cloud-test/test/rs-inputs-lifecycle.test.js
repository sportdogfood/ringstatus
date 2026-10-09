import test from 'node:test';
import assert from 'node:assert/strict';
import { createAirtableInputStore } from '../src/lib/rs-inputs-airtable.js';

// Existing schema choices verified 2026-10-08: Active/Inactive/Archived,
// Test/Live. All calls below terminate in this in-memory provider.
const env={RS_INPUTS_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_WRITE_MODE:'isolated-trial',AIRTABLE_TOKEN:'synthetic-only'};
function fixture(){
  const tables=new Map(),calls=[];
  const rows=table=>{if(!tables.has(table))tables.set(table,[]);return tables.get(table);};
  const store=createAirtableInputStore({env,minimumIntervalMs:0,fetchImpl:async(value,init)=>{
    const url=new URL(value);assert.equal(url.origin,'https://api.airtable.com');
    assert.equal(url.pathname.split('/')[2],env.RS_INPUTS_BASE_ID);
    const table=decodeURIComponent(url.pathname.split('/')[3]);
    assert.ok(['rs_input_barns','rs_input_users','rs_input_riders','rs_input_horses','rs_input_locations','rs_input_events'].includes(table));
    const body=init.body?JSON.parse(init.body):null;calls.push({table,method:init.method,body});
    if(init.method==='GET'){
      const match=/^\{(entity_uid|event_uid)\} = '([^']+)'$/.exec(url.searchParams.get('filterByFormula')||'');
      return Response.json({records:structuredClone(match?rows(table).filter(r=>r.fields[match[1]]===match[2]):rows(table))});
    }
    assert.equal(init.method,'PATCH');
    const fields=body.records[0].fields,key=body.performUpsert.fieldsToMergeOn[0];
    let row=rows(table).find(r=>r.fields[key]===fields[key]);
    if(!row){row={id:`recSynthetic${rows(table).length}`,fields:{}};rows(table).push(row);}
    Object.assign(row.fields,fields);return Response.json({records:[structuredClone(row)]});
  }});
  return {store,rows,calls};
}
const record={id:'entity-synthetic',barnId:'barn-synthetic',name:'Synthetic',ownerUid:'person-synthetic',revision:1,requestUid:'request-synthetic',requestHash:'hash-synthetic'};
const event={eventId:'event-synthetic',actorId:'person-synthetic',kind:'horses',barnId:record.barnId,requestId:'request-synthetic',inputHash:'hash-synthetic',record,occurredAt:'2026-10-09T01:00:00.000Z',action:'create'};

test('new isolated-trial entities receive Active/Test without accepting caller lifecycle values',async()=>{
  for(const [kind,table] of [['barn','rs_input_barns'],['users','rs_input_users'],['riders','rs_input_riders'],['horses','rs_input_horses'],['locations','rs_input_locations']]){
    const f=fixture();await f.store.put(kind,{...record,status:'Archived',record_mode:'Live'});
    assert.equal(f.rows(table)[0].fields.status,'Active');
    assert.equal(f.rows(table)[0].fields.record_mode,'Test');
  }
});
test('editing existing entity preserves explicit or blank legacy lifecycle',async()=>{
  for(const lifecycle of [{status:'Inactive',record_mode:'Live'},{status:'Archived',record_mode:'Test'},{}]){
    const f=fixture();f.rows('rs_input_horses').push({id:'recExisting',fields:{entity_uid:record.id,revision:1,...lifecycle}});
    await f.store.put('horses',{...record,name:'Edited',revision:2},{expectedRevision:1});
    const saved=f.rows('rs_input_horses')[0].fields;
    assert.equal(saved.status,lifecycle.status);assert.equal(saved.record_mode,lifecycle.record_mode);
    const sent=f.calls.find(c=>c.method==='PATCH').body.records[0].fields;
    assert.equal(Object.hasOwn(sent,'status'),false);assert.equal(Object.hasOwn(sent,'record_mode'),false);
  }
});
test('new audit is Test; replay preserves existing or blank legacy classification',async()=>{
  const f=fixture();await f.store.appendEvent(event);assert.equal(f.rows('rs_input_events')[0].fields.record_mode,'Test');
  for(const mode of ['Live',undefined]){
    const g=fixture();g.rows('rs_input_events').push({id:'recLegacy',fields:{event_uid:event.eventId,...(mode?{record_mode:mode}:{})}});
    await g.store.appendEvent(event);assert.equal(g.rows('rs_input_events')[0].fields.record_mode,mode);
    assert.equal(Object.hasOwn(g.calls.find(c=>c.method==='PATCH').body.records[0].fields,'record_mode'),false);
  }
});
test('ambiguous existing audit cannot be silently upserted',async()=>{
  const f=fixture();f.rows('rs_input_events').push({id:'recOne',fields:{event_uid:event.eventId}},{id:'recTwo',fields:{event_uid:event.eventId}});
  await assert.rejects(f.store.appendEvent(event),{code:'ambiguous_audit_event'});
  assert.equal(f.calls.some(c=>c.method==='PATCH'),false);
});
test('adapter refuses every non-designated base before any provider access',()=>{
  for(const base of [undefined,'','appOtherSynthetic','appZahVgD156cMAe3','apptdhhNzduxm5gjn']){
    assert.throws(()=>createAirtableInputStore({env:{...env,RS_INPUTS_BASE_ID:base},fetchImpl:()=>assert.fail('No data access permitted')}),{code:'clean_input_base_required'});
  }
});
