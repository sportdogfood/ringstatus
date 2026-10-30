"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {airtable}=require('../airtable');
test('restored adapter rejects Live reads and writes before network access',async()=>{
 let calls=0;const api=airtable({pause:async()=>{},request:async()=>{calls++;throw Error('Network must not be reached');}});
 for(const table of ['live-entries','live-classes','collections'])await assert.rejects(api.list(table,'appgzosUcVqA4vZdf'),/Unapproved read target/);
 await assert.rejects(api.call('appgzosUcVqA4vZdf','live-entries','PATCH',{}),/Unapproved/);assert.equal(calls,0);
});
