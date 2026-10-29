const test=require('node:test'),assert=require('node:assert/strict');
const {resolve,scheduleKeys}=require('../participation');
function fixture(){const now='2026-10-24T16:00:00Z',key='75|2026-10-24|20_100';
 const f={'entry-key':'75|2026-10-24|200|30','show-id':75,date:'2026-10-24','entry-id':200,'class-id':30,'horse-id':600,'rider-id':700,status:'active','schedule-key':key,observed:now,observation:'successful'};
 const s={fields:{'stage-key':key,'show-id':75,date:f.date,'class-id':30,status:'active',ring:3}};
 return {now,entry:f,previous:{entry:['old-local']},binder:[{id:'local-entry',fields:{...f}}],localSchedules:[structuredClone(s)],sourceSchedules:[structuredClone(s)],health:[{fields:{'show-id':75,date:f.date,started:now,completed:now,status:'complete'}}],arrayFieldPresent:false,observationField:'observed',observationStatusField:'observation'};}
test('legacy singleton and explicit multi-group arrays validate exact local identities',()=>{
 const a=fixture();assert.equal(resolve(a).fields['participation-link-status'],'current');
 const f=a.binder[0].fields;f['schedule-key']='';f['schedule-keys']=JSON.stringify(['75|2026-10-24|20_100','75|2026-10-24|21_100']);a.arrayFieldPresent=true;
 for(const list of [a.localSchedules,a.sourceSchedules])list.push({fields:{...list[0].fields,'stage-key':'75|2026-10-24|21_100'}});
 assert.deepEqual(resolve(a).fields.entry,['local-entry']);
});
test('array compatibility refuses blank, malformed, duplicate, unsorted and conflicting values',()=>{
 for(const value of ['', '{}','[]','["b","a"]','["a","a"]'])assert.throws(()=>scheduleKeys({'schedule-keys':value,'schedule-key':'a'},true));
 assert.throws(()=>scheduleKeys({'schedule-keys':'["a"]','schedule-key':'b'},true));
 assert.throws(()=>scheduleKeys({'schedule-keys':'["a","b"]','schedule-key':'a'},true));
});
test('bad identity/status/freshness/schedules retain historical local link',()=>{
 const cases=[a=>a.binder[0].fields['horse-id']=999,a=>delete a.binder[0].fields['rider-id'],a=>a.binder[0].fields.status='dropped',a=>a.binder[0].fields.observed='2026-10-24T15:00:00Z',a=>a.binder[0].fields.observation='partial',a=>a.health[0].fields.status='failed',a=>a.health[0].fields.completed='2026-10-24T15:00:00Z',a=>a.localSchedules[0].fields.status='dropped',a=>a.sourceSchedules[0].fields.ring=4,a=>a.localSchedules.push(structuredClone(a.localSchedules[0])),a=>a.binder.push(structuredClone(a.binder[0])),a=>a.binder=null,a=>a.classUnresolved=true];
 for(const mutate of cases){const a=fixture();mutate(a);const r=resolve(a);assert.equal(r.fields['participation-link-status'],'historical');assert.deepEqual(r.fields.entry,['old-local']);assert(r.issue);assert.equal(r.fields['participation-verified-at'],undefined);}
});
test('untracked entry is unmatched, but missing formerly linked entry is historical',()=>{
 const a=fixture();a.binder=[];assert.equal(resolve(a).fields['participation-link-status'],'historical');a.previous={};assert.equal(resolve(a).fields['participation-link-status'],'unmatched');
});
test('source complete empty schedule does not validate missing schedule keys',()=>{const a=fixture();a.sourceSchedules=[];a.localSchedules=[];assert.equal(resolve(a).fields['participation-link-status'],'historical');});
test('full dated active key parity rejects an unlisted missing or extra sync row',()=>{
 for(const side of ['sourceSchedules','localSchedules']){
  const a=fixture();a[side].push({fields:{...a[side][0].fields,'stage-key':'75|2026-10-24|21_100'}});
  const r=resolve(a);assert.equal(r.fields['participation-link-status'],'historical');assert.deepEqual(r.fields.entry,['old-local']);
 }
 const a=fixture();a.sourceSchedules.push({fields:{...a.sourceSchedules[0].fields,'stage-key':'75|2026-10-24|21_100',status:'dropped'}});
 assert.equal(resolve(a).fields['participation-link-status'],'current');
});
