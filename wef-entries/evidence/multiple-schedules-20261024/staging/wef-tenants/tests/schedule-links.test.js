const test=require('node:test'),assert=require('node:assert/strict');
const link=require('../schedule-links'),consumer=require('../entries-consumer');
const key='75|2026-10-23|10|123',a='75|2026-10-23|1_123',b='75|2026-10-23|2_123';
const row={'entry-key':key,'show-id':75,date:'2026-10-23','class-id':123,'horse-id':20,'rider-id':30,'trainer-ids':'8778',status:'active','schedule-key':'','schedule-keys':JSON.stringify([a,b]),'observation-status':'successful'};
const schedules=[a,b].map((k,i)=>({id:'local'+i,fields:{'stage-key':k,'show-id':75,date:row.date,'class-id':123,'group-id':i+1,ring:i+1,status:'active'}}));
test('strict transport accepts legacy and singleton; rejects blank malformed unordered duplicate or conflicting arrays',()=>{
 assert.deepEqual(link.keys({'schedule-key':a}),[a]);assert.deepEqual(link.keys({'schedule-key':a,'schedule-keys':JSON.stringify([a])}),[a]);
 for(const value of ['',null,'bad','[]',JSON.stringify([b,a]),JSON.stringify([a,a]),JSON.stringify([''])])assert.throws(()=>link.keys({'schedule-key':a,'schedule-keys':value}));
 assert.throws(()=>link.keys({...row,'schedule-key':a}));assert.throws(()=>link.keys({'schedule-key':b,'schedule-keys':JSON.stringify([a])}));
});
test('multiple links require exact participation group evidence; no first or all groups inferred',()=>{
 assert.throws(()=>link.eligible(schedules),/eligibility unresolved/);assert.deepEqual(link.eligible(schedules,[1,2]).map(r=>r.id),['local0','local1']);
 assert.throws(()=>link.eligible(schedules,[3]));assert.throws(()=>link.eligible([...schedules,schedules[0]],[1]));
 assert.throws(()=>link.eligible([{...schedules[0],fields:{...schedules[0].fields,status:'dropped'}}],[1]));
});
test('CWF plural links use local schedule and distinct ring IDs and preserve helpers on repeat',()=>{
 const refs={showRecord:{id:'showLocal'},schedules,rings:[1,2].map(n=>({id:'ringLocal'+n,fields:{ring_date:row.date,ring_number:n}})),horses:[{id:'horseLocal',fields:{'horse-id':20,disabled:true,'nick-name':'keep'}}],riders:[{id:'riderLocal',fields:{'rider-id':30}}],trainers:[{id:'trainerLocal',fields:{'trainer-id':8778}}]};
 const before=structuredClone(refs);for(let i=0;i<2;i++){const result=consumer.links([row],refs)[0];assert.deepEqual(result.schedules,['local0','local1']);assert.deepEqual(result['synced-rings'],['ringLocal1','ringLocal2']);assert.deepEqual(result.horses,['horseLocal']);assert(!('owner-key' in result));}assert.deepEqual(refs,before);
 assert.throws(()=>consumer.links([{...row,'observation-status':'failed'}],refs),/historical/);
});
test('missing dropped duplicate and conflicting schedule references fail closed',()=>{
 assert.throws(()=>link.resolve(row,schedules.slice(0,1)));assert.throws(()=>link.resolve(row,[...schedules,schedules[0]]));
 for(const change of [{status:'dropped'},{date:'2026-10-24'},{'show-id':76},{'class-id':124}])assert.throws(()=>link.resolve(row,[{...schedules[0],fields:{...schedules[0].fields,...change}},schedules[1]]));
});
test('date health accepts identified empty completion and rejects stale partial or failed collections',()=>{
 const c={fields:{'show-id':75,date:row.date,status:'complete',completed:'2026-10-23T12:00:00Z','row-count':0}};
 link.health([c],{show_id:75},[row.date],'2026-10-23T12:05:00Z');
 for(const status of ['failed','partial'])assert.throws(()=>link.health([{fields:{...c.fields,status}}],{show_id:75},[row.date],'2026-10-23T12:05:00Z'));
 assert.throws(()=>link.health([c],{show_id:75},[row.date],'2026-10-23T12:31:00Z'));
});

test('plural transport blanks misleading scalar schedule and ring while keeping exact keys',()=>{const t=link.transport(link.eligible(schedules,[2,1]));assert.deepEqual(t,{'schedule-key':'','schedule-keys':JSON.stringify([a,b]),'ring-number':null});assert.deepEqual(link.transport([schedules[0]]),{'schedule-key':a,'schedule-keys':JSON.stringify([a]),'ring-number':1});});
