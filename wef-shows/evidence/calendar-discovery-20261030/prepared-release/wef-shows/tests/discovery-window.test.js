const test=require('node:test'),assert=require('node:assert/strict');
const p=require('../discovery-policy');
test('Sunday 22 Eastern including DST, three-hour durable retry, stop and next cycle',()=>{
 let state=p.discoveryPlan('2026-10-30T22:00:00Z',null,76).state;
 assert.equal(state.next,'2026-11-02T03:00:00.000Z');
 assert.equal(p.discoveryPlan('2026-11-02T02:59:59Z',state,76).due,false);
 let plan=p.discoveryPlan('2026-11-02T03:00:00Z',state,76);assert.equal(plan.due,true);
 state={...plan.state,next:'2026-11-02T06:04:00Z',failed:true};
 assert.equal(p.discoveryPlan('2026-11-02T06:03:59Z',state,76).due,false);
 assert.equal(p.discoveryPlan('2026-11-02T06:04:00Z',state,76).due,true);
 assert.equal(p.discoveryPlan('2026-11-09T04:00:00Z',state,76).state.baseline,76);
 state={...state,complete:true,cycle:p.sunday('2026-11-02T06:04:00Z'),next:p.nextSunday('2026-11-02T06:04:00Z')};
 assert.equal(p.discoveryPlan('2026-11-08T20:00:00Z',state,77).due,false);
 assert.equal(p.discoveryPlan('2026-11-09T03:00:00Z',state,77).state.baseline,77);
 assert.equal(p.nextSunday('2027-03-13T20:00:00Z'),'2027-03-15T02:00:00.000Z');
 assert.throws(()=>p.discoveryPlan('bad',null,76),/clock/);
});
test('7 p.m. calendar selection uses tomorrow within inclusive show range, never repeats or rolls backward',()=>{
 const f={start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-30'};
 assert.equal(p.calendarSelection(f,'2026-10-30T22:59:59Z'),'2026-10-30');
 assert.equal(p.calendarSelection(f,'2026-10-30T23:00:00Z'),'2026-10-31');
 assert.equal(p.calendarSelection({...f,selected_date:'2026-10-31'},'2026-10-31T03:55:00Z'),'2026-10-31');
 assert.equal(p.calendarSelection({...f,selected_date:'2026-11-01'},'2026-11-02T00:00:00Z'),'2026-11-01');
 assert.equal(p.calendarSelection({...f,start_date:'2026-11-05',end_date:'2026-11-08',selected_date:'2026-11-05'},'2026-10-30T23:00:00Z'),'2026-11-05');
});
