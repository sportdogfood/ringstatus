'use strict';
const assert=require('node:assert/strict');
const {alertFor}=require('./candidate/cleanup-lane');
let n=0;function test(name,fn){fn();n++;console.log('PASS '+name);}
const state={show:77,last_log_check:'2026-11-07T21:00:00Z',calendar_day:'2026-11-07',event_id:'calendar-day|2026-11-07'};
test('before19 Eastern no daily clear',()=>assert.equal(alertFor(state,77,'2026-11-07T23:59:59Z'),null));
test('19 Eastern daily clear includes existing runs and has distinct event id',()=>{
 const a=alertFor(state,77,'2026-11-08T00:00:00Z');assert.equal(a.trigger,'calendar-day');assert.equal(a.event_id,'daily-19|2026-11-07');assert.equal(a.calendar_day,'2026-11-07');
});
test('summer19 Eastern uses daylight time',()=>assert.equal(alertFor({...state,last_log_check:'2026-07-01T22:00:00Z'},77,'2026-07-01T23:00:00Z').event_id,'daily-19|2026-07-01'));
test('same date daily clear not repeated',()=>assert.equal(alertFor({...state,last_log_check:'2026-11-08T00:00:00Z',daily_clear_date:'2026-11-07'},77,'2026-11-08T01:00:00Z'),null));
test('midnight does not trigger daily clear',()=>assert.equal(alertFor({...state,last_log_check:'2026-11-08T04:30:00Z',daily_clear_date:'2026-11-07'},77,'2026-11-08T05:00:00Z'),null));
test('3h threshold before19 is still log-check',()=>assert.equal(alertFor({...state,last_log_check:'2026-11-07T18:00:00Z'},77,'2026-11-07T21:00:00Z').trigger,'log-check'));
test('same scheduled slot is not due',()=>assert.equal(alertFor({...state,last_log_check:'2026-11-07T18:00:00Z'},77,'2026-11-07T20:59:59Z'),null));
test('newshow clears owned records before19',()=>assert.equal(alertFor(state,78,'2026-11-07T23:00:00Z').trigger,'new-show'));
test('newshow takes priority at19',()=>assert.equal(alertFor(state,78,'2026-11-08T00:00:00Z').trigger,'new-show'));
test('no mutation of supplied saved state',()=>{const s=JSON.stringify(state);alertFor(state,77,'2026-11-08T00:00:00Z');assert.equal(JSON.stringify(state),s);});
test('delivery jitter does not skip next3h slot',()=>assert.equal(alertFor({...state,last_log_check:'2026-11-08T00:00:05Z',daily_clear_date:'2026-11-07'},77,'2026-11-08T03:00:00Z').trigger,'log-check'));
console.log(n+' policy checks passed; no network or production execution');
