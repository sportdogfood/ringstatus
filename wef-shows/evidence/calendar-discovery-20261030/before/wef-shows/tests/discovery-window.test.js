const test=require('node:test'),assert=require('node:assert/strict');
const {discoveryWindow}=require('../discovery-window');

test('Eastern discovery windows include 16:00 and exclude 20:00; new shows only Sunday',()=>{
 for(const [at,show,day] of [
  ['2026-10-30T19:59:59Z',false,false],['2026-10-30T20:00:00Z',false,true],
  ['2026-10-30T23:59:59Z',false,true],['2026-10-31T00:00:00Z',false,false],
  ['2026-10-25T19:59:59Z',false,false],['2026-10-25T20:00:00Z',true,true],
  ['2026-10-26T00:00:00Z',false,false],
  // DST ends November 1; window moves by one UTC hour, not one local hour.
  ['2026-11-01T20:59:59Z',false,false],['2026-11-01T21:00:00Z',true,true],
  ['2026-11-02T00:59:59Z',true,true],['2026-11-02T01:00:00Z',false,false],
  // Spring DST also uses Eastern civil time.
  ['2027-03-14T20:00:00Z',true,true],['2027-03-15T00:00:00Z',false,false]
 ]){
  const w=discoveryWindow(at);assert.equal(w.show,show,at);assert.equal(w.showDay,day,at);
 }
 assert.throws(()=>discoveryWindow('invalid'),/clock/);
});
