import test from 'node:test';
import assert from 'node:assert/strict';
import {estimateWalk,withinRing} from '../test-support/location-walk-estimate.mjs';
test('labelled synthetic Ring 5: 0.001 degrees at equator is about 111.195m and 1.324min at assumed 1.4m/s',()=>{
 const result=estimateWalk({latitude:0,longitude:0,source:'gps'},{latitude:0,longitude:0.001});
 assert.ok(Math.abs(result.metres-111.19508)<0.001);assert.ok(Math.abs(result.minutes-1.323751)<0.001);
});
test('zero distance, antimeridian and reverse path',()=>{
 const a={latitude:0,longitude:179.999,source:'gps'},b={latitude:0,longitude:-179.999,source:'gps'};
 assert.equal(estimateWalk(a,a).minutes,0);assert.ok(Math.abs(estimateWalk(a,b).metres-222.39016)<0.001);assert.ok(Math.abs(estimateWalk(a,b).metres-estimateWalk(b,a).metres)<0.001);
});
test('GeoIP does not imply a ring-level walking ETA; invalid points and speed rejected',()=>{
 assert.equal(estimateWalk({latitude:1,longitude:2,source:'geoip'},{latitude:1,longitude:2.001}).minutes,null);
 assert.throws(()=>estimateWalk({latitude:91,longitude:0},{latitude:0,longitude:0}),RangeError);
 assert.throws(()=>estimateWalk({latitude:0,longitude:0},{latitude:0,longitude:0},0),RangeError);
});

test('conservative test-only 100ft boundary: inside, outside, uncertain, stale and GeoIP',()=>{
 const now=Date.parse('2026-11-13T12:00:00Z'),target={latitude:0,longitude:0.0001},device={latitude:0,longitude:0,source:'gps',accuracy:5,observedAt:new Date(now).toISOString()},policy={radiusFeet:100,maxAgeMs:60000,now};
 assert.equal(withinRing(device,target,policy).qualified,true);
 assert.equal(withinRing(device,target,{...policy,radiusFeet:30}).qualified,false);
 assert.equal(withinRing({...device,accuracy:100},target,policy).qualified,false);
 assert.equal(withinRing({...device,source:'geoip'},target,policy).qualified,false);
 assert.equal(withinRing({...device,observedAt:new Date(now-60001).toISOString()},target,policy).qualified,false);
});

// Owner-supplied coordinates, verified in Profiles/w-rings on 2026-11-13.
// Device fixes below are labelled test fixtures, not actual user observations.
test('saved WEF rings: Ring 6 to Rost distance and conservative GPS qualification',()=>{
 const ring6={latitude:26.634806,longitude:-80.274528},rost={latitude:26.635028,longitude:-80.273806};
 const now=Date.parse('2026-11-13T17:00:00Z'),device={...ring6,source:'gps',accuracy:5,observedAt:new Date(now).toISOString()},policy={radiusFeet:100,maxAgeMs:60000,now};
 const walk=estimateWalk(device,rost);
 assert.ok(walk.metres>75&&walk.metres<77);assert.ok(walk.minutes>0.89&&walk.minutes<0.92);
 assert.equal(withinRing(device,ring6,policy).qualified,true);
 assert.equal(withinRing(device,rost,policy).qualified,false);
 assert.equal(withinRing({...device,source:'geoip'},ring6,policy).qualified,false);
 assert.equal(withinRing({...device,accuracy:31},ring6,policy).qualified,false);
 assert.equal(withinRing({...device,observedAt:new Date(now-60001).toISOString()},ring6,policy).qualified,false);
});
