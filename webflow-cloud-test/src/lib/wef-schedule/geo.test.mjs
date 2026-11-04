import test from 'node:test';
import assert from 'node:assert/strict';
import {approximateLocation} from './geo.mjs';
test('GeoIP returns only approximate valid coordinates, including zero',()=>{
 assert.deepEqual(approximateLocation({cf:{latitude:'0',longitude:'-80.25',city:'not returned',ip:'not returned'}}),{available:true,source:'geoip',approximate:true,latitude:0,longitude:-80.25});
});
test('missing or invalid GeoIP is unavailable, never a fabricated zero location',()=>{
 for(const cf of [undefined,{}, {latitude:'',longitude:'0'},{latitude:'91',longitude:'0'},{latitude:'1',longitude:'181'},{latitude:'NaN',longitude:'0'}]){
  const result=approximateLocation({cf});assert.equal(result.available,false);assert.equal(result.latitude,null);assert.equal(result.longitude,null);
 }
});
