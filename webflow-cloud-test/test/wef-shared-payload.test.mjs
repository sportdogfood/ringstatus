import test from 'node:test';
import assert from 'node:assert/strict';
import { encodePayload, decodePayload } from '../src/lib/wef-schedule/shared-payload.mjs';

test('legacy JSON remains unchanged',()=>{
  const value={metadata:{defaultDate:'2026-11-15'},records:[{name:'Étoile 🐴',order:null}]};
  assert.equal(encodePayload(value),JSON.stringify(value));
  assert.deepEqual(decodePayload(JSON.stringify(value)),value);
});

test('large JSON round-trips every field including Unicode and nulls',()=>{
  const value={metadata:{defaultDate:'2026-11-15'},records:Array.from({length:1200},(_,i)=>({key:String(i),name:'Étoile 🐴',entries:[{key:'trip-'+i,order:null,trainer:'Trainer'}]}))};
  assert.ok(JSON.stringify(value).length>100000);
  const stored=encodePayload(value);
  assert.ok(stored.length<100000);
  assert.deepEqual(decodePayload(stored),value);
  assert.equal(encodePayload(value),stored);
});

test('corrupt, truncated and unknown encodings are rejected',()=>{
  for(const value of [{encoding:'unknown',data:'AAAA'},{encoding:'gzip-base64-v1',data:'bad!'}, {encoding:'gzip-base64-v1',data:'AAAA'}]){
    assert.throws(()=>decodePayload(JSON.stringify(value)));
  }
  assert.throws(()=>decodePayload('{'));
});
