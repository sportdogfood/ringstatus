import test from 'node:test';
import assert from 'node:assert/strict';
import {recognizeTenantsFixture} from '../test-support/recognize-tenants-fixture.mjs';
test('valid recognition from several browsers does not consume failed-guess allowance',async()=>{
 const f=await recognizeTenantsFixture();try{
  for(let n=0;n<6;n++){
   const r=await f.dispatch(new Request('https://ringstatus.com/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:'https://ringstatus.com','Content-Type':'application/json'},body:JSON.stringify({action:'credential',kind:'phone',value:'6318752160',request_id:crypto.randomUUID()})}));
   assert.equal((await r.json()).verified,true);
  }
  assert.equal(f.rows.tblfkRSJAEMzuzApR.length,6);assert.equal(f.code,null);
 }finally{f.db.close();}
});
