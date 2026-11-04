import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {Miniflare} from 'miniflare';
test('horse reader invokes native Workers fetch without a receiver',async()=>{
 const source=readFileSync(new URL('../src/lib/rs-profile-horses.js',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'').replace(/^export /gm,'');
 const script=`const BASE='fixture',TABLE={barn:'team'};class InputError extends Error{};${source}
 export default {async fetch(){return Response.json(await loadProfileHorses({person:{id:'person'},memberships:[{fields:{Person:['person'],Tenant:['team'],Status:'Enabled'}}],call:async()=>({id:'team',fields:{tenant_id:39138,tenant_active:true}}),callFetch:fetch,env:{AIRTABLE_TOKEN:'fixture'}}));}};`;
 const mf=new Miniflare({modules:true,script,compatibilityDate:'2026-06-01',outboundService:()=>Response.json({records:[{id:'horse',fields:{'horse-id':123,'horse-name':'Fixture','w-trainers':'39138'}}]})});
 try{const response=await mf.dispatchFetch('http://fixture.test');assert.equal(response.status,200);const result=await response.json();assert.equal(result.horses[0].name,'Fixture');assert.equal(result.horseTeamId,'team');}finally{await mf.dispose();}
});
