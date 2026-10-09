import test from 'node:test';
import assert from 'node:assert/strict';
import {readNativePreferences,PAGE_ID} from '../src/assets/rs-input-subscriptions/native-reader.js';
import {subscriptionAlertDefinitions as definitions} from '../src/lib/rs-input-subscription-alerts.js';

test('native reader retains parameter values and never infers consent or timezone',()=>{
 const controls=new Map(),node=value=>Object.freeze(value);
 controls.set('[data-rs-sms]',[node({checked:true})]);
 controls.set('[data-rs-field="phone"]',[node({value:'+12025550148'})]);
 for(const def of definitions){
  controls.set('[data-rs-alert="'+def.key+'"]',[node({checked:true})]);
  if(def.input==='time')controls.set('[data-rs-alert-time="'+def.key+'"]',[node({value:'10:15'})]);
  else if(def.presets)controls.set('[data-rs-action="alert-value"][data-key="'+def.key+'"][aria-pressed="true"]',[node({getAttribute:()=>String(def.presets[0])})]);
 }
 const form=Object.freeze({querySelectorAll:selector=>controls.get(selector)||[]});
 const root=Object.freeze({querySelector:selector=>selector==='[data-rs-form="alerts"]'?form:null});
 const document=Object.freeze({documentElement:Object.freeze({getAttribute:()=>PAGE_ID}),querySelector:selector=>selector==='#rs-component-drafts'?root:null});
 const read=readNativePreferences(document,definitions);
 assert.equal(Object.keys(read.variables).length,18);assert.equal(read.decision,null);assert.equal(read.timeZone,null);
 for(const def of definitions.filter(d=>d.input))assert.equal(read.variables[def.key].value,def.input==='time'?'10:15':String(def.presets[0]));
 assert.throws(()=>readNativePreferences({...document,documentElement:{getAttribute:()=> 'other_page'}},definitions),/wrong_native_page/);
 controls.set('[data-rs-sms]',[]);assert.throws(()=>readNativePreferences(document,definitions),/native_control_missing_or_ambiguous/);
});
