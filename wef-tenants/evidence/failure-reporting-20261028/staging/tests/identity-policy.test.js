"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {assertUniqueRoster,assertProspectiveRoster}=require('../identity-policy');
const horse=(key,id,name,nick)=>({'horse-key':key,'horse-id':id,name,'horse-name':name,'show-name':name,'nick-name':nick});
const rider=(key,id,name,nick)=>({'rider-key':key,'rider-id':id,name,'rider-name':name,'nick-name':nick,'first-name':'Victoria','last-name':name.split(' ').at(-1)});
test('policy enforces each named horse and rider column, ignoring case/outer and repeated whitespace',()=>{
 for(const table of ['horses','riders']){
  const make=table==='horses'?horse:rider;
  const fields=table==='horses'?['nick-name','show-name','name','horse-id','horse-key','horse-name']:['name','nick-name','rider-key','rider-name','rider-id'];
  for(const field of fields){const a=make('one',1,'Alpha Beta','A'),b=make('two',2,'Other Name','B');b[field]=field.endsWith('-id')?a[field]:field.endsWith('-key')?a[field]:'  '+String(a[field]).toUpperCase().replace(' ','   ')+'  ';assert.throws(()=>assertUniqueRoster(table,[a,b]),new RegExp(field));}
 }
});
test('separate columns and separate tables may share a value',()=>{assert.doesNotThrow(()=>assertUniqueRoster('horses',[horse('1',1,'Alpha','Beta'),horse('2',2,'Beta','Gamma')]));assert.doesNotThrow(()=>assertUniqueRoster('riders',[rider('1',1,'Alpha','Beta')]));});
test('unknown optional IDs/nicknames stay blank; keys are required',()=>{assert.doesNotThrow(()=>assertUniqueRoster('horses',[horse('a',null,'Alpha',null),horse('b',null,'Beta','')]));assert.doesNotThrow(()=>assertUniqueRoster('riders',[rider('a',1,'Alpha',null),rider('b',2,'Beta',null)]));assert.throws(()=>assertUniqueRoster('horses',[horse('',1,'Alpha','A')]),/horse-key/);});
test('rider first and last names may repeat while full names remain unique',()=>{assert.doesNotThrow(()=>assertUniqueRoster('riders',[rider('1',1,'Victoria Rotsaert'),rider('2',2,'Victoria Sheehan')]));});
test('prospective partial writes are checked against retained records before mutation',()=>{const rows=[{id:'a',fields:horse('1',1,'Alpha','A')},{id:'b',fields:horse('2',2,'Beta','B')}];assert.throws(()=>assertProspectiveRoster('horses','horse-key',rows,[{'horse-key':'2','nick-name':'a'}]),/nick-name/);assert.doesNotThrow(()=>assertProspectiveRoster('horses','horse-key',rows,[{'horse-key':'2',notes:'Owner note'}]));});
test('numeric IDs cannot be zero, negative or non-integer; opaque keys retain exact case',()=>{for(const id of [0,-1,1.5,'bad'])assert.throws(()=>assertUniqueRoster('horses',[horse('one',id,'Alpha','A')]),/horse-id/);assert.doesNotThrow(()=>assertUniqueRoster('horses',[horse('legacy:recABC',null,'Alpha','A'),horse('legacy:recabc',null,'Beta','B')]));});
