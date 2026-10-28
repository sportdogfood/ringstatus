"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),{plan,fields}=require('../tenant-slices');
const now='2026-10-24T15:30:00.000Z';
const record=(id,f)=>({id,fields:f});
const entry=(id,extra={})=>record(id,{'entry-key':id,'horse-id':1,'rider-id':2,status:'active','trainer-ids':'8778',...extra});
function input(extra={}){return{local:[entry('team')],selected:[{'entry-key':'team'}],horses:[record('h',{'horse-id':1})],riders:[record('r',{'rider-id':2})],now,...extra};}
test('active slice hides both owner-disabled and source-inactive horses and riders without mutating collected rows',()=>{
 for(const table of ['horses','riders'])for(const flag of ['disabled','source-inactive']){const data=input();data[table][0].fields[flag]=true;const before=structuredClone(data),out=plan(data);assert.equal(out[0].fields['active-team'],false);assert.match(out[0].fields['team-status'],/inactive/);assert.deepEqual(data,before);assert.deepEqual(Object.keys(out[0].fields),fields);}
});
test('reactivation restores visibility and repeat preserves identity',()=>{const data=input();data.horses[0].fields.disabled=true;assert.equal(plan(data)[0].fields['active-team'],false);data.horses[0].fields.disabled=false;const a=plan(data),b=plan(data);assert.deepEqual(a,b);assert.equal(a[0].id,'team');assert.equal(a[0].fields['active-team'],true);});
test('dropped, previous-show and personal-only rows never become team; overlap publishes no follow ownership',()=>{
 const local=[entry('team',{'owner-key':'main-user'}),entry('personal',{'trainer-ids':''}),entry('dropped',{status:'dropped'}),entry('previous')];
 const out=plan(input({local,selected:[{'entry-key':'team'},{'entry-key':'personal'},{'entry-key':'dropped'}]}));assert.deepEqual(out.map(r=>r.fields['active-team']),[true,false,false,false]);assert.equal(JSON.stringify(out).includes('main-user'),false);assert.equal(JSON.stringify(out).includes('owner-key'),false);
});
test('valid empty slice succeeds; ambiguous and missing helpers fail before any writes',()=>{assert.deepEqual(plan(input({local:[],selected:[]})),[]);assert.throws(()=>plan(input({horses:[]})),/helper missing/);assert.throws(()=>plan(input({riders:[record('a',{'rider-id':2}),record('b',{'rider-id':2})]})),/ambiguous/);});
