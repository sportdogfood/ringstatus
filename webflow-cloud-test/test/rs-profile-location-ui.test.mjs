import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const app=readFileSync(new URL('../public/rs-profile-v3/app.js',import.meta.url),'utf8');
const block=app.slice(app.indexOf('// Location uses'),app.indexOf("document.addEventListener('click',event=>{",app.indexOf('// Location uses')));
function fixture(){
 const input={checked:true,disabled:false,matches:()=>true};let change,success,failure,saves=0;const logs=[],notices=[];
 const ctx={state:{currentUserId:'a',recognized:true},ui:{},document:{querySelector:()=>input,addEventListener:(_,fn)=>{change=fn;}},navigator:{geolocation:{getCurrentPosition:(ok,no)=>{success=ok;failure=no;}}},service:{updateProfileLocation:p=>logs.push(p)},saveUI:()=>saves++,notice:m=>notices.push(m),Date};
 vm.createContext(ctx);vm.runInContext(block,ctx);
 return {ctx,input,logs,notices,change:()=>change({target:input}),success:()=>success({coords:{latitude:26,longitude:-80,accuracy:5},timestamp:Date.now()}),fail:code=>failure({code}),saves:()=>saves};
}
test('choice persists and control remains usable before any GPS or save response',()=>{const f=fixture();f.change();assert.equal(f.ctx.ui.locationAllowed.a,true);assert.equal(f.saves(),1);assert.equal(f.input.checked,true);assert.equal(f.input.disabled,false);assert.equal(f.logs.length,0);f.success();assert.equal(f.logs[0].accuracy,5);});
test('timeout falls back quietly without undoing opted-in choice',()=>{const f=fixture();f.change();f.fail(3);assert.equal(f.ctx.ui.locationAllowed.a,true);assert.equal(f.input.checked,true);assert.deepEqual(f.logs,[null]);assert.deepEqual(f.notices,[]);});
test('denial does not claim permission was granted',()=>{const f=fixture();f.change();f.fail(1);assert.equal(f.input.checked,false);assert.equal(f.ctx.ui.locationAllowed.a,false);assert.deepEqual(f.logs,[null]);});
test('turning off while GPS waits prevents late GPS submission',()=>{const f=fixture();f.change();f.input.checked=false;f.change();f.success();assert.deepEqual(f.logs,[null]);assert.equal(f.ctx.ui.locationAllowed.a,false);});
test('account switch prevents late location submission',()=>{const f=fixture();f.change();f.ctx.state.currentUserId='b';f.success();assert.equal(f.logs.length,0);});
