import test from 'node:test';
import assert from 'node:assert/strict';
import {diffRecords} from '../public/wef-schedule-v2/data-diff.js';
test('unchanged records retain identity; key ordering is not a change',()=>{
 const old={id:'a',name:'Horse',details:{x:1,y:2}};
 const diff=diffRecords([old],[{details:{y:2,x:1},name:'Horse',id:'a'}]);
 assert.equal(diff.changed,false);assert.equal(diff.records[0],old);
});
test('adds, edits, removals and source reordering are separate changes',()=>{
 const old=[{id:'a',n:1},{id:'b',n:2},{id:'c',n:3}];
 const diff=diffRecords(old,[{id:'b',n:4},{id:'a',n:1},{id:'d',n:5}]);
 assert.deepEqual(diff.added,['d']);assert.deepEqual(diff.updated,['b']);assert.deepEqual(diff.removed,['c']);assert.equal(diff.records[1],old[0]);assert.equal(diff.reordered,true);
});
test('duplicate or missing identities reject a package',()=>{
 assert.throws(()=>diffRecords([],[{id:'a'},{id:'a'}]),/invalid_record_identity/);
 assert.throws(()=>diffRecords([],[{}]),/invalid_record_identity/);
});
