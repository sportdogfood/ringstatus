"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {preserve}=require('../participation');
test('preservation never creates links or renews historical qualification',()=>{
 for(const status of [undefined,'historical','unmatched','unresolved'])assert.deepEqual(preserve({'participation-link-status':status}),{});
 const previous={entry:['local'],'participation-link-status':'current','participation-verified-at':'original'};
 const copy=structuredClone(previous),fields=preserve(previous);assert.deepEqual(previous,copy);
 assert.equal(fields['participation-link-status'],'historical');assert.equal(fields.entry,undefined);assert.equal(fields['participation-verified-at'],undefined);
});
