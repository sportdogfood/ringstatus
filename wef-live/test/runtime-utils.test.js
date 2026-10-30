"use strict";
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const {execFileSync} = require('node:child_process');
const {same, unique, acquireLock} = require('../runtime-utils');
const {archiveFile} = require('../archive');

test('Live modules load with tenant imports forbidden', () => {
  execFileSync(process.execPath, ['-e', `
    const Module=require('node:module'), original=Module._load;
    Module._load=function(name,...args){
      if(name.includes('wef-tenants'))throw Error('Tenant dependency: '+name);
      return original.call(this,name,...args);
    };
    for(const name of ['entry','collector','airtable','verify','archive','runtime-utils'])
      require('./'+name);
  `], {cwd:path.resolve(__dirname,'..')});
});

test('local helpers preserve blank comparison, uniqueness and overlap protection', () => {
  assert.equal(same(null,''),true);
  assert.equal(same('',false),true);
  assert.equal(same(null,[]),true);
  assert.equal(same(null,0),false);
  assert.equal(same([1],[2]),false);
  assert.throws(()=>unique([{fields:{key:'x'}},{fields:{key:'x'}}],'key'),/duplicate/);
  assert.throws(()=>unique([{fields:{}}],'key'),/Missing/);
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'live-lock-test-'));
  try {
    const file=path.join(root,'collector.lock');
    const release=acquireLock(file);
    assert.throws(()=>acquireLock(file),/already running/);
    release();
    assert.equal(fs.existsSync(file),false);
    fs.writeFileSync(file,'invalid');
    assert.throws(()=>acquireLock(file),/Invalid live collector lock/);
  } finally {fs.rmSync(root,{recursive:true,force:true});}
});

test('local archives reuse verified content and reject corruption', async () => {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'live-archive-test-'));
  try {
    const rows=[{id:'existing',fields:{entry:['linked-record']}}];
    const first=await archiveFile(root,'prior',rows);
    const again=await archiveFile(root,'prior',rows);
    assert.equal(first.outcome,'Created');
    assert.equal(again.outcome,'Reused');
    assert.equal(first.sha256,again.sha256);
    fs.writeFileSync(first.file,'corrupt');
    await assert.rejects(archiveFile(root,'prior',rows));
  } finally {fs.rmSync(root,{recursive:true,force:true});}
});
