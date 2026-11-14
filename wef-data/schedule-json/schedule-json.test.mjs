import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {collectDay0} from './day0.mjs';
import {collectDay1} from './day1.mjs';
import {collectDay2} from './day2.mjs';
import {collectDay3} from './day3.mjs';
import {collectDay4} from './day4.mjs';

const sf = {'show-key':'fldbB6tZgkcR3Eaia', customer:'fldSOs5Y4LwVO0Bgh', show:'fld3HHtWV71deVDKX'};
function mock(day, payload, status = 200, available = true) {
  const calls = [];
  return {calls, fetchImpl: async (url, options) => {
    url = new URL(url); calls.push({url, options});
    if (url.pathname.endsWith('tblGc03icHzudV1v2')) return Response.json({records:[{fields:{[sf['show-key']]:'15|200000078',[sf.customer]:15,[sf.show]:200000078}}]});
    if (url.pathname.endsWith('tblpRrsNXQdr3KrC9')) {
      assert.equal(url.searchParams.get('filterByFormula'), 'AND({fldVSmOAJBBPeHAL1}="15|200000078",{fldFyIIUliqgcl2AT}='+day+')');
      return Response.json({records:available ? [{fields:{fldYIy1hTW3aLXOeo:'2026-11-'+(13+day)}}] : []});
    }
    assert.equal(url.hostname, 'wef-schedules-decoded.gombcg.workers.dev');
    assert.equal(url.pathname, '/schedule');
    assert.equal(url.searchParams.get('date'), '2026-11-'+(13+day));
    assert.equal(url.searchParams.get('show_id'), '200000078');
    assert.equal(options.headers['User-Agent'], 'Mozilla/5.0');
    assert.equal(options.headers.Authorization, undefined);
    return Response.json(payload, {status});
  }};
}
for (const [day, collect] of [[0,collectDay0],[1,collectDay1],[2,collectDay2],[3,collectDay3],[4,collectDay4]]) {
  test('Day '+day+' independently saves full JSON, overwrites only on success, and never fetches classes', async t => {
    const output = await fs.mkdtemp(path.join(os.tmpdir(), 'schedule-json-'));
    t.after(() => fs.rm(output, {recursive:true, force:true}));
    const payload = {show:{show_id:200000078},show_date:'2026-11-'+(13+day),rings:[{ring_number:2,classes:[{class_id:123,unmapped:'retained'}]}]};
    const source = mock(day,payload);
    const first = await collect({output,token:'offline-test',fetchImpl:source.fetchImpl});
    assert.equal(first.state,'Saved',first.error); assert.equal(source.calls.length,3);
    assert.deepEqual(JSON.parse(await fs.readFile(first.file,'utf8')),payload);
    const updated = {...payload,extra:'latest'};
    assert.equal((await collect({output,token:'offline-test',fetchImpl:mock(day,updated).fetchImpl})).ok,true);
    const failed = mock(day,{message:'failure'},502);
    assert.equal((await collect({output,token:'offline-test',fetchImpl:failed.fetchImpl})).state,'Failed');
    assert.equal(failed.calls.length,3);
    assert.deepEqual(JSON.parse(await fs.readFile(first.file,'utf8')),updated);
  });
  test('Day '+day+' accepts legitimate zero rings and skips unavailable dates without a source request', async t => {
    const output = await fs.mkdtemp(path.join(os.tmpdir(), 'schedule-json-'));
    t.after(() => fs.rm(output, {recursive:true, force:true}));
    const payload = {show:{show_id:200000078},show_date:'2026-11-'+(13+day),rings:[]};
    assert.equal((await collect({output,token:'offline-test',fetchImpl:mock(day,payload).fetchImpl})).rings,0);
    const unavailable = mock(day,null,200,false);
    assert.equal((await collect({output,token:'offline-test',fetchImpl:unavailable.fetchImpl})).state,'Skipped');
    assert.equal(unavailable.calls.length,2);
    const wrong = {...payload,show_date:'2026-11-30'};
    assert.equal((await collect({output,token:'offline-test',fetchImpl:mock(day,wrong).fetchImpl})).state,'Failed');
  });
}
