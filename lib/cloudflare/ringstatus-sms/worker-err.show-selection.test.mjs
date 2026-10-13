import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';

// Execute the real Worker offline. Only its module export is adapted for the VM;
// every network boundary is denied unless this test supplies a fixture response.
const source = readFileSync(new URL('./worker-err.js', import.meta.url), 'utf8');
function loadWorker(fetchFixture = () => { throw new Error('Network disabled in offline tests'); }) {
  const context = vm.createContext({
    Response, Request, URL, URLSearchParams, AbortController, setTimeout, clearTimeout,
    console: { log() {}, error() {}, warn() {} },
    fetch: fetchFixture,
    Date: class extends Date {
      constructor(...args) { super(...(args.length ? args : ['2026-10-10T16:00:00Z'])); }
      static now() { return 1791648000000; }
    },
  });
  vm.runInContext(source.replace('export default {', 'globalThis.worker = {'), context);
  return context;
}

const api = loadWorker();
const show = (show_id) => ({ show_id, live_data: {} });
const derbyShow = {
  show_id: 'wanted',
  live_data: { 1: { name: 'Derby', livenow: [{
    ring_number: 1, ring_id: 10, group_name: 'Open Hunters', class_numbers: '101',
    total: 12, gone: 4, curr_updated_at: 1791648000,
  }], upcoming: [], completed: [] } },
};

// Each case catches wrong-first-record fallback, duplicate acceptance, or missing
// identity validation. Expected selection is independent of the selector logic.
for (const [name, payload, env, expected] of [
  ['configured match first', [show('wanted'), show('other')], { SHOW_ID: 'wanted' }, 'wanted'],
  ['configured match last', [show('other'), show('wanted')], { SHOW_ID: 'wanted' }, 'wanted'],
  ['configured ID trimmed', [show(' wanted ')], { SHOW_ID: ' wanted ' }, ' wanted '],
  ['numeric ID matches configured string', [show(42)], { SHOW_ID: '42' }, 42],
  ['configured mismatch rejects first show', [show('other')], { SHOW_ID: 'wanted' }, null],
  ['duplicate configured matches rejected', [show('wanted'), show('wanted')], { SHOW_ID: 'wanted' }, null],
  ['normalized duplicate configured matches rejected', [show('wanted'), show(' wanted ')], { SHOW_ID: 'wanted' }, null],
  ['unconfigured multiple shows rejected', [show('other'), show('wanted')], {}, null],
  ['unconfigured duplicate shows rejected', [show('wanted'), show('wanted')], {}, null],
  ['sole identified show accepted', [show('wanted')], {}, 'wanted'],
  ['blank configuration uses sole show', [show('wanted')], { SHOW_ID: ' ' }, 'wanted'],
  ['sole numeric identity accepted', [show(42)], {}, 42],
  ['zero numeric identity accepted', [show(0)], {}, 0],
  ['missing identity rejected', [{ live_data: {} }], {}, null],
  ['empty identity rejected', [show('')], {}, null],
  ['whitespace identity rejected', [show(' ')], {}, null],
  ['object identity rejected', [show({})], {}, null],
  ['array identity rejected', [show(['wanted'])], { SHOW_ID: 'wanted' }, null],
  ['boolean identity rejected', [show(true)], {}, null],
  ['nonfinite identity rejected', [show(Infinity)], {}, null],
  ['null record rejected', [null], {}, null],
  ['array record rejected', [[]], {}, null],
  ['scalar record rejected', ['wanted'], {}, null],
  ['valid show with malformed extra record rejected without configuration', [show('wanted'), null], {}, null],
  ['configured unique show does not guess malformed extra record', [null, show('wanted')], { SHOW_ID: 'wanted' }, 'wanted'],
  ['empty array rejected', [], {}, null],
  ['null payload rejected', null, {}, null],
  ['object payload rejected', { records: [show('wanted')] }, {}, null],
  ['string payload rejected', 'bad JSON', {}, null],
]) {
  test(name, () => {
    const selected = api.selectLiveScoreShow(payload, env);
    if (expected === null) assert.equal(selected, null);
    else assert.equal(selected?.show_id, expected);
  });
}

for (const [name, payload, env] of [
  ['configured mismatch', [derbyShow], { SHOW_ID: 'absent' }],
  ['duplicate match', [derbyShow, derbyShow], { SHOW_ID: 'wanted' }],
  ['unconfigured ambiguity', [derbyShow, show('other')], {}],
  ['missing identity', [{ live_data: derbyShow.live_data }], {}],
  ['malformed JSON', '{broken', {}],
]) {
  test(`actual inbound ring caller returns existing unavailable reply for ${name}`, async () => {
    const offline = loadWorker(async (_url, options) => {
      // The inbound caller's log POST is intercepted too; nothing is transmitted.
      if (options?.method === 'POST') return new Response('{}');
      return new Response(typeof payload === 'string' ? payload : JSON.stringify(payload));
    });
    const response = await offline.worker.fetch(new Request('https://offline.invalid/inbound', {
      method: 'POST', body: 'Body=Derby&MessageSid=fixture&From=fixture',
    }), { ...env, LIVE_SCORE_WIDGET_URL: 'https://offline.invalid/widget' });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), '<?xml version="1.0" encoding="UTF-8"?><Response><Message>Derby — as of 12:00P\nring data unavailable right now. Try again shortly.</Message></Response>');
  });
}

test('known show ring reply retains class and trip output', async () => {
  const offline = loadWorker(async () => new Response(JSON.stringify([derbyShow])));
  const reply = await offline.buildLiveScoreWidgetRingReply({
    env: { SHOW_ID: 'wanted', LIVE_SCORE_WIDGET_URL: 'https://offline.invalid/widget' },
    decision: offline.decideRingStrict('Derby').ring, addDbg() {},
  });
  assert.equal(reply.title, 'Derby — as of 12:00P');
  assert.equal(reply.first, 'Now: Open Hunters (101)\nTrips: 4/12 | Left: 8 | Ends: 24m');
});

for (const [input, number, id, label] of [
  ['Derby', 1, 10, 'Derby'], ['df', 1, 10, 'Derby'], ['Hunter 1', 5, 48, 'Hunter 1'],
  ['h2', 6, 45, 'Hunter 2'], ['Covered', 3, 44, 'Covered'],
]) {
  test(`ring alias ${input} retains routing`, () => {
    const result = api.decideRingStrict(input);
    assert.equal(result.status, 'one');
    assert.equal(result.ring.ring_number, number);
    assert.equal(result.ring.ring_id, id);
    assert.equal(result.ring.sms_label, label);
  });
}

for (const [input, expected] of [
  ['rider Jane Doe 101', { shouldLookup: true, lane: 'rider', query: 'Jane Doe', mode: 'class', classQuery: '101' }],
  ['horse Silver Star all', { shouldLookup: true, lane: 'horse', query: 'Silver Star', mode: 'all', classQuery: '' }],
  ['r Jane Doe results', { shouldLookup: true, lane: 'rider', query: 'Jane Doe', mode: 'result', classQuery: '' }],
  ['h Silver Star next', { shouldLookup: true, lane: 'horse', query: 'Silver Star', mode: 'next', classQuery: '' }],
]) {
  test(`lookup intent ${input} retains parsing`, () => {
    assert.deepEqual(JSON.parse(JSON.stringify(api.parseLookupIntent(input))), expected);
  });
}

for (const [lane, title, first] of [
  ['rider', 'Jane Doe - as of 12:00P', 'Next: Open Hunters (101)\nSilver Star'],
  ['horse', 'Silver Star - as of 12:00P', 'Next: Open Hunters (101)\nJane Doe'],
]) {
  test(`${lane} known lookup output retains subject and class`, () => {
    const reply = api.buildLookupReply({ selected: {
      riderName: 'Jane Doe', horseName: 'Silver Star', className: 'Open Hunters', classNumber: '101',
    } }, lane, 'unused');
    assert.equal(reply.title, title);
    assert.equal(reply.first, first);
    assert.equal(reply.next, '');
    assert.equal(reply.fullText, `${title}\n${first}`);
  });
}
