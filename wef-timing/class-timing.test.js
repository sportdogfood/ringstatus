'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {evaluateClassTiming} = require('./class-timing');

function fixture(lane = 'data_live') {
  const identity = {customer:1, show:2, date:'2026-10-29', ring:3, group:4, class:5,
    occurrence:'source|occurrence-1', phase:'base'};
  const evidence = {identity:{...identity}, source_lane:lane, observed_at:'2026-10-29T14:30:00Z',
    valid_until:'2026-10-29T14:40:00Z', ref:{source_key:'schedule-1', revision:'revision-1'}};
  return {identity, sourceLane:lane, timezone:'America/New_York', now:'2026-10-29T14:30:00Z',
    data:{...evidence, start:{at:'2026-10-29T10:00:00-04:00', kind:'estimated'},
      counts:{scope:'class', total:10, completed:10, completion_scope:'whole_class'}}};
}
function withLive(input) {
  input.live = structuredClone(input.data);
  input.live.ref = {source_key:'live-class-5', revision:'live-revision-1'};
  return input;
}
function withPace(input, value = 2) {
  input.ringPace = {...structuredClone(input.data), answer_key:'pace-answer-1',
    member_class_ids:[5, 6], pace_minutes_per_trip:value, method:'observed_interval',
    ref:{source_key:'group-4', revision:'pace-revision-1'}};
  return input;
}

test('unchanged evidence does not generate a new snapshot just from clock movement',()=>{
  const input=fixture(); input.data.counts.completed=0;
  const first=evaluateClassTiming(input);
  input.now='2026-10-29T14:31:00Z';
  const next=evaluateClassTiming(input);
  assert.equal(first.fields['content-hash'],next.fields['content-hash']);
  assert.notEqual(first.payload.minutes_since_start,next.payload.minutes_since_start);
});

for (const lane of ['data_live']) {
  test(`${lane}: missing Live uses schedule and three minutes; candidate is not actual`, () => {
    const result = evaluateClassTiming(fixture(lane));
    assert.equal(result.payload.duration_minutes, 30);
    assert.equal(result.payload.estimated_end_at, '2026-10-29T14:30:00.000Z');
    assert.equal(result.payload.completion_state, 'estimated');
    assert.deepEqual(result.flags, {completion_candidate:true, actual_completion:false,
      data_fallback:true, default_pace:true});
    assert.equal(result.fields['source-lane'], lane);
    assert.deepEqual(JSON.parse(result.fields['payload-json']), result.payload);
  });
}

test('matching group pace changes duration without copying group counts', () => {
  const input = withPace(fixture()); input.ringPace.counts.total = 999;
  const result = evaluateClassTiming(input);
  assert.equal(result.payload.total_trips, 10);
  assert.equal(result.payload.duration_minutes, 20);
  assert.equal(result.payload.pace_answer_key, 'pace-answer-1');
});

test('invalid pace, wrong lane, wrong class roster, expiry and missing provenance use default', () => {
  for (const change of [p => p.pace_minutes_per_trip = 0, p => p.pace_minutes_per_trip = Infinity,
    p => p.source_lane = 'schedules_live', p => p.identity.group = 8, p => p.member_class_ids = [6],
    p => p.valid_until = '2026-10-29T14:30:00Z', p => delete p.ref]) {
    const input = withPace(fixture()); change(input.ringPace);
    const result = evaluateClassTiming(input);
    assert.equal(result.payload.pace_minutes_per_trip, 3);
    assert.equal(result.payload.pace_answer_key, null);
  }
});

test('no arbitrary pace maximum and no input mutation', () => {
  const input = withPace(fixture(), 25), before = structuredClone(input);
  assert.equal(evaluateClassTiming(input).payload.duration_minutes, 250);
  assert.deepEqual(input, before);
});

test('Live class evidence overrides schedule independently for counts and start', () => {
  const input = withLive(fixture());
  input.live.start = {at:'2026-10-29T14:10:00Z', kind:'actual'};
  input.live.counts.completed = 2;
  const {payload} = evaluateClassTiming(input);
  assert.equal(payload.start_kind, 'actual');
  assert.equal(payload.count_source, 'live');
  assert.equal(payload.estimated_end_at, '2026-10-29T14:40:00.000Z');
  assert.equal(payload.completion_state, 'unknown');
});

test('wrong occurrence, future observation and stale Live cannot contaminate fallback', () => {
  for (const change of [s => s.identity.occurrence = 'other', s => s.identity.customer = 9,
    s => s.observed_at = '2026-10-29T14:31:00Z', s => s.valid_until = '2026-10-29T14:30:00Z']) {
    const input = withLive(fixture()); input.live.counts.total = 100; change(input.live);
    assert.equal(evaluateClassTiming(input).payload.total_trips, 10);
  }
});

test('group-scoped counts never become class counts; missing is not zero', () => {
  const input = fixture(); input.data.counts.scope = 'group';
  const {payload} = evaluateClassTiming(input);
  assert.equal(payload.total_trips, null);
  assert.equal(payload.duration_minutes, null);
  assert.equal(payload.completion_candidate, false);
});

test('completion requires positive complete whole-class counts and end reached', () => {
  for (const change of [i => i.data.counts.completed = null,
    i => i.data.counts = {scope:'class', total:0, completed:0, completion_scope:'whole_class'},
    i => i.data.counts.completed = 9, i => i.data.counts.completed = 11,
    i => i.data.counts.total = 10.5, i => i.data.counts.total = -1,
    i => i.data.counts.completion_scope = 'phase', 
    i => i.now = '2026-10-29T14:29:59Z']) {
    const input = fixture(); change(input);
    assert.equal(evaluateClassTiming(input).flags.completion_candidate, false);
  }
});

test('explicit fresh all-phase Live completion is distinguished from estimated completion', () => {
  const input = withLive(fixture());
  input.live.completion = {state:'completed', at:'2026-10-29T14:29:00Z', all_phases_complete:true};
  assert.equal(evaluateClassTiming(input).payload.completion_state, 'actual');
  input.live.completion.all_phases_complete = false;
  assert.equal(evaluateClassTiming(input).payload.completion_state, 'unknown');
  input.live.completion = {state:'cancelled', at:'2026-10-29T14:29:00Z', all_phases_complete:true};
  assert.equal(evaluateClassTiming(input).flags.actual_completion, false);
  assert.equal(evaluateClassTiming(input).flags.completion_candidate, false);
});

test('full date arithmetic crosses midnight without wrapping or losing a day', () => {
  const input = fixture(); input.data.start.at = '2026-10-29T23:50:00-04:00';
  const {payload} = evaluateClassTiming(input);
  assert.equal(payload.estimated_end_at, '2026-10-30T04:20:00.000Z');
  assert.equal(payload.minutes_until_start, 800);
  assert.equal(payload.completion_candidate, false);
});

test('DST rollover uses elapsed minutes, not local clock subtraction', () => {
  const input = fixture(); input.identity.date = '2026-11-01';
  input.data.identity.date = input.identity.date;
  input.now = input.data.observed_at = '2026-11-01T05:50:00Z';
  input.data.valid_until = '2026-11-01T06:00:00Z';
  input.data.start.at = '2026-11-01T01:50:00-04:00';
  assert.equal(evaluateClassTiming(input).payload.estimated_end_at, '2026-11-01T06:20:00.000Z');
});

test('offsetless, impossible and wrong local date starts stay unknown', () => {
  for (const at of ['2026-10-29T10:00:00', '2026-02-30T10:00:00Z', '2026-10-30T14:00:00Z']) {
    const input = fixture(); input.data.start.at = at;
    assert.equal(evaluateClassTiming(input).payload.start_kind, 'unknown');
  }
});

test('invalid identity, lane, timezone and clock fail explicitly', () => {
  for (const change of [i => i.identity.class = null, i => i.identity.occurrence = '',
    i => i.identity.phase = 'jump_off', i => i.identity.date = '2026-02-30',
    i => i.sourceLane = 'other', i => i.timezone = 'bad/timezone', i => i.now = '14:30']) {
    const input = fixture(); change(input); assert.throws(() => evaluateClassTiming(input));
  }
});

test('same inputs give identical output; different class occurrence and lanes have separate keys', () => {
  const input = fixture(), first = evaluateClassTiming(input);
  assert.deepEqual(evaluateClassTiming(structuredClone(input)), first);
  input.identity.occurrence = input.data.identity.occurrence = 'another|occurrence';
  assert.notEqual(evaluateClassTiming(input).fields['scope-key'], first.fields['scope-key']);
  assert.throws(() => evaluateClassTiming(fixture('schedules_live')), /Invalid source lane/);
});

test('all missing observations produce explicit unavailable output with no completion', () => {
  const input = fixture(); delete input.data;
  const result = evaluateClassTiming(input);
  assert.equal(result.fields.availability, 'unavailable');
  assert.equal(result.payload.completion_state, 'unknown');
  assert.equal(result.payload.total_trips, null);
  assert.equal(result.flags.completion_candidate, false);
});

test('unknown round coverage produces only the owner-requested calculated candidate',()=>{
 const input=fixture();delete input.data.counts.completion_scope;
 const result=evaluateClassTiming(input);
 assert.equal(result.flags.completion_candidate,true);
 assert.equal(result.payload.count_completion_scope,'unknown');
 assert.equal(result.payload.completion_state,'estimated');
 assert.equal(result.flags.actual_completion,false);
});
test('Schedules input is explicitly rejected and unbounded freshness is not accepted',()=>{
 const input=fixture();assert.throws(()=>evaluateClassTiming({...input,schedule:input.data}),/Schedules/);
 input.data.valid_until='2026-10-30T14:40:00Z';
 assert.equal(evaluateClassTiming(input).fields.availability,'unavailable');
});
