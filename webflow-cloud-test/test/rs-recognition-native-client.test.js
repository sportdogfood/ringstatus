import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { mountNativeRecognition, createNativeRecognitionPresenter } from "../src/assets/rs-recognition/native-client.js";

// Small DOM protocol double: these are adapter unit tests, not browser/native proof.
function fixture() {
  const nodes = [], listeners = new Map(), values = new Map();
  function node(kind, state, name) {
    const n = { kind, state, name, value: "", textContent: "", tagName: kind === "input" ? "INPUT" : "A",
      attrs: {}, focus() { n.focused = true; }, getAttribute(key) { return n.attrs[key]; },
      closest() { return kind === "action" ? n : null; }, click() { return fire(n); } };
    if (kind === "action") n.attrs['data-rs-native-action'] = name;
    nodes.push(n); return n;
  }
  for (const state of ["recognized", "profile", "login", "recovery", "received", "unavailable"]) node("view", state);
  for (const state of ["recognized", "login", "received", "unavailable"]) node("feedback", state);
  for (const name of ["person_name", "first_name", "last_name", "sms", "pin", "email"]) node("input", "profile", name);
  node("input", "login", "identifier");
  for (const name of ["first", "last", "email"]) node("input", "recovery", name);
  for (const name of ["open", "profile", "login", "recovery", "cancel", "save", "phone-login", "recover", "retire", "continue", "close", "retry"]) node("action", "profile", name);
  function query(selector) {
    if (selector.includes(",")) return [...new Set(selector.split(",").flatMap(query))];
    const state = selector.match(/data-rs-native-state="([^"]+)"/)?.[1];
    const inputName = selector.match(/name="([^"]+)"/)?.[1];
    const action = selector.match(/data-rs-native-action="([^"]+)"/)?.[1];
    return nodes.filter(n => (!state || n.state === state) &&
      (selector.includes("data-rs-native-feedback") ? n.kind === "feedback" :
       selector.includes("data-rs-native-display") ? false :
       selector.includes("data-rs-native-action") ? n.kind === "action" && (!action || n.name === action) :
       selector.includes("input") || inputName ? n.kind === "input" && (!inputName || n.name === inputName) : n.kind === "view"));
  }
  const root = { querySelector: selector => query(selector)[0], querySelectorAll: query,
    contains: target => nodes.includes(target), getAttribute: () => "/rs-barn-onboarding-setup",
    addEventListener(type, callback, options) {
      if (!listeners.has(type)) listeners.set(type, []);
      listeners.get(type).push(callback);
      options.signal.addEventListener("abort", () => listeners.set(type, listeners.get(type).filter(fn => fn !== callback)));
    } };
  async function fire(target) { for (const fn of listeners.get("click") || []) await fn({ target, preventDefault() {} }); }
  const storage = { getItem: key => values.get(key), setItem: (key, value) => values.set(key, value) };
  let uid = 0;
  return { root, nodes, listeners, storage, uuid: () => "synthetic-" + (++uid),
    click: name => fire(nodes.find(n => n.kind === "action" && n.name === name)),
    input: (name, value) => { nodes.find(n => n.kind === "input" && n.state === "profile" && n.name === name).value = value; } };
}
const settle = () => new Promise(resolve => setImmediate(resolve));
const args = f => ({ root: f.root, baseUrl: "https://example.invalid/test/rs-recognition/", present: () => {},
  persistentStorage: f.storage, sessionStorageImpl: f.storage, uuid: f.uuid, navigate: () => {} });

test("distinct lookup observations in one session retain distinct audit identities", async () => {
  const f = fixture(), events = [];
  let recognized = false;
  const api = mountNativeRecognition({ ...args(f), fetchImpl: async (url, options = {}) => {
    if (url.pathname.endsWith('session')) { events.push(JSON.parse(options.body)); return Response.json({ ok: true }); }
    return Response.json({ ok: true, recognized, person_uid: 'synthetic-person', person_record_id: 'recSynthetic00001' });
  } });
  await settle(); await settle();
  recognized = true; await api.lookup();
  assert.equal(events.length, 2);
  assert.equal(events[0].session_uid, events[1].session_uid);
  assert.equal(events[0].event_result, 'not_matched');
  assert.equal(events[1].event_result, 'matched');
  assert.notEqual(events[0].idempotency_key, events[1].idempotency_key);
  assert.notEqual(events[0].session_event_uid, events[1].session_event_uid);
  api.destroy();
});

test("native presenter emits IX3 events and makes only the open state keyboard-accessible", () => {
  const nodes = new Map(), events = [];
  const root = { attrs: {}, setAttribute(key, value) { this.attrs[key] = value; }, querySelector(selector) {
    if (!nodes.has(selector)) nodes.set(selector, { attrs: {}, setAttribute(key, value) { this.attrs[key] = value; } });
    return nodes.get(selector);
  } };
  const present = createNativeRecognitionPresenter({ root, ix3: { emit: event => events.push(event) } });
  present({ state: "profile", open: true, busy: false });
  present({ state: "profile", open: true, busy: true });
  assert.deepEqual(events, ["rs-native-recognize-profile"]);
  assert.equal(root.attrs["aria-busy"], "true");
  assert.equal(nodes.get('[data-rs-native-state="profile"]').inert, false);
  assert.equal(nodes.get('[data-rs-native-state="login"]').inert, true);
  present({ state: "profile", open: false, busy: false });
  assert.equal(events.at(-1), "rs-native-recognize-closed");
  assert.equal(nodes.get('[data-rs-native-overlay]').inert, true);
  assert.throws(() => present({ state: "invented", open: true }), /invalid_native_state/);
  assert.equal(events.length, 2);
});

test("loading adapter twice attaches one owner and never injects markup/styles", async () => {
  const f = fixture(); let lookups = 0;
  const options = { ...args(f), fetchImpl: async url => {
    if (url.pathname.endsWith("device")) lookups++;
    return Response.json({ ok: true, recognized: false, recognition_status: "unknown_device" });
  } };
  const first = mountNativeRecognition(options);
  assert.equal(mountNativeRecognition(options), first);
  await settle(); await settle();
  assert.equal(f.listeners.get("click").length, 1); assert.equal(lookups, 1);
  const source = readFileSync(new URL("../src/assets/rs-recognition/native-client.js", import.meta.url), "utf8");
  assert.doesNotMatch(source, /innerHTML|createElement|\.style\b|<style|React/);
  first.destroy(); assert.equal(f.listeners.get("click").length, 0);
});

test("double-click and retry retain one request ID without inventing successful display", async () => {
  const f = fixture(), requests = [], presentations = [];
  let fail = true;
  mountNativeRecognition({ ...args(f), present: value => presentations.push(value), fetchImpl: async (url, options) => {
    if (url.pathname.endsWith("action")) {
      const body = JSON.parse(options.body); requests.push(body);
      if (fail) throw new Error("synthetic-timeout");
      return Response.json({ ok: true, recognized: true, person_name: body.user, person_uid: "synthetic-person", person_record_id: "recSynthetic00001", audit_status: "recorded" });
    }
    return Response.json({ ok: true, recognized: false, recognition_status: "unknown_device" });
  } });
  await settle(); await settle();
  f.input("person_name", "Synthetic Profile"); f.input("sms", "2025550199");
  await Promise.all([f.click("save"), f.click("save")]);
  assert.equal(requests.length, 1);
  assert.equal(presentations.at(-1).state, "unavailable");
  await f.click("save"); assert.equal(requests.length, 1);
  fail = false; await f.click("retry");
  assert.equal(requests.length, 2); assert.deepEqual(requests[0], requests[1]);
  assert.equal(presentations.at(-1).state, "recognized");
});

test("recognized response never fills the PIN input even if upstream sends one", async () => {
  const f = fixture();
  mountNativeRecognition({ ...args(f), fetchImpl: async () => Response.json({ ok: true, recognized: true,
    person_name: "Synthetic", member_pin: "synthetic-secret", first_name: "Synthetic" }) });
  await settle(); await settle();
  assert.equal(f.nodes.find(n => n.kind === "input" && n.name === "pin").value, "");
});

test("missing native presenter and insecure origin fail before any data request", () => {
  const f = fixture();
  assert.throws(() => mountNativeRecognition({ ...args(f), present: null }), /native_presenter_required/);
  assert.throws(() => mountNativeRecognition({ ...args(f), baseUrl: "http://example.invalid/" }), /invalid_recognition_origin/);
});

test("unmatched login retains the request ID while its audit is pending", async () => {
  const f = fixture(), requests = [];
  let pending = true;
  mountNativeRecognition({ ...args(f), fetchImpl: async (url, options) => {
    if (url.pathname.endsWith("action")) {
      requests.push(JSON.parse(options.body));
      return Response.json({ ok: true, recognized: false, audit_status: pending ? "pending" : "recorded" });
    }
    return Response.json({ ok: true, recognized: false });
  } });
  await settle(); await settle();
  await f.click("phone-login"); pending = false; await f.click("retry");
  assert.equal(requests.length, 2); assert.deepEqual(requests[0], requests[1]);
});
