// Native Recognize behavior only. The page owns markup, classes and presentation.
// Nothing mounts until the page supplies its verified native state presenter.
export function createNativeRecognitionPresenter({ root, ix3 }) {
  if (!root || typeof ix3?.emit !== "function") throw new Error("native_ix3_required");
  const states = ["recognized", "profile", "login", "recovery", "received", "unavailable"];
  const surfaces = states.map(state => {
    const node = root.querySelector(`[data-rs-native-state="${state}"]`);
    if (!node) throw new Error("native_state_missing:" + state);
    return { state, node };
  });
  const overlay = root.querySelector('[data-rs-native-overlay]');
  if (!overlay) throw new Error("native_overlay_missing");
  let lastEvent;
  return ({ state, open, busy }) => {
    if (!states.includes(state)) throw new Error("invalid_native_state");
    root.setAttribute("aria-busy", String(!!busy));
    overlay.inert = !open;
    overlay.setAttribute("aria-hidden", String(!open));
    for (const surface of surfaces) {
      const hidden = !open || surface.state !== state;
      surface.node.inert = hidden;
      surface.node.setAttribute("aria-hidden", String(hidden));
    }
    // Native IX3 owns visibility and layout. DOM custom events do not invoke IX3.
    const eventName = "rs-native-recognize-" + (open ? state : "closed");
    if (lastEvent !== eventName) { ix3.emit(eventName); lastEvent = eventName; }
  };
}

export function mountNativeRecognition({ root, baseUrl, present, fetchImpl = fetch,
  persistentStorage = localStorage, sessionStorageImpl = sessionStorage,
  locationImpl = globalThis.location, historyImpl = globalThis.history,
  navigate = url => location.assign(url), uuid = () => crypto.randomUUID(), timeoutMs = 12000 }) {
  if (!root || typeof present !== "function") throw new Error("native_presenter_required");
  if (root.__rsNativeRecognition) return root.__rsNativeRecognition;
  const endpoint = new URL(baseUrl);
  if (endpoint.protocol !== "https:" && endpoint.hostname !== "localhost") throw new Error("invalid_recognition_origin");
  if (globalThis.location?.origin && endpoint.origin !== globalThis.location.origin) throw new Error("native_same_origin_required");
  endpoint.pathname = endpoint.pathname.replace(/\/rs-recognition\/?$/, "/rs-inputs/native-recognition");
  if (!endpoint.pathname.endsWith('/rs-inputs/native-recognition')) throw new Error('native_cookie_scope_required');
  const get = selector => root.querySelector(selector);
  const all = selector => [...root.querySelectorAll(selector)];
  for (const state of ["recognized", "profile", "login", "recovery", "received", "unavailable"]) {
    if (!get(`[data-rs-native-state="${state}"]`)) throw new Error("native_state_missing:" + state);
  }
  if (!get('[data-rs-native-feedback]')) throw new Error("native_feedback_missing");
  const fragment = new URLSearchParams(locationImpl?.hash?.slice(1) || '');
  let invitation = null;
  if (fragment.has('invite')) {
    const tokens = fragment.getAll('invite');
    historyImpl.replaceState(historyImpl.state, '', locationImpl.pathname + locationImpl.search);
    invitation = tokens.length === 1 && /^[A-Za-z0-9_-]{43}$/.test(tokens[0]) ? tokens[0] : '';
  }
  const tokenKey = "rs_recognition_device_token_v1";
  const sessionKey = "rs_native_recognition_session_v1";
  let deviceToken = persistentStorage.getItem(tokenKey);
  if (!deviceToken) { deviceToken = uuid(); persistentStorage.setItem(tokenKey, deviceToken); }
  let sessionUid = sessionStorageImpl.getItem(sessionKey);
  if (!sessionUid) { sessionUid = uuid(); sessionStorageImpl.setItem(sessionKey, sessionUid); }
  let person = null, deviceConfirmed = false, busy = false, pending = null, state = "unavailable", opened = false, returnFocus = null;
  const controller = new AbortController();
  function feedback(message) {
    const target = get(`[data-rs-native-state="${state}"] [data-rs-native-feedback]`) || get('[data-rs-native-feedback]');
    target.textContent = message || ({ recognized: deviceConfirmed ? "This browser recognizes your profile." : "Confirm this browser with Continue before editing.", login: "Open your invitation link to sign in.", received: "SMS recovery has not been confirmed." }[state] || "");
  }
  function show(next, message = "") {
    const changed = state !== next;
    state = next; feedback(message);
    present({ state, open: opened, busy });
    if (opened && changed) get(`[data-rs-native-state="${state}"] input, [data-rs-native-state="${state}"] [data-rs-native-action]`)?.focus();
  }
  function bindPerson(value) {
    person = value?.recognized || value?.invited_profile ? value : null;
    deviceConfirmed = value?.recognized === true && !value?.requires_device_confirmation;
    for (const field of ["person_name", "first_name", "last_name", "primary_phone_e164", "email"]) {
      for (const node of all(`[data-rs-native-display="${field}"]`)) node.textContent = person?.[field] || "";
      const inputName = field === "primary_phone_e164" ? "sms" : field;
      for (const node of all(`[data-rs-native-state="profile"] [name="${inputName}"]`)) node.value = person?.[field] || "";
    }
    for (const node of all('[name="pin"]')) node.value = "";
    for (const node of all('[data-rs-native-display="greeting"]')) node.textContent = person ? `Hi ${person.first_name || person.person_name || "there"}` : "Recognize";
  }
  function values(view) {
    return Object.fromEntries(all(`[data-rs-native-state="${view}"] input`).map(node => [node.name, node.value.trim()]));
  }
  async function json(path, options = {}) {
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), timeoutMs);
    try {
      const operationUrl = new URL(path, 'https://native.invalid/');
      const target = path === 'accept_invitation' ? new URL('access', endpoint) : new URL(endpoint);
      if (path !== 'accept_invitation') {
        target.search = operationUrl.search;
        target.searchParams.set('operation', operationUrl.pathname.slice(1));
      }
      const response = await fetchImpl(target, { ...options, credentials: 'same-origin', signal: abort.signal });
      const data = await response.json();
      if (!response.ok || data.ok !== true) throw new Error(data.error || "recognition_unavailable");
      return data;
    } finally { clearTimeout(timer); }
  }
  async function acceptInvitation() {
    opened = true;
    if (!invitation) { show('login', 'This invitation is invalid. Request a new recovery SMS.'); return; }
    busy = true; show('login', 'Accepting your invitation…');
    const token = invitation;
    invitation = null;
    try {
      await json('accept_invitation', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }) });
    } catch {
      show('login', 'Invitation acceptance could not be confirmed. Open recognition to check access, or request a new recovery SMS.');
      busy = false; present({ state, open: opened, busy });
      return;
    }
    busy = false;
    await lookup();
  }
  async function lookup() {
    if (busy || pending) return;
    busy = true; feedback("Checking recognition…"); present({ state, open: opened, busy });
    try {
      const data = await json("device?device_token=" + encodeURIComponent(deviceToken));
      bindPerson(data); pending = null;
      show(person ? "recognized" : "login");
      const eventUid = uuid();
      const event = { device_token: deviceToken, session_uid: sessionUid, session_event_uid: eventUid, event_type: "recognition",
        event_result: person ? "matched" : "not_matched", idempotency_key: "recognition:" + eventUid,
        recognition_status: data.recognition_status, matched_by: data.matched_by,
        person_record_id: person?.person_record_id || "", device_record_id: data.device_record_id || "" };
      try { await json("session", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(event) }); }
      catch { feedback("Recognition checked. The visit log could not be recorded."); }
    } catch (error) { bindPerson(null); show(error.message === 'authentication_required' ? 'login' : 'unavailable', error.message === 'authentication_required' ? 'Open your invitation link to sign in.' : 'Recognition could not be checked. Try again.'); }
    finally { busy = false; present({ state, open: opened, busy }); }
  }
  async function action(name, data = {}, continuation = "recognized") {
    if (busy) return;
    if (pending) { feedback("Confirm the pending request with Try again before starting another action."); return; }
    busy = true;
    const body = { action: name, device_token: deviceToken, session_uid: sessionUid,
      session_event_uid: uuid(), ...data };
    pending = { body, continuation };
    await executePending();
  }
  async function executePending() {
    if (!pending) return;
    busy = true; feedback("Saving…"); present({ state, open: opened, busy });
    try {
      const data = await json("action", { method: "POST", headers: { "Content-Type": "application/json", "X-RS-Audit-Outcome": "report" }, body: JSON.stringify(pending.body) });
      const next = pending.continuation;
      if (pending.body.action === "retire_device") {
        bindPerson(null);
        deviceToken = uuid(); persistentStorage.setItem(tokenKey, deviceToken);
      } else if (data.recognized === true) bindPerson(data);
      else if (pending.body.action === "phone_login") {
        bindPerson(null);
        if (data.audit_status === "pending") show("login", "No matching active profile was found. The action log is pending; retry the same request.");
        else { pending = null; show("login", "No matching active profile was found."); }
        return;
      }
      if (data.audit_status === "pending") {
        // Preserve the successful mutation. A retry reuses this exact request ID.
        show(next, "Saved. The action log is pending; retry the same request to check it.");
        return;
      }
      pending = null;
      if (next === "navigate") {
        opened = false; show("recognized"); navigate(root.getAttribute("data-rs-setup-path"));
      } else if (next === "closed") { opened = false; show("recognized"); returnFocus?.focus(); }
      else show(next, next === "received" ? "Request received. If your account is eligible, check for an SMS. Delivery is not confirmed." : "");
    } catch (error) {
      const invalid = /^(missing_|invalid_|ambiguous_|phone_already|unauthorized_|request_id_conflict|authentication_required|invitation_only|sms_recovery_unconfigured)/.test(error.message);
      if (invalid) { pending = null; show("unavailable", "Please check your details. " + error.message.replaceAll("_", " ")); }
      else show("unavailable", "The outcome could not be confirmed. Retry keeps the same request ID.");
    } finally { busy = false; present({ state, open: opened, busy }); }
  }
  function owned() { return { person_record_id: person?.person_record_id || "", person_uid: person?.person_uid || "" }; }
  async function click(event) {
    const target = event.target.closest?.('[data-rs-native-action]');
    if (!target || !root.contains(target)) return;
    event.preventDefault(); if (busy) return;
    const command = target.getAttribute('data-rs-native-action');
    if (command === "open") { returnFocus = target; opened = true; show(state); await lookup(); }
    else if (command === "profile") { opened = true; show(deviceConfirmed ? "profile" : "recognized"); }
    else if (command === "login" || command === "recovery") show(command);
    else if (command === "cancel") show(person ? "recognized" : "login");
    else if (command === "save") {
      if (!person || !deviceConfirmed) { show(person ? "recognized" : "login", "Use your invitation and confirm this browser before editing."); return; }
      const v = values("profile");
      await action(person ? "update_profile" : "create_profile", { user: v.person_name, first: v.first_name, last: v.last_name, sms: v.sms, pin: v.pin, email: v.email, ...owned() });
    } else if (command === "phone-login") await action("phone_login", { sms: values("login").identifier });
    else if (command === "recover") await action("recovery", values("recovery"), "received");
    else if (command === "retire") await action("retire_device", {}, "recovery");
    else if (command === "continue") { if (person) await action("confirm_device", owned(), "navigate"); }
    else if (command === "close") {
      if (person && deviceConfirmed) await action("confirm_device", owned(), "closed");
      else { opened = false; show(state); returnFocus?.focus(); }
    } else if (command === "retry") { if (pending) await executePending(); else await lookup(); }
  }
  root.addEventListener("click", click, { signal: controller.signal });
  root.addEventListener("keydown", event => {
    const target = event.target.closest?.('[data-rs-native-action]');
    if (target && event.key === " " && target.tagName === "A") { event.preventDefault(); target.click(); }
    if (event.key === "Escape" && opened && !busy) get('[data-rs-native-action="close"]')?.click();
    if (event.key === "Tab" && opened) {
      const nodes = all(`[data-rs-native-state="${state}"] input, [data-rs-native-state="${state}"] [data-rs-native-action], [data-rs-native-action="close"]`).filter(node => !node.disabled && !node.inert);
      if (nodes.length && event.shiftKey && event.target === nodes[0]) { event.preventDefault(); nodes.at(-1).focus(); }
      else if (nodes.length && !event.shiftKey && event.target === nodes.at(-1)) { event.preventDefault(); nodes[0].focus(); }
    }
    if (event.key === "Enter" && event.target.tagName === "INPUT") {
      const command = state === "profile" ? "save" : state === "login" ? "phone-login" : state === "recovery" ? "recover" : null;
      if (command) { event.preventDefault(); get(`[data-rs-native-action="${command}"]`)?.click(); }
    }
  }, { signal: controller.signal });
  const api = { lookup, destroy() { controller.abort(); delete root.__rsNativeRecognition; } };
  root.__rsNativeRecognition = api;
  bindPerson(null); show("unavailable");
  if (invitation !== null) void acceptInvitation();
  else void lookup();
  return api;
}
