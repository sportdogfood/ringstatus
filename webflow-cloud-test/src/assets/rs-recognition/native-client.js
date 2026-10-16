// Native Recognize behavior only. The page owns markup, classes and presentation.
// Nothing mounts until the page supplies its verified native state presenter.
// Standalone so the same silent entry can later be called from a global footer.
const confirmationKey = 'rs_native_recognition_confirmation_v1';
const signOutKey = 'rs_recognition_pending_signout_v1';
const confirmationGenerations = new WeakMap();
const safeStores = new WeakMap();
function safeStorage(storage) {
  if (safeStores.has(storage)) return safeStores.get(storage);
  const memory = new Map();
  const safe = {getItem(k){try{return storage.getItem(k) ?? memory.get(k) ?? null;}catch{return memory.get(k) ?? null;}},setItem(k,v){memory.set(k,String(v));try{storage.setItem(k,String(v));}catch{}},removeItem(k){memory.delete(k);try{storage.removeItem(k);}catch{}}};
  safeStores.set(storage,safe); safeStores.set(safe,safe); return safe;
}
function browserStorage(name) {try{return globalThis[name] || {};}catch{return {};}}
function clearConfirmation(storage) {
  const generation = (confirmationGenerations.get(storage) || 0) + 1;
  confirmationGenerations.set(storage, generation);
  storage.removeItem(confirmationKey);
  return generation;
}

export async function runSilentRecognition({ endpoint, fetchImpl = fetch,
  persistentStorage = browserStorage("localStorage"), sessionStorageImpl = browserStorage("sessionStorage"),
  navigate = path => location.assign(path), launcherPath = '/', redirectOnRecognized = true, now = () => Date.now(),
  uuid = () => crypto.randomUUID(), timeoutMs = 12000, pagePath = globalThis.location?.pathname || '/', onOutcome = () => {} }) {
  persistentStorage=safeStorage(persistentStorage); sessionStorageImpl=safeStorage(sessionStorageImpl);
  const tokenKey = 'rs_recognition_device_token_v1', expiryKey = 'rs_recognition_device_expires_v1';
  const sessionKey = 'rs_native_recognition_session_v1';
  const abort = new AbortController(), timer = setTimeout(() => abort.abort(), timeoutMs);
  let generation;
  try {
    if(persistentStorage.getItem(signOutKey)){onOutcome({outcome:'unknown'});return false;}
    // Display context only: every visit still waits for server revalidation.
    generation = clearConfirmation(sessionStorageImpl);
    const target = new URL(endpoint);
    if (globalThis.location?.origin && target.origin !== globalThis.location.origin) return false;
    if (!launcherPath.startsWith('/') || launcherPath.startsWith('//')) return false;
    const expiry = persistentStorage.getItem(expiryKey);
    if (expiry && (!Number.isFinite(Number(expiry)) || Number(expiry) <= now())) {
      persistentStorage.removeItem(tokenKey); persistentStorage.removeItem(expiryKey);
    }
    let token = persistentStorage.getItem(tokenKey);
    if (!token) { token = uuid(); persistentStorage.setItem(tokenKey, token); }
    let session = sessionStorageImpl.getItem(sessionKey);
    if (!session) { session = uuid(); sessionStorageImpl.setItem(sessionKey, session); }
    target.searchParams.set('operation', 'recognize');
    const response = await fetchImpl(target, { method: 'POST', credentials: 'same-origin', signal: abort.signal,
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ device_token: token, session_uid: session, page_path: pagePath.split(/[?#]/)[0] }) });
    const data = await response.json();
    // A newer check or account-switch action supersedes an older response.
    if (confirmationGenerations.get(sessionStorageImpl) !== generation) return false;
    if (['session_identity_conflict', 'session_closed'].includes(data.error)) {
      // A later normal visit uses a fresh tab UID; never reopen/rebind this row.
      sessionStorageImpl.setItem(sessionKey, uuid());
    }
    if (!response.ok || data.ok !== true) {onOutcome({outcome:'unavailable',error:data.error});return false;}
    if(data.recognized!==true){persistentStorage.removeItem('rs_recognition_identity_v1');onOutcome({outcome:'unknown'});return false;}
    onOutcome({outcome:'recognized',data});
    persistentStorage.setItem(expiryKey, String(now() + 365 * 86400000));
    sessionStorageImpl.setItem(confirmationKey, JSON.stringify({ session_uid: session, confirmed: true, confirmed_at: now() }));
    // A global footer must not cause a redirect loop on the launcher itself.
    if (redirectOnRecognized && globalThis.location?.pathname !== launcherPath) navigate(launcherPath);
    return true;
  } catch {
    onOutcome({outcome:'unavailable'});
    if (confirmationGenerations.get(sessionStorageImpl) === generation) {
      try { clearConfirmation(sessionStorageImpl); } catch { /* Storage can be unavailable. */ }
    }
    return false;
  }
  finally { clearTimeout(timer); }
}

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
  persistentStorage = browserStorage("localStorage"), sessionStorageImpl = browserStorage("sessionStorage"),
  locationImpl = globalThis.location, historyImpl = globalThis.history,
  navigate = url => location.assign(url), uuid = () => crypto.randomUUID(), timeoutMs = 12000,
  silentRecognition = true, launcherPath = '/', minimal = false }) {
  persistentStorage=safeStorage(persistentStorage); sessionStorageImpl=safeStorage(sessionStorageImpl);
  if (!root || typeof present !== "function") throw new Error("native_presenter_required");
  if (root.__rsNativeRecognition) return root.__rsNativeRecognition;
  // Only this explicitly supported native input destination may override the page default.
  const returns = new URLSearchParams(locationImpl?.search || '').getAll('return_to');
  const signOut = new URLSearchParams(locationImpl?.search || '').get('sign_out') === '1';
  const returnPath = returns.length === 1 && ['/rs-barn-onboarding-alerts','/rs-barn-onboarding-setup-v24'].includes(returns[0]) ? returns[0] : null;
  const endpoint = new URL(baseUrl);
  if (endpoint.protocol !== "https:" && endpoint.hostname !== "localhost") throw new Error("invalid_recognition_origin");
  if (globalThis.location?.origin && endpoint.origin !== globalThis.location.origin) throw new Error("native_same_origin_required");
  endpoint.pathname = endpoint.pathname.replace(/\/rs-recognition\/?$/, "/rs-inputs/native-recognition");
  if (!endpoint.pathname.endsWith('/rs-inputs/native-recognition')) throw new Error('native_cookie_scope_required');
  const get = selector => root.querySelector(selector);
  const all = selector => [...root.querySelectorAll(selector)];
  for (const state of (minimal ? ["recognized", "login", "unavailable"] : ["recognized", "profile", "login", "recovery", "received", "unavailable"])) {
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
  function currentSessionUid() {
    sessionUid = sessionStorageImpl.getItem(sessionKey) || sessionUid;
    return sessionUid;
  }
  let person = null, deviceConfirmed = false, busy = false, pending = null, state = "unavailable", opened = false, returnFocus = null;
  let otpPhone = null, otpRequestId = null;
  let entryMode='phone', verified=false, tenantReady=false, credentialPending=null, resultVersion=0;
  const nativePresent=present;
  function snapshot(){
    const stage=busy?'checking':tenantReady?'tenant_ready':verified?'recognized':otpPhone?'code':state==='login'?'entry':'unavailable';
    const allowedActions=busy?[]:verified?(tenantReady?['not_you']:['not_you','retry']):['show_phone','show_pin','show_invite','retry'];
    return {stage,busy,halted:false,entryMode,allowedActions,resultVersion};
  }
  present=value=>{nativePresent(value);root.setAttribute('data-rs-recognition-state',JSON.stringify(snapshot()));};
  async function finishIdentity(alreadyRecognized=null){
    const previousBusy=busy;busy=true;present({state,open:opened,busy});
    try {
    let outcome;
    const matched=alreadyRecognized?.recognized===true || await runSilentRecognition({endpoint,fetchImpl,persistentStorage,sessionStorageImpl,navigate,launcherPath,redirectOnRecognized:false,uuid,timeoutMs,onOutcome:v=>{outcome=v;}});
    verified=matched;
    if(!matched){show(outcome?.outcome==='unknown'?'login':'unavailable','Recognition could not finish. Try again.');return;}
    // The same existing access endpoint establishes the actor for both interfaces.
    const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),timeoutMs);
    let response,access;
    try {
      response=await fetchImpl(new URL('access',endpoint),{credentials:'same-origin',signal:abort.signal});
      access=await response.json();
    } finally {clearTimeout(timer);}
    if(!response.ok||access.ok!==true||!access.actor?.id||access.actor.id!==(alreadyRecognized||outcome?.data)?.person_uid){show('unavailable','Recognized, but Tenants access could not be confirmed.');return;}
    tenantReady=true; resultVersion++; opened=false; show('recognized');
    persistentStorage.setItem('rs_recognition_identity_v1', JSON.stringify({ personUid: access.actor.id, deviceToken, expiresAt: Date.now() + 365 * 86400000 }));
    const destination=returnPath||root.getAttribute('data-rs-setup-path')||launcherPath;
    if(!destination.startsWith('/')||destination.startsWith('//')||destination.includes('\\'))throw new Error('invalid_destination');
    navigate(destination);
    }finally{busy=previousBusy;present({state,open:opened,busy});}
  }
  function choose(mode){
    if(busy||pending||credentialPending)throw new Error('request_pending');
    clearConfirmation(sessionStorageImpl);
    entryMode=mode==='sms'?'phone':mode;mode=entryMode;otpPhone=null;otpRequestId=null;opened=true;otpInput(false);
    const input=get('[data-rs-native-state="login"] [name="identifier"]');
    input.value='';input.setAttribute('autocomplete',mode==='phone'||mode==='sms'?'tel':'off');
    input.setAttribute('aria-label',mode==='pin'?'4-digit PIN':mode==='invite'?'7-digit invitation':'10-digit phone');
    input.setAttribute('placeholder',minimal?'Phone, PIN or owner invite':mode==='pin'?'4-digit PIN':mode==='invite'?'7-digit invitation':'10-digit phone');
    const button=get('[data-rs-native-action="phone-login"]');if(button)button.textContent=mode==='sms'?'Send SMS code':'Continue';
    show('login',mode==='sms'?'Optional SMS verification.':mode==='pin'?'Enter your PIN.':mode==='invite'?'Enter your active invitation.':'Enter your phone number.');
  }
  async function submitCredential(value,phone){
    if(busy||pending)return;
    if(!credentialPending){const kind=entryMode==='phone'&&/^\d{4}$/.test(value)?'pin':entryMode==='phone'&&/^\d{7}$/.test(value)?'invite':entryMode;credentialPending={action:'credential',kind,value,phone,request_id:uuid()};}
    busy=true;show('login','Checking…');
    try {
      const data=await json('action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(credentialPending)});
      credentialPending=null;resultVersion++;
      if(data.verified){deviceToken=data.device_token;persistentStorage.setItem(tokenKey,deviceToken);await finishIdentity();}
      else show('login',data.reason==='phone_required'?'This PIN needs your phone number.':data.reason==='retry_limit'?'Too many attempts. Please wait before trying again.':'No matching active identity. Check your details or use another option.');
    }catch(error){
      if(/^(invalid_|ambiguous_|conflicting_)/.test(error.message))credentialPending=null;
      show('unavailable',credentialPending?'The result is uncertain. Retry checks the same request.':'Check your details and try again.');
    }finally{busy=false;present({state,open:opened,busy});}
  }
  const controller = new AbortController();
  function feedback(message) {
    const target = get(`[data-rs-native-state="${state}"] [data-rs-native-feedback]`) || get('[data-rs-native-feedback]');
    target.textContent = message || ({ recognized: deviceConfirmed ? "This browser recognizes your profile." : "Confirm this browser with Continue before editing.", login: "Enter your phone number to sign in.", received: "SMS recovery has not been confirmed." }[state] || "");
  }
  function show(next, message = "") {
    const changed = state !== next;
    state = next; feedback(message);
    present({ state, open: opened, busy });
    if (opened && changed) get(`[data-rs-native-state="${state}"] input, [data-rs-native-state="${state}"] [data-rs-native-action]`)?.focus();
  }
  function bindPerson(value) {
    if (!value?.recognized) clearConfirmation(sessionStorageImpl);
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
      if (data.recognized === false) clearConfirmation(sessionStorageImpl);
      return data;
    } catch (error) { clearConfirmation(sessionStorageImpl); throw error; }
    finally { clearTimeout(timer); }
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
      const event = { device_token: deviceToken, session_uid: currentSessionUid(), session_event_uid: eventUid, event_type: "recognition",
        event_result: person ? "matched" : "not_matched", idempotency_key: "recognition:" + eventUid,
        recognition_status: data.recognition_status, matched_by: data.matched_by,
        person_record_id: person?.person_record_id || "", device_record_id: data.device_record_id || "" };
      try { await json("session", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(event) }); }
      catch { feedback("Recognition checked. The visit log could not be recorded."); }
    } catch (error) { bindPerson(null); show(error.message === 'authentication_required' ? 'login' : 'unavailable', error.message === 'authentication_required' ? 'Enter your phone number to sign in.' : 'Recognition could not be checked. Try again.'); }
    finally { busy = false; present({ state, open: opened, busy }); }
  }
  async function action(name, data = {}, continuation = "recognized") {
    if (busy) return;
    if (pending) { feedback("Confirm the pending request with Try again before starting another action."); return; }
    if (name === 'retire_device' || name === 'phone_login') clearConfirmation(sessionStorageImpl);
    busy = true;
    const body = { action: name, device_token: deviceToken, session_uid: currentSessionUid(),
      session_event_uid: uuid(), ...data };
    pending = { body, continuation };
    if(name==='retire_device'){
      persistentStorage.setItem(signOutKey,JSON.stringify(pending));
      clearBrowserRecognition();opened=true;busy=false;show('login','Signed out on this device.');
    }
    await executePending();
  }
  function clearBrowserRecognition(){
    clearConfirmation(sessionStorageImpl);bindPerson(null);verified=false;tenantReady=false;
    persistentStorage.removeItem('rs_recognition_identity_v1');persistentStorage.removeItem('rs_recognition_device_expires_v1');
    sessionUid=uuid();sessionStorageImpl.setItem(sessionKey,sessionUid);deviceToken=uuid();persistentStorage.setItem(tokenKey,deviceToken);
  }
  function otpInput(checking) {
    const input = get('[data-rs-native-state="login"] [name="identifier"]');
    input.value = checking ? '' : otpPhone || '';
    input.setAttribute?.('aria-label', checking ? 'SMS verification code' : 'Phone number');
    input.setAttribute?.('placeholder', checking ? 'Enter SMS code' : 'Phone number');
    input.setAttribute?.('autocomplete', checking ? 'one-time-code' : 'tel');
    input.setAttribute?.('inputmode', checking ? 'numeric' : 'tel');
    const button = get('[data-rs-native-action="phone-login"]');
    if (button) button.textContent = checking ? 'Verify code' : 'Send SMS code';
  }
  async function verifyPhone() {
    if (busy || pending) return;
    clearConfirmation(sessionStorageImpl);
    const value = values('login').identifier || '';
    const checking = otpPhone !== null;
    if (checking && !/^\d{4,10}$/.test(value)) { show('login', 'Enter the code from your SMS here.'); return; }
    busy = true; show('login', checking ? 'Checking code…' : 'Requesting SMS code…');
    try {
      const data = await json('action', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(checking ? { action: 'otp_check', sms: otpPhone, code: value, otp_request_id: otpRequestId } : { action: 'otp_start', sms: value }) });
      if (!checking) {
        if (data.otp_required !== true) throw new Error('otp_send_unconfirmed');
        otpPhone = value; otpRequestId = data.otp_request_id; otpInput(true);
        show('login', 'If this phone is registered, enter its SMS code in this browser.');
      } else if (data.verified === true && /^[a-f0-9-]{36}$/i.test(data.device_token || '')) {
        deviceToken = data.device_token; persistentStorage.setItem(tokenKey, deviceToken);
        persistentStorage.setItem('rs_recognition_device_expires_v1', String(Date.now() + 365 * 86400000));
        otpInput(false); otpPhone = null;
        await finishIdentity();
      } else if (data.expired) {
        otpInput(false); otpPhone = null; show('login', 'The code expired or was already used. Request a new SMS code.');
      } else { otpInput(true); show('login', 'That code was not accepted. Check your SMS and try again.'); }
    } catch (error) {
      otpInput(false); otpPhone = null;
      show('login', error.message === 'otp_rate_limited' ? 'Too many attempts. Wait before requesting another code.' : 'Verification could not finish. Request a new SMS code.');
    } finally { busy = false; present({ state, open: opened, busy }); }
  }
  let signOutInFlight=false;
  async function executePending() {
    if (!pending) return;
    const signingOut=pending.body.action==='retire_device';
    if(signingOut&&signOutInFlight)return;
    if(signingOut)signOutInFlight=true;
    busy = !signingOut;if(!signingOut)feedback("Saving…"); present({ state, open: opened, busy });
    try {
      const data = await json("action", { method: "POST", headers: { "Content-Type": "application/json", "X-RS-Audit-Outcome": "report" }, body: JSON.stringify(pending.body) });
      const next = pending.continuation;
      if (pending.body.action === "retire_device") {
        clearBrowserRecognition();persistentStorage.removeItem(signOutKey);pending=null;opened=true;show('login','Signed out.');return;
      } else if (data.recognized === true) bindPerson(data);
      else if (pending.body.action === "phone_login") {
        bindPerson(null);
        if (data.accepted === true) { pending = null; show("received", "If your phone is registered, check your SMS. Otherwise request an invitation, contact support, or try again."); return; }
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
        opened = false; show("recognized");
        if (silentRecognition) {
          const matched = await runSilentRecognition({ endpoint, fetchImpl, persistentStorage, sessionStorageImpl, navigate, launcherPath:returnPath||launcherPath, uuid, timeoutMs });
          if (!matched) { opened = true; show('unavailable', 'Your browser was confirmed, but recognition could not finish. Try again.'); }
        }
        else navigate(returnPath||root.getAttribute("data-rs-setup-path"));
      } else if (next === "closed") { opened = false; show("recognized"); returnFocus?.focus(); }
      else show(next, next === "received" ? "Request received. If your account is eligible, check for an SMS. Delivery is not confirmed." : "");
    } catch (error) {
      if(signingOut){opened=true;show('login','Signed out on this device. Server sign-out will retry.');return;}
      const invalid = /^(missing_|invalid_|ambiguous_|phone_already|unauthorized_|request_id_conflict|authentication_required|invitation_only|sms_recovery_unconfigured)/.test(error.message);
      if (invalid) { pending = null; show("unavailable", "Please check your details. " + error.message.replaceAll("_", " ")); }
      else show("unavailable", "The outcome could not be confirmed. Retry keeps the same request ID.");
    } finally { if(signingOut)signOutInFlight=false;busy = false; present({ state, open: opened, busy }); }
  }
  function owned() { return { person_record_id: person?.person_record_id || "", person_uid: person?.person_uid || "" }; }
  async function click(event) {
    const target = event.target.closest?.('[data-rs-native-action]');
    if (!target || !root.contains(target)) return;
    event.preventDefault(); if (busy) return;
    const command = target.getAttribute('data-rs-native-action');
    if (command === "open") { returnFocus = target; opened = true; show(state); await lookup(); }
    else if (command === "profile") { if(deviceConfirmed)navigate('/rs-barn-onboarding-setup-v24?profile=me');else choose('phone'); }
    else if (command === "login" || command === "recovery") { choose("phone"); }
    else if (command === "cancel") { otpInput(false); otpPhone = null; show(person ? "recognized" : "login"); }
    else if (command === "save") {
      if (!person || !deviceConfirmed) { show(person ? "recognized" : "login", "Use your invitation and confirm this browser before editing."); return; }
      navigate('/rs-barn-onboarding-setup-v24?profile=me');
    } else if (command === "phone-login") {await submitCredential(values('login').identifier||'',values('login').phone);}
    else if (command === "recover") choose("phone");
    else if (command === "retire") await action("retire_device", {}, "login");
    else if (command === "continue") { if (person) await action("confirm_device", owned(), "navigate"); }
    else if (command === "close") {
      if (person && deviceConfirmed) await action("confirm_device", owned(), "closed");
      else { opened = false; show(state); returnFocus?.focus(); }
    } else if (command === "retry") {
      if(credentialPending)await submitCredential();
      else if (pending) await executePending();
      else await finishIdentity();
    }
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
  async function perform(command,value){
    if(busy)throw new Error('native_busy');
    if(command.startsWith('show_')){const mode=command.slice(5);if(!['phone','pin','invite','sms'].includes(mode))throw new Error('action_not_allowed');choose(mode);}
    else if(command==='submit_credential'){if(!['phone','pin','invite'].includes(entryMode))throw new Error('action_not_allowed');await submitCredential(typeof value==='object'?value.value:value,typeof value==='object'?value.phone:undefined);}
    else if(command==='request_code'){choose('sms');get('[data-rs-native-state="login"] [name="identifier"]').value=value;await verifyPhone();}
    else if(command==='verify_code'){if(!otpPhone)throw new Error('code_not_requested');get('[data-rs-native-state="login"] [name="identifier"]').value=value;await verifyPhone();}
    else if(command==='not_you'){if(!verified)throw new Error('identity_required');await action('retire_device',{},'login');}
    else if(command==='retry'){if(credentialPending)await submitCredential();else if(pending)await executePending();else await finishIdentity();}
    else throw new Error('action_not_allowed');
    return {ok:true,state:snapshot()};
  }
  root.addEventListener('rs-recognition-command',event=>{
    const {id,action:command,value}=event.detail||{};
    if(typeof id!=='string')return;
    void perform(command,value).then(result=>root.dispatchEvent(new CustomEvent('rs-recognition-result',{detail:{id,result}})),()=>root.dispatchEvent(new CustomEvent('rs-recognition-result',{detail:{id,error:'native_action_unavailable'}})));
  },{signal:controller.signal});
  const api={lookup,snapshot,perform,destroy(){controller.abort();delete root.__rsNativeRecognition;root.removeAttribute('data-rs-recognition-state');}};
  root.__rsNativeRecognition=api;
  bindPerson(null);show('unavailable');
  let savedSignOut;try{savedSignOut=JSON.parse(persistentStorage.getItem(signOutKey)||'null');}catch{}
  globalThis.addEventListener?.('online',()=>{if(pending?.body.action==='retire_device')void executePending();},{signal:controller.signal});
  if(savedSignOut?.body?.action==='retire_device'){
    pending=savedSignOut;clearBrowserRecognition();opened=true;show('login','Signed out on this device.');void executePending();
  }
  else if(signOut){
    const search=new URLSearchParams(locationImpl?.search||'');search.delete('sign_out');
    historyImpl.replaceState(historyImpl.state,'',locationImpl.pathname+(search.size?'?'+search.toString():'')+(locationImpl.hash||''));
    opened=true;void action('retire_device',{},'login');
  }
  else if(invitation!==null)void acceptInvitation();
  else if(silentRecognition){
    busy=true;present({state,open:opened,busy});
    let outcome;
    void runSilentRecognition({endpoint,fetchImpl,persistentStorage,sessionStorageImpl,navigate,launcherPath,redirectOnRecognized:false,uuid,timeoutMs,onOutcome:v=>{outcome=v;}}).then(async matched=>{
      deviceToken=persistentStorage.getItem(tokenKey)||deviceToken;busy=false;
      if(matched){await finishIdentity(outcome?.data);}
      else if(outcome?.outcome==='unknown'){choose('phone');}
      else {opened=true;show('unavailable','Recognition could not be checked. Try again.');}
    }).catch(()=>{busy=false;show('unavailable','Recognition could not be checked. Try again.');});
  }else void lookup();
  return api;
}
