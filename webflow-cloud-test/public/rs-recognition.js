(function () {
  "use strict";

  const script = document.currentScript;
  const apiBase = (script && script.dataset.rsApiBase) || "https://ringstatus.webflow.io/test/rs-recognition";
  const previewMode = Boolean(script && script.dataset.rsPreview === "true");
  const tokenKey = "rs_device_token";
  const sessionKey = "rs_recognition_session_uid";
  const path = window.location.pathname.replace(/\/+$/, "") || "/";
  const query = new URLSearchParams(window.location.search);
  const isHome = path === "/";
  const isMembers = path === "/members";
  let activeState = "quiet";
  let currentPerson = null;

  const previewIdentity = {
    person_record_id: "recxMolAW8UhI3Hph",
    person_uid: "63187",
    person_name: "Lainey",
    first_name: "Lainey",
    last_name: "Posa",
    primary_phone_e164: "+16318752160",
    email: "",
    device_record_id: "rec0OtWNkYWs7iGgk",
    matched_by: "device_token",
    recognition_status: "known_device"
  };

  const style = document.createElement("style");
  style.id = "rs-recognition-style";
  style.textContent = `
    #rs-recognition-test{--ink:#142236;--muted:#667080;--line:#dfe3e8;--paper:#fff;--navy:#10243c;--accent:#98492b;position:fixed;right:22px;bottom:22px;z-index:2147483000;width:min(392px,calc(100vw - 44px));font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;pointer-events:none}
    #rs-recognition-test *{box-sizing:border-box}
    #rs-recognition-card{overflow:hidden;border:1px solid rgba(20,34,54,.12);border-radius:18px;background:var(--paper);box-shadow:0 24px 70px rgba(20,34,54,.2),0 4px 16px rgba(20,34,54,.08);opacity:0;pointer-events:none;transform:translateY(28px) scale(.985);transform-origin:bottom right;transition:opacity 180ms ease,transform 220ms cubic-bezier(.2,.75,.25,1)}
    #rs-recognition-test.is-open #rs-recognition-card{opacity:1;pointer-events:auto;transform:translateY(0) scale(1)}
    #rs-recognition-test .rs-card-head{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;padding:22px 22px 8px}
    #rs-recognition-test .rs-card-eyebrow{margin:0 0 7px;color:var(--accent);font-size:10px;font-weight:800;letter-spacing:.15em;text-transform:uppercase}
    #rs-recognition-test .rs-card-title{margin:0;color:var(--ink);font:400 29px/1.08 Georgia,"Times New Roman",serif;letter-spacing:-.025em}
    #rs-recognition-close{width:38px;height:38px;margin:-8px -8px 0 0;border:0;border-radius:50%;color:var(--muted);background:transparent;cursor:pointer;font-size:24px;line-height:1}
    #rs-recognition-close:hover,#rs-recognition-close:focus-visible{color:var(--ink);background:#f3f4f5}
    #rs-recognition-test .rs-card-body{padding:10px 22px 22px}
    #rs-recognition-test .rs-card-copy{margin:0 0 17px;color:var(--muted);font-size:14px;line-height:1.55}
    #rs-recognition-test .rs-link-row{display:flex;flex-wrap:wrap;gap:10px 18px;padding-top:7px}
    #rs-recognition-test .rs-text-link{padding:0;border:0;color:var(--accent);background:transparent;cursor:pointer;font-size:13px;font-weight:700;text-align:left;text-decoration:underline;text-underline-offset:3px}
    #rs-recognition-test .rs-form-grid{display:grid;grid-template-columns:1fr 1fr;gap:13px}
    #rs-recognition-test .rs-field-wide{grid-column:1/-1}
    #rs-recognition-test .rs-field label{display:block;margin:0 0 6px;color:var(--ink);font-size:12px;font-weight:700}
    #rs-recognition-test .rs-required{color:var(--accent)}
    #rs-recognition-test .rs-field input{width:100%;min-height:44px;padding:10px 12px;border:1px solid #cfd5dc;border-radius:10px;color:var(--ink);background:#fff;outline:none;font:inherit}
    #rs-recognition-test .rs-field input:focus{border-color:#b85f38;box-shadow:0 0 0 3px rgba(184,95,56,.13)}
    #rs-recognition-test .rs-submit{width:100%;min-height:46px;margin-top:16px;border:0;border-radius:11px;color:#fff;background:var(--navy);cursor:pointer;font-size:14px;font-weight:750}
    #rs-recognition-test .rs-submit:disabled{cursor:wait;opacity:.62}
    #rs-recognition-test .rs-form-status{min-height:19px;margin:10px 0 0;color:var(--muted);font-size:12px;line-height:1.45}
    #rs-recognition-test .rs-form-status.is-error{color:#a63838}
    #rs-recognition-test .rs-form-status.is-success{color:#276748}
    #rs-recognition-test .rs-contact-copy{margin:18px 0 0;padding-top:16px;border-top:1px solid var(--line);color:var(--muted);font-size:12px;line-height:1.55}
    #rs-recognition-test .rs-contact-copy a{color:var(--accent);font-weight:700}
    #rs-recognition-test [hidden]{display:none!important}
    @media(max-width:720px){#rs-recognition-test{right:12px;bottom:12px;width:calc(100vw - 24px)}#rs-recognition-test .rs-form-grid{grid-template-columns:1fr}#rs-recognition-test .rs-field-wide{grid-column:auto}}
    @media(prefers-reduced-motion:reduce){#rs-recognition-card{transition:none}}
  `;
  document.head.appendChild(style);

  const root = document.createElement("div");
  root.id = "rs-recognition-test";
  root.setAttribute("aria-live", "polite");
  root.innerHTML = `
    <section id="rs-recognition-card" role="dialog" aria-modal="false" aria-labelledby="rs-card-title">
      <header class="rs-card-head">
        <div><p class="rs-card-eyebrow" id="rs-card-eyebrow">Welcome back</p><h2 class="rs-card-title" id="rs-card-title">Hi</h2></div>
        <button id="rs-recognition-close" type="button" aria-label="Close">×</button>
      </header>
      <div class="rs-card-body">
        <section id="rs-recognized-view"><p class="rs-card-copy">We recognized this device.</p><div class="rs-link-row"><button type="button" class="rs-text-link" id="rs-not-you">Not you?</button><button type="button" class="rs-text-link" id="rs-update-details">Update my details</button></div></section>
        <form id="rs-profile-form" hidden novalidate>${profileFields("profile")}<button class="rs-submit" type="submit">Save</button><p class="rs-form-status" id="rs-profile-status" aria-live="polite"></p></form>
        <form id="rs-new-profile-form" hidden novalidate>${profileFields("new")}<button class="rs-submit" type="submit">Save</button><p class="rs-form-status" id="rs-new-profile-status" aria-live="polite"></p></form>
        <form id="rs-members-login-form" hidden novalidate><p class="rs-card-copy">Enter the SMS number on your member profile.</p><div class="rs-field"><label for="rs-login-sms">SMS <span class="rs-required">*</span></label><input id="rs-login-sms" name="sms" required inputmode="tel" autocomplete="tel" placeholder="+1 phone"></div><button class="rs-submit" type="submit">Submit</button><p class="rs-form-status" id="rs-login-status" aria-live="polite"></p></form>
        <form id="rs-recovery-form" hidden novalidate><p class="rs-card-copy">We didn’t recognize your phone. Try your full name or email. If we find it, we’ll send your member link to the email we have on file.</p><div class="rs-form-grid"><div class="rs-field"><label for="rs-recovery-first">First</label><input id="rs-recovery-first" name="first" autocomplete="given-name"></div><div class="rs-field"><label for="rs-recovery-last">Last</label><input id="rs-recovery-last" name="last" autocomplete="family-name"></div><div class="rs-field rs-field-wide"><label for="rs-recovery-email">Email</label><input id="rs-recovery-email" name="email" type="email" autocomplete="email"></div></div><button class="rs-submit" type="submit">Send</button><p class="rs-form-status" id="rs-recovery-status" aria-live="polite"></p><p class="rs-contact-copy">If your barn or trainer is not already a member, ask them to <a id="rs-demo-contact" href="/contact">contact me</a> and we can create a demo account for your full barn.</p></form>
      </div>
    </section>`;
  document.body.appendChild(root);

  const title = document.getElementById("rs-card-title");
  const eyebrow = document.getElementById("rs-card-eyebrow");
  const recognizedView = document.getElementById("rs-recognized-view");
  const profileForm = document.getElementById("rs-profile-form");
  const newProfileForm = document.getElementById("rs-new-profile-form");
  const loginForm = document.getElementById("rs-members-login-form");
  const recoveryForm = document.getElementById("rs-recovery-form");
  const views = [recognizedView, profileForm, newProfileForm, loginForm, recoveryForm];

  function profileFields(prefix) {
    const placeholder = prefix === "new";
    return `<div class="rs-form-grid">
      <div class="rs-field rs-field-wide"><label for="rs-${prefix}-user">User <span class="rs-required">*</span></label><input id="rs-${prefix}-user" name="user" required autocomplete="nickname"${placeholder ? ' placeholder="User"' : ""}></div>
      <div class="rs-field"><label for="rs-${prefix}-first">First</label><input id="rs-${prefix}-first" name="first" autocomplete="given-name"${placeholder ? ' placeholder="First"' : ""}></div>
      <div class="rs-field"><label for="rs-${prefix}-last">Last</label><input id="rs-${prefix}-last" name="last" autocomplete="family-name"${placeholder ? ' placeholder="Last"' : ""}></div>
      <div class="rs-field rs-field-wide"><label for="rs-${prefix}-sms">SMS <span class="rs-required">*</span></label><input id="rs-${prefix}-sms" name="sms" required inputmode="tel" autocomplete="tel" placeholder="+1 phone"></div>
      <div class="rs-field rs-field-wide"><label for="rs-${prefix}-email">Email</label><input id="rs-${prefix}-email" name="email" type="email" autocomplete="email" placeholder="name@example.com"></div>
    </div>`;
  }

  function uid(prefix) {
    const random = window.crypto && typeof window.crypto.randomUUID === "function"
      ? window.crypto.randomUUID().replace(/-/g, "")
      : Date.now().toString(36) + Math.random().toString(36).slice(2);
    return `${prefix}_${random}`;
  }

  function sessionUid() {
    let value = sessionStorage.getItem(sessionKey);
    if (!value) { value = uid("session"); sessionStorage.setItem(sessionKey, value); }
    return value;
  }

  function deviceToken(create) {
    let value = localStorage.getItem(tokenKey);
    if (!value && create) { value = uid("device_token"); persistDeviceToken(value); }
    return value || "";
  }

  function persistDeviceToken(value) {
    localStorage.setItem(tokenKey, value);
    document.cookie = `${tokenKey}=${encodeURIComponent(value)}; Max-Age=31536000; Path=/; SameSite=Lax; Secure`;
  }

  function clearIdentity() {
    localStorage.removeItem(tokenKey);
    document.cookie = `${tokenKey}=; Max-Age=0; Path=/; SameSite=Lax; Secure`;
    currentPerson = null;
  }

  function context() {
    return {
      client_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "",
      viewport_width: window.innerWidth,
      page_path: window.location.pathname,
      referrer: document.referrer
    };
  }

  async function recordSessionEvent(event) {
    const sessionEventUid = uid("session_event");
    const payload = {
      session_event_uid: sessionEventUid,
      session_uid: sessionUid(),
      event_type: event.event_type,
      event_result: event.event_result,
      idempotency_key: `${event.event_type}:${sessionEventUid}`,
      person_record_id: event.person_record_id || "",
      device_record_id: event.device_record_id || "",
      matched_by: event.matched_by || "unknown",
      recognition_status: event.recognition_status || "pending",
      detail: event.detail || { source: "recognition_widget" },
      ...context()
    };
    const response = await fetch(`${apiBase}/session`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    if (!response.ok) throw new Error("session_event_failed");
  }

  async function identityAction(specification) {
    const payload = {
      action: specification.action,
      session_uid: sessionUid(),
      session_event_uid: uid("session_event"),
      device_token: deviceToken(specification.action !== "retire_device"),
      ...context(),
      ...(specification.data || {})
    };
    const response = await fetch(`${apiBase}/identity`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    const result = await response.json().catch(function () { return {}; });
    if (!response.ok || !result.ok) {
      const error = new Error(result.error || "identity_action_failed");
      error.code = result.error || "identity_action_failed";
      throw error;
    }
    return result;
  }

  function showOnly(view) { views.forEach(function (item) { item.hidden = item !== view; }); }
  function openCard() { root.classList.add("is-open"); }
  function closeCard() { root.classList.remove("is-open"); }
  function setStatus(element, message, kind) { element.textContent = message || ""; element.className = `rs-form-status${kind ? ` is-${kind}` : ""}`; }
  function setBusy(form, busy) { const button = form.querySelector('[type="submit"]'); if (button) button.disabled = busy; }

  function showRecognized(person) {
    activeState = "recognized";
    currentPerson = person;
    eyebrow.textContent = "Welcome back";
    title.textContent = `Hi ${person.person_name || person.first_name || "there"}`;
    showOnly(recognizedView);
    openCard();
  }

  function showProfile() {
    activeState = "profile";
    eyebrow.textContent = "Member details";
    title.textContent = "Profile";
    profileForm.elements.user.value = currentPerson?.person_name || "";
    profileForm.elements.first.value = currentPerson?.first_name || "";
    profileForm.elements.last.value = currentPerson?.last_name || "";
    profileForm.elements.sms.value = currentPerson?.primary_phone_e164 || "";
    profileForm.elements.email.value = currentPerson?.email || "";
    showOnly(profileForm);
    openCard();
  }

  function showNewProfile() {
    activeState = "new";
    currentPerson = null;
    eyebrow.textContent = "Start fresh";
    title.textContent = "New Profile";
    newProfileForm.reset();
    showOnly(newProfileForm);
    openCard();
  }

  function showMembersLogin() {
    activeState = "login";
    currentPerson = null;
    eyebrow.textContent = "Members";
    title.textContent = "Login";
    loginForm.reset();
    showOnly(loginForm);
    openCard();
  }

  function showRecovery() {
    activeState = "recovery";
    eyebrow.textContent = "Member lookup";
    title.textContent = "Let’s find you";
    recoveryForm.reset();
    showOnly(recoveryForm);
    openCard();
  }

  function showQuiet() { activeState = "quiet"; currentPerson = null; closeCard(); }
  function redirectToMembers(personUid) { window.location.assign(`/members?user=${encodeURIComponent(personUid)}`); }
  function errorMessage(error) {
    if (error.code === "phone_already_registered") return "That SMS number already belongs to a profile. Use Members login.";
    if (error.code === "invalid_email") return "Enter a valid email address.";
    return "We couldn’t complete that request. Please try again.";
  }

  document.getElementById("rs-update-details").addEventListener("click", showProfile);
  document.getElementById("rs-not-you").addEventListener("click", async function () {
    try { await identityAction({ action: "retire_device", data: {} }); } catch (_) { /* local identity is still cleared */ }
    clearIdentity();
    showNewProfile();
  });

  document.getElementById("rs-recognition-close").addEventListener("click", function () {
    if ((activeState === "recognized" || activeState === "profile") && currentPerson) {
      const token = deviceToken(true);
      persistDeviceToken(token);
      identityAction({ action: "confirm_device", data: { person_record_id: currentPerson.person_record_id, person_uid: currentPerson.person_uid, source: "card_close" } }).catch(function () {});
      closeCard();
    } else if (isMembers) {
      window.location.assign("/");
    } else {
      closeCard();
    }
  });

  profileForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    const status = document.getElementById("rs-profile-status");
    setBusy(profileForm, true); setStatus(status, "Saving…");
    try {
      const data = Object.fromEntries(new FormData(profileForm));
      const result = await identityAction({ action: "update_profile", data: { ...data, person_record_id: currentPerson.person_record_id, person_uid: currentPerson.person_uid } });
      persistDeviceToken(deviceToken(true));
      currentPerson = result;
      redirectToMembers(result.person_uid);
    } catch (error) { setStatus(status, errorMessage(error), "error"); setBusy(profileForm, false); }
  });

  newProfileForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    const status = document.getElementById("rs-new-profile-status");
    setBusy(newProfileForm, true); setStatus(status, "Creating profile…");
    try {
      const data = Object.fromEntries(new FormData(newProfileForm));
      const result = await identityAction({ action: "create_profile", data });
      persistDeviceToken(deviceToken(true));
      currentPerson = result;
      redirectToMembers(result.person_uid);
    } catch (error) { setStatus(status, errorMessage(error), "error"); setBusy(newProfileForm, false); }
  });

  loginForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    const status = document.getElementById("rs-login-status");
    setBusy(loginForm, true); setStatus(status, "Checking…");
    try {
      const data = Object.fromEntries(new FormData(loginForm));
      const result = await identityAction({ action: "phone_login", data });
      if (!result.recognized) { setBusy(loginForm, false); showRecovery(); return; }
      persistDeviceToken(deviceToken(true));
      currentPerson = result;
      redirectToMembers(result.person_uid);
    } catch (error) { setStatus(status, errorMessage(error), "error"); setBusy(loginForm, false); }
  });

  recoveryForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    const status = document.getElementById("rs-recovery-status");
    setBusy(recoveryForm, true); setStatus(status, "Sending…");
    try {
      const data = Object.fromEntries(new FormData(recoveryForm));
      await identityAction({ action: "recovery", data });
      setStatus(status, "If we found your profile, we sent your member link to the email on file.", "success");
      window.setTimeout(function () { window.location.assign("/"); }, 1200);
    } catch (error) { setStatus(status, errorMessage(error), "error"); setBusy(recoveryForm, false); }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && root.classList.contains("is-open")) document.getElementById("rs-recognition-close").click();
  });

  async function recognize() {
    if (isMembers && query.get("user")) return;
    if (!isHome && !isMembers && !previewMode) return;
    if (previewMode) { showRecognized(previewIdentity); return; }
    const token = deviceToken(false);
    if (!token) {
      recordSessionEvent({ event_type: "recognition", event_result: "not_matched", matched_by: "unknown", recognition_status: "pending", detail: { reason: "missing_device_token" } }).catch(function () {});
      if (isMembers) showMembersLogin(); else showQuiet();
      return;
    }
    try {
      const response = await fetch(`${apiBase}/device?device_token=${encodeURIComponent(token)}`);
      const result = await response.json().catch(function () { return {}; });
      if (!response.ok || !result.recognized) {
        await recordSessionEvent({ event_type: "recognition", event_result: "not_matched", matched_by: result.matched_by || "unknown", recognition_status: result.recognition_status || "rejected", device_record_id: result.device_record_id || "" });
        if (isMembers) showMembersLogin(); else showQuiet();
        return;
      }
      currentPerson = result;
      await recordSessionEvent({ event_type: "recognition", event_result: "matched", matched_by: result.matched_by, recognition_status: result.recognition_status, person_record_id: result.person_record_id, device_record_id: result.device_record_id });
      if (isMembers) redirectToMembers(result.person_uid); else showRecognized(result);
    } catch (_) {
      if (isMembers) showMembersLogin(); else showQuiet();
    }
  }

  window.RSRecognition = {
    preview: function (state) {
      if (!previewMode) return;
      if (state === "recognized") showRecognized(previewIdentity);
      else if (state === "members") showMembersLogin();
      else showQuiet();
    }
  };
  document.querySelectorAll("[data-rs-preview-state]").forEach(function (button) {
    button.addEventListener("click", function () { window.RSRecognition.preview(button.dataset.rsPreviewState); });
  });

  recognize();
})();
