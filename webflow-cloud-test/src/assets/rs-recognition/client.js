(function () {
  if (document.getElementById("rs-recognition-test")) return;

  const root = document.createElement("div");
  root.id = "rs-recognition-test";
  root.innerHTML = `
    <style>
      #rs-recognition-test {
        --rs-ink: #142236;
        --rs-muted: #667080;
        --rs-line: #dfe3e8;
        --rs-accent: #98492b;
        position: fixed;
        right: 22px;
        bottom: 22px;
        z-index: 9999;
        width: min(390px, calc(100vw - 44px));
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        pointer-events: none;
      }

      #rs-recognition-test * { box-sizing: border-box; }

      #rs-access-ringstatus,
      #rs-login-ringstatus,
      #rs-request-demo { display: none !important; }

      #rs-access-ringstatus.is-active,
      #rs-login-ringstatus.is-active,
      #rs-request-demo.is-active { display: flex !important; }

      #rs-recognition-card {
        overflow: hidden;
        border: 1px solid rgba(20, 34, 54, 0.12);
        border-radius: 18px;
        background: #fff;
        box-shadow: 0 24px 70px rgba(20, 34, 54, 0.20), 0 4px 16px rgba(20, 34, 54, 0.08);
        opacity: 0;
        pointer-events: none;
        transform: translateY(26px) scale(0.985);
        transition: opacity 180ms ease, transform 220ms cubic-bezier(.2,.75,.25,1);
      }

      #rs-recognition-test.is-open #rs-recognition-card {
        opacity: 1;
        pointer-events: auto;
        transform: translateY(0) scale(1);
      }

      #rs-recognition-test .rs-card-head {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 18px;
        padding: 22px 22px 8px;
      }

      #rs-recognition-test .rs-eyebrow {
        margin: 0 0 7px;
        color: var(--rs-accent);
        font-size: 10px;
        font-weight: 800;
        letter-spacing: 0.15em;
        text-transform: uppercase;
      }

      #rs-recognition-test h2 {
        margin: 0;
        color: var(--rs-ink);
        font: 400 29px/1.08 Georgia, "Times New Roman", serif;
        letter-spacing: -0.025em;
      }

      #rs-recognition-close {
        width: 38px;
        height: 38px;
        margin: -8px -8px 0 0;
        border: 0;
        border-radius: 50%;
        color: var(--rs-muted);
        background: transparent;
        cursor: pointer;
        font-size: 24px;
        line-height: 1;
      }

      #rs-recognition-close:hover,
      #rs-recognition-close:focus-visible { color: var(--rs-ink); background: #f3f4f5; }

      #rs-recognition-test .rs-card-body { padding: 10px 22px 22px; }

      #rs-recognition-test .rs-copy {
        margin: 0 0 17px;
        color: var(--rs-muted);
        font-size: 14px;
        line-height: 1.55;
      }

      #rs-recognition-test .rs-link-row { display: flex; flex-wrap: wrap; gap: 10px 18px; }

      #rs-recognition-test .rs-text-link {
        padding: 0;
        border: 0;
        color: var(--rs-accent);
        background: transparent;
        cursor: pointer;
        font-size: 13px;
        font-weight: 700;
        text-decoration: underline;
        text-underline-offset: 3px;
      }

      #rs-recognition-test .rs-form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 13px; }
      #rs-recognition-test .rs-wide { grid-column: 1 / -1; }

      #rs-recognition-test .rs-single-field {
        padding: 14px;
        border: 1px solid var(--rs-line);
        border-radius: 12px;
        background: #f7f8fa;
      }

      #rs-recognition-test label {
        display: block;
        margin: 0 0 6px;
        color: var(--rs-ink);
        font-size: 12px;
        font-weight: 700;
      }

      #rs-recognition-test .rs-required { color: var(--rs-accent); }

      #rs-recognition-test input {
        width: 100%;
        min-height: 44px;
        padding: 10px 12px;
        border: 1px solid #cfd5dc;
        border-radius: 10px;
        color: var(--rs-ink);
        background: #fff;
        outline: none;
        font: inherit;
      }

      #rs-recognition-test input:focus {
        border-color: #b85f38;
        box-shadow: 0 0 0 3px rgba(184, 95, 56, 0.13);
      }

      #rs-recognition-test .rs-save {
        width: 100%;
        min-height: 46px;
        margin-top: 16px;
        border: 0;
        border-radius: 11px;
        color: #fff;
        background: #10243c;
        cursor: pointer;
        font-size: 14px;
        font-weight: 750;
      }

      #rs-recognition-test .rs-save:disabled { cursor: wait; opacity: 0.65; }

      #rs-recognition-test .rs-status {
        min-height: 18px;
        margin: 9px 0 0;
        color: #276748;
        font-size: 12px;
      }

      #rs-recognition-test .rs-demo-callout {
        margin-top: 18px;
        padding-top: 16px;
        border-top: 1px solid var(--rs-line);
      }

      #rs-recognition-test .rs-demo-title {
        margin: 0 0 5px;
        color: var(--rs-ink);
        font-size: 13px;
        font-weight: 800;
      }

      #rs-recognition-test .rs-demo-callout .rs-copy { margin-bottom: 0; }
      #rs-recognition-test [hidden] { display: none !important; }

      @media (max-width: 620px) {
        #rs-recognition-test { right: 12px; bottom: 12px; width: calc(100vw - 24px); }
        #rs-recognition-test .rs-form-grid { grid-template-columns: 1fr; }
        #rs-recognition-test .rs-wide { grid-column: auto; }
      }

      @media (prefers-reduced-motion: reduce) {
        #rs-recognition-card { transition: none; }
      }
    </style>

    <section id="rs-recognition-card" role="dialog" aria-modal="false" aria-labelledby="rs-card-title">
      <header class="rs-card-head">
        <div>
          <p class="rs-eyebrow" id="rs-card-eyebrow">Welcome back</p>
          <h2 id="rs-card-title"></h2>
        </div>
        <button id="rs-recognition-close" type="button" aria-label="Close">×</button>
      </header>

      <div class="rs-card-body">
        <section id="rs-recognized-view">
          <div class="rs-link-row">
            <button type="button" class="rs-text-link" id="rs-not-you">Not you?</button>
            <button type="button" class="rs-text-link" id="rs-update-details">Update my details</button>
          </div>
        </section>

        <form id="rs-profile-form" hidden>
          <div class="rs-form-grid">
            <div class="rs-wide"><label for="rs-profile-user">User <span class="rs-required">*</span></label><input id="rs-profile-user" name="user" required></div>
            <div><label for="rs-profile-first">First</label><input id="rs-profile-first" name="first"></div>
            <div><label for="rs-profile-last">Last</label><input id="rs-profile-last" name="last"></div>
            <div class="rs-wide"><label for="rs-profile-sms">SMS <span class="rs-required">*</span></label><input id="rs-profile-sms" name="sms" required></div>
            <div class="rs-wide"><label for="rs-profile-pin">PIN</label><input id="rs-profile-pin" name="pin" maxlength="4" pattern="[0-9]{4}" inputmode="numeric" autocomplete="off"></div>
            <div class="rs-wide"><label for="rs-profile-email">Email</label><input id="rs-profile-email" name="email" type="email"></div>
          </div>
          <button class="rs-save" type="submit">Save</button>
          <p class="rs-status" id="rs-profile-status" aria-live="polite"></p>
        </form>

        <form id="rs-members-login-form" hidden>
          <p class="rs-copy">Add your SMS number or PIN.</p>
          <div class="rs-single-field"><label for="rs-login-sms">SMS number or PIN <span class="rs-required">*</span></label><input id="rs-login-sms" name="sms" inputmode="tel" autocomplete="tel" required placeholder="Phone number or 4-digit PIN"></div>
          <button class="rs-save" type="submit">Continue</button>
          <p class="rs-status" id="rs-login-status" aria-live="polite"></p>
        </form>

        <form id="rs-recovery-form" hidden>
          <p class="rs-copy">We did not recognize this device. Search by your full name or email. If we find you, we will send your member link to the email on file.</p>
          <div class="rs-form-grid">
            <div><label for="rs-recovery-first">First name</label><input id="rs-recovery-first" name="first" autocomplete="given-name"></div>
            <div><label for="rs-recovery-last">Last name</label><input id="rs-recovery-last" name="last" autocomplete="family-name"></div>
            <div class="rs-wide"><label for="rs-recovery-email">Email</label><input id="rs-recovery-email" name="email" type="email" autocomplete="email"></div>
          </div>
          <button class="rs-save" type="submit">Save</button>
          <p class="rs-status" id="rs-recovery-status" aria-live="polite"></p>
          <aside class="rs-demo-callout" aria-label="Schedule a demo">
            <p class="rs-demo-title">Schedule a demo</p>
            <p class="rs-copy">If your barn or trainer is not already a member, ask them to <a class="rs-text-link" href="/contact">Contact Me</a> and we can create a demo account for your full barn.</p>
          </aside>
        </form>
      </div>
    </section>`;

  document.body.appendChild(root);

  const tokenKey = "rs_device_token";
  const sessionKey = "rs_recognition_session_uid";
  const recognitionEndpoint = "https://ringstatus.webflow.io/test/rs-recognition/device";
  const sessionEndpoint = "https://ringstatus.webflow.io/test/rs-recognition/session";
  const actionEndpoint = "https://ringstatus.webflow.io/test/rs-recognition/action";
  const preview = window.location.protocol === "file:";
  const previewView = window.location.search.includes("view=login")
    ? "login"
    : window.location.search.includes("view=recovery") ? "recovery" : "recognized";
  const path = window.location.pathname.replace(/\/+$/, "") || "/";
  const homePath = path === "/" || path === "/ks2";
  const memberPath = path === "/members";
  const title = document.getElementById("rs-card-title");
  const eyebrow = document.getElementById("rs-card-eyebrow");
  const recognized = document.getElementById("rs-recognized-view");
  const profile = document.getElementById("rs-profile-form");
  const login = document.getElementById("rs-members-login-form");
  const recovery = document.getElementById("rs-recovery-form");
  const views = [recognized, profile, login, recovery];
  const entryButtons = [
    { element: document.getElementById("rs-access-ringstatus"), activeWhen: "recognized" },
    { element: document.getElementById("rs-login-ringstatus"), activeWhen: "unrecognized" },
    { element: document.getElementById("rs-request-demo"), activeWhen: "unrecognized" }
  ];
  let activeView = "quiet";
  let currentPerson = null;

  function uid(prefix) {
    const random = window.crypto && typeof window.crypto.randomUUID === "function"
      ? window.crypto.randomUUID().replace(/-/g, "")
      : Date.now().toString(36) + Math.random().toString(36).slice(2);
    return prefix + "_" + random;
  }

  function getSessionUid() {
    let value = sessionStorage.getItem(sessionKey);
    if (!value) {
      value = uid("session");
      sessionStorage.setItem(sessionKey, value);
    }
    return value;
  }

  function getDeviceToken(create) {
    let value = localStorage.getItem(tokenKey);
    if (!value && create) {
      value = uid("device_token");
      setDeviceToken(value);
    }
    return value || "";
  }

  function setDeviceToken(value) {
    localStorage.setItem(tokenKey, value);
    document.cookie = tokenKey + "=" + encodeURIComponent(value) + "; Max-Age=31536000; Path=/; SameSite=Lax; Secure";
  }

  function clearDeviceToken() {
    localStorage.removeItem(tokenKey);
    document.cookie = tokenKey + "=; Max-Age=0; Path=/; SameSite=Lax; Secure";
  }

  function requestContext() {
    return {
      client_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "",
      viewport_width: window.innerWidth,
      page_path: window.location.pathname,
      referrer: document.referrer
    };
  }

  function showOnly(view) {
    views.forEach(function (item) {
      item.hidden = item !== view;
    });
  }

  function setEntryButtons(state) {
    entryButtons.forEach(function (item) {
      if (item.element) item.element.classList.toggle("is-active", item.activeWhen === state);
    });
  }

  function showProfile() {
    activeView = "profile";
    setEntryButtons("recognized");
    eyebrow.textContent = "Member details";
    title.textContent = "Profile";
    profile.elements.user.value = currentPerson.person_name || "";
    profile.elements.first.value = currentPerson.first_name || "";
    profile.elements.last.value = currentPerson.last_name || "";
    profile.elements.sms.value = phoneForDisplay(currentPerson.primary_phone_e164);
    profile.elements.pin.value = currentPerson.member_pin || phoneForDisplay(currentPerson.primary_phone_e164).slice(-4);
    profile.elements.email.value = currentPerson.email || "";
    showOnly(profile);
    root.classList.add("is-open");
  }

  function showRecognized(person) {
    activeView = "recognized";
    currentPerson = person;
    setEntryButtons("recognized");
    eyebrow.textContent = "Welcome back";
    title.textContent = "Hi " + (person.person_name || person.first_name || "there");
    showOnly(recognized);
    root.classList.add("is-open");
  }

  function showLogin() {
    activeView = "login";
    currentPerson = null;
    setEntryButtons("unrecognized");
    eyebrow.textContent = "Members";
    title.textContent = "Login";
    login.reset();
    showOnly(login);
    root.classList.add("is-open");
  }

  function showRecovery() {
    activeView = "recovery";
    setEntryButtons("unrecognized");
    eyebrow.textContent = "Member lookup";
    title.textContent = "Let’s find you";
    recovery.reset();
    showOnly(recovery);
    root.classList.add("is-open");
  }

  function status(formId, message, error) {
    const element = document.getElementById(formId);
    element.textContent = message || "";
    element.style.color = error ? "#a63838" : "#276748";
  }

  function setBusy(form, busy) {
    form.querySelector('[type="submit"]').disabled = busy;
  }

  async function recordSession(event) {
    const eventUid = uid("session_event");
    const payload = Object.assign({
      session_event_uid: eventUid,
      session_uid: getSessionUid(),
      event_type: event.event_type,
      event_result: event.event_result,
      idempotency_key: event.event_type + ":" + eventUid,
      person_record_id: event.person_record_id || "",
      device_record_id: event.device_record_id || "",
      matched_by: event.matched_by || "unknown",
      recognition_status: event.recognition_status || "pending",
      detail: event.detail || { source: "recognition_card" }
    }, requestContext());
    const response = await fetch(sessionEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    if (!response.ok) throw new Error("session_event_failed");
  }

  async function callAction(specification) {
    const payload = Object.assign({
      action: specification.action,
      session_uid: getSessionUid(),
      session_event_uid: uid("session_event"),
      device_token: getDeviceToken(specification.action !== "retire_device")
    }, requestContext(), specification.data || {});
    const response = await fetch(actionEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const result = await response.json().catch(function () { return {}; });
    if (!response.ok || !result.ok) {
      const error = new Error(result.error || "action_failed");
      error.code = result.error || "action_failed";
      throw error;
    }
    return result;
  }

  function errorText(error) {
    if (error.code === "phone_already_registered") return "That SMS number already belongs to a profile. Use Members login.";
    if (error.code === "invalid_email") return "Enter a valid email address.";
    if (error.code === "invalid_pin") return "Enter a 4-digit PIN.";
    if (error.code === "missing_recovery_identity") return "Enter your full name or email.";
    return "We could not complete that request. Please try again.";
  }

  function phoneForDisplay(value) {
    const digits = String(value || "").replace(/\D/g, "");
    return digits.length === 11 && digits.charAt(0) === "1" ? digits.slice(1) : digits;
  }

  function redirectToMembers(personUid, statusId) {
    const target = "/members?user=" + encodeURIComponent(personUid);
    if (preview) {
      if (statusId) status(statusId, "Ready: " + target, false);
      return;
    }
    window.location.assign(target);
  }

  async function recognize() {
    if (memberPath && new URLSearchParams(window.location.search).get("user")) {
      setEntryButtons("recognized");
      return;
    }
    if (preview) {
      if (previewView === "login") showLogin();
      else if (previewView === "recovery") showRecovery();
      else showRecognized({});
      return;
    }
    if (!homePath && !memberPath) return;
    const token = getDeviceToken(false);
    if (!token) {
      setEntryButtons("unrecognized");
      recordSession({ event_type: "recognition", event_result: "not_matched", detail: { reason: "missing_device_token" } }).catch(function () {});
      if (memberPath) showLogin();
      return;
    }
    try {
      const response = await fetch(recognitionEndpoint + "?device_token=" + encodeURIComponent(token));
      const result = await response.json().catch(function () { return {}; });
      if (!response.ok || !result.recognized) {
        setEntryButtons("unrecognized");
        await recordSession({ event_type: "recognition", event_result: "not_matched", matched_by: result.matched_by, recognition_status: result.recognition_status, device_record_id: result.device_record_id });
        if (memberPath) showLogin();
        return;
      }
      await recordSession({ event_type: "recognition", event_result: "matched", matched_by: result.matched_by, recognition_status: result.recognition_status, person_record_id: result.person_record_id, device_record_id: result.device_record_id });
      setEntryButtons("recognized");
      if (memberPath) redirectToMembers(result.person_uid);
      else showRecognized(result);
    } catch (error) {
      setEntryButtons("unrecognized");
      if (memberPath) showLogin();
    }
  }

  document.getElementById("rs-update-details").addEventListener("click", showProfile);
  document.getElementById("rs-not-you").addEventListener("click", function () {
    callAction({ action: "retire_device", data: {} }).catch(function () {});
    clearDeviceToken();
    showRecovery();
  });
  document.getElementById("rs-recognition-close").addEventListener("click", function () {
    if ((activeView === "recognized" || activeView === "profile") && currentPerson) {
      callAction({ action: "confirm_device", data: { person_record_id: currentPerson.person_record_id, person_uid: currentPerson.person_uid } }).catch(function () {});
    }
    root.classList.remove("is-open");
  });

  profile.addEventListener("submit", async function (event) {
    event.preventDefault();
    if (!profile.reportValidity()) return;
    setBusy(profile, true);
    status("rs-profile-status", "Saving…", false);
    try {
      const data = Object.fromEntries(new FormData(profile));
      const result = await callAction({ action: "update_profile", data: Object.assign(data, { person_record_id: currentPerson.person_record_id, person_uid: currentPerson.person_uid }) });
      currentPerson = result;
      setDeviceToken(getDeviceToken(true));
      redirectToMembers(result.person_uid, "rs-profile-status");
    } catch (error) {
      status("rs-profile-status", errorText(error), true);
      setBusy(profile, false);
    }
  });

  login.addEventListener("submit", async function (event) {
    event.preventDefault();
    if (!login.reportValidity()) return;
    setBusy(login, true);
    status("rs-login-status", "Checking…", false);
    try {
      const result = await callAction({ action: "phone_login", data: Object.fromEntries(new FormData(login)) });
      if (!result.recognized) {
        setBusy(login, false);
        showRecovery();
        return;
      }
      currentPerson = result;
      setDeviceToken(getDeviceToken(true));
      redirectToMembers(result.person_uid, "rs-login-status");
    } catch (error) {
      status("rs-login-status", errorText(error), true);
      setBusy(login, false);
    }
  });

  recovery.addEventListener("submit", async function (event) {
    event.preventDefault();
    setBusy(recovery, true);
    status("rs-recovery-status", "Sending…", false);
    try {
      await callAction({ action: "recovery", data: Object.fromEntries(new FormData(recovery)) });
      status("rs-recovery-status", "Thank you. If we found your profile, we sent your member link to the email on file.", false);
      if (!preview) window.setTimeout(function () { window.location.assign("/"); }, 2500);
    } catch (error) {
      status("rs-recovery-status", errorText(error), true);
      setBusy(recovery, false);
    }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && root.classList.contains("is-open")) document.getElementById("rs-recognition-close").click();
  });

  setEntryButtons("pending");
  recognize();
})();
