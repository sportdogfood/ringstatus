import { mountNativeRecognition } from './native-client.js';

// Presentation only: identity, credentials, eligibility and retirement stay in
// the existing shared controller/server used by the API-agent adapter.
export function mountRecognitionEmbed(root, options = {}) {
  if (!root || root.__rsNativeRecognition) return root?.__rsNativeRecognition;
  root.id = 'rs-native-recognize';
  root.innerHTML = `<style>
    #rs-native-recognize{box-sizing:border-box;width:100%;max-width:420px;margin:32px auto;padding:20px;font:16px/1.5 system-ui,sans-serif;color:#252025}
    #rs-native-recognize [hidden]{display:none!important}
    #rs-native-recognize input,#rs-native-recognize button{box-sizing:border-box;width:100%;min-height:48px;font:inherit;border-radius:10px;padding:12px}
    #rs-native-recognize input{border:1px solid #bdb6bd;margin:8px 0 12px;background:white}
    #rs-native-recognize button{border:0;background:#815374;color:white;cursor:pointer}
    #rs-native-recognize button:disabled{opacity:.6;cursor:wait}
    #rs-native-recognize [data-rs-native-feedback]{min-height:24px;font-size:14px}
  </style><section data-rs-native-state="login" hidden>
    <form><label for="rs-rec2-identifier">Phone, PIN or owner invite</label>
    <input id="rs-rec2-identifier" name="identifier" inputmode="numeric" autocomplete="tel" maxlength="24" required>
    <button type="submit" data-rs-native-action="phone-login">Continue</button></form>
    <p data-rs-native-feedback role="status" aria-live="polite"></p>
  </section><section data-rs-native-state="recognized" hidden><p data-rs-native-feedback role="status"></p>
    <button type="button" data-rs-native-action="retire">Not you? Sign out</button></section>
  <section data-rs-native-state="unavailable"><p data-rs-native-feedback role="status" aria-live="polite">Checking recognition…</p>
    <button type="button" data-rs-native-action="retry">Try again</button></section>`;
  const baseUrl = new URL('/test/rs-recognition/', location.origin).href;
  let persistentStorage = options.persistentStorage;
  try { persistentStorage ||= globalThis.localStorage; } catch { /* Native controller has its existing storage fallback. */ }
  const destination = '/rs-barn-onboarding-setup-v24';
  // Cached state controls presentation only. The existing Tenants server still
  // validates its secure cookie before returning any private records.
  if (!options.skipCachedReturn && new URLSearchParams(location.search).get('sign_out') !== '1') {
    try {
      const remembered = JSON.parse(persistentStorage?.getItem('rs_recognition_identity_v1') || 'null');
      const token = persistentStorage?.getItem('rs_recognition_device_token_v1');
      if (remembered?.personUid && remembered.deviceToken === token && remembered.expiresAt > Date.now()) {
        (options.navigate || (path => location.assign(path)))(destination);
        return null;
      }
    } catch { /* A missing cache uses the existing silent cookie lookup. */ }
  }
  const app = mountNativeRecognition({ root, baseUrl, launcherPath: destination, minimal: true, ...options,
    present({ state, busy }) {
      root.setAttribute('aria-busy', String(!!busy));
      for (const section of root.querySelectorAll('[data-rs-native-state]')) section.hidden = section.dataset.rsNativeState !== state;
      for (const button of root.querySelectorAll('button')) button.disabled = !!busy;
      options.present?.({ state, busy });
    }
  });
  root.querySelector('form').addEventListener('submit', event => {
    event.preventDefault();
    if (!app.snapshot().busy) void app.perform('submit_credential', root.querySelector('[name="identifier"]').value.trim());
  });
  return app;
}

if (typeof document !== 'undefined') {
  const start = () => { const root = document.getElementById('rs-rec2'); if (root) mountRecognitionEmbed(root); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true });
  else start();
}
