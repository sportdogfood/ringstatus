import { createInputsApi } from '../../components/rs-onboarding/api';
import { prepareNativeBarn } from './barn-native-view.mjs';
import { PAGE_ID } from './barn-native-adapter.mjs';

export function prepareBarnOnboarding(document: Document, moduleUrl: string) {
  const url = new URL(moduleUrl);
  if (url.origin !== document.location.origin || !url.pathname.endsWith('/rs-inputs/native-barn.js')) throw new Error('native_api_origin_mismatch');
  const base = url.pathname.slice(0, -'/native-barn.js'.length);
  return prepareNativeBarn({ document, api: createInputsApi(base) });
}

function start() {
  if (document.documentElement.getAttribute('data-wf-page') !== PAGE_ID) return;
  try {
    const app = prepareBarnOnboarding(document, import.meta.url);
    void app.initialize();
  } catch (error) {
    const feedback = document.querySelector('#email-form .rs-barn-v24-error');
    if (feedback instanceof HTMLElement) {
      feedback.textContent = 'Barn setup could not start. Please reload the page.';
      feedback.hidden = false;
      feedback.classList.remove('rs-barn-v24-state-hidden');
      feedback.setAttribute('role', 'alert');
    }
    console.error('Barn setup initialization failed', error);
  }
}
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true });
else start();
