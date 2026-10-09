// Preparation-only reader. No mounting, event handlers, network, storage or style changes.
export const PAGE_ID = '6ac459b4506821bd349ed047';
export function readNativePreferences(document, definitions) {
  if (document.documentElement.getAttribute('data-wf-page') !== PAGE_ID) throw Error('wrong_native_page');
  const root = document.querySelector('#rs-component-drafts');
  const form = root?.querySelector('[data-rs-form="alerts"]');
  if (!form) throw Error('native_form_missing');
  const required = selector => {
    const matches = form.querySelectorAll(selector);
    if (matches.length !== 1) throw Error('native_control_missing_or_ambiguous');
    return matches[0];
  };
  const variables = Object.fromEntries(definitions.map(def => {
    const enabled = required('[data-rs-alert="' + def.key + '"]').checked;
    let value = '';
    if (def.input === 'time') value = required('[data-rs-alert-time="' + def.key + '"]').value;
    else if (def.presets) {
      const selected = form.querySelectorAll('[data-rs-action="alert-value"][data-key="' + def.key + '"][aria-pressed="true"]');
      if (selected.length > 1) throw Error('native_value_ambiguous');
      value = selected[0]?.getAttribute('data-value') || '';
    }
    return [def.key, { enabled, value }];
  }));
  return {
    enabled: required('[data-rs-sms]').checked,
    phone: required('[data-rs-field="phone"]').value,
    variables,
    // Existing page renders a sentence; it has no authoritative machine timezone field.
    timeZone: null,
    decision: null,
    pageId: PAGE_ID,
  };
}
