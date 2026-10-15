// Uses the existing createInputsApi instance; never fetches Airtable or handles identity tokens.
export const PAGE_ID = '6ac7b2de07b992a509c90029';
export const ROOT = '.rs-barn-v24-root';
export const FORMS = Object.freeze({
  barn: { form: '#email-form', fields: { name: '#Barn-name' } },
  users: { form: '#email-form-2', fields: { name: '#User-name', email: '#Email-optional' } },
  riders: { form: '#email-form-3', fields: { name: '#Rider-name', userId: '#User-account-optional' } },
  horses: { form: '#email-form-4', fields: { name: '#Horse-name', riderId: '#Rider', locationId: '#Location' } },
  locations: { form: '#email-form-5', fields: { name: '#Location-name', address: '#Address-or-description-optional' } }
});
export const blank = () => ({ name: '', email: '', userId: '', riderId: '', locationId: '', address: '' });
const mounted = new WeakMap();
const list = (state, kind) => kind === 'barn' ? state.barns : state[kind].filter(r => r.barnId === state.selectedBarn);
function one(root, selector) {
  const nodes = root.querySelectorAll(selector);
  if (nodes.length !== 1) throw new Error(`native_target_mismatch:${selector}`);
  return nodes[0];
}
export function inspectMainForms(document) {
  if (document.documentElement.getAttribute('data-wf-page') !== PAGE_ID) throw new Error('native_page_mismatch');
  const root = one(document, ROOT);
  const forms = Object.fromEntries(Object.entries(FORMS).map(([kind, spec]) => {
    const form = one(root, spec.form);
    const fields = Object.fromEntries(Object.entries(spec.fields).map(([field, selector]) => [field, one(form, selector)]));
    const primary = one(form, '.rs-barn-v24-primary');
    const secondary = kind === 'barn' ? null : one(form, '.rs-barn-v24-secondary');
    return [kind, { form, fields, primary, secondary }];
  }));
  return { root, forms };
}
export function readDraft(fields, editing) {
  const draft = blank();
  for (const [key, node] of Object.entries(fields)) draft[key] = node.value.trim();
  if (editing) {
    if (!editing.id || !Number.isInteger(editing.revision)) throw new Error('record_revision_missing');
    draft.id = editing.id;
    draft.revision = editing.revision;
  }
  return draft;
}
export function fillDraft(fields, record = {}) {
  for (const [key, node] of Object.entries(fields)) node.value = typeof record[key] === 'string' ? record[key] : '';
}
function validate(kind, draft, state) {
  if (!draft.name) throw new Error('name_required');
  if (kind !== 'barn' && !state.selectedBarn) throw new Error('barn_required');
  if (draft.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(draft.email)) throw new Error('invalid_email');
  const relations = kind === 'riders' ? [['userId', 'users']] : kind === 'horses' ? [['riderId', 'riders'], ['locationId', 'locations']] : [];
  for (const [field, related] of relations) {
    if (draft[field] && !list(state, related).some(r => r.id === draft[field])) throw new Error('invalid_relationship');
  }
}
// The native view implements these callbacks against the verified Webflow tree.
// state: populate actual records/options; feedback: display designated error/success;
// busy: inhibit navigation/edit during pending calls; editor: select native edit/add/tab state.
export function bindMainForms({ document, api, view, navigate = path => document.location.assign(path) }) {
  const targets = inspectMainForms(document);
  if (mounted.has(targets.root)) return mounted.get(targets.root);
  for (const method of ['state', 'feedback', 'busy', 'editor']) if (typeof view?.[method] !== 'function') throw new Error(`native_view_contract_missing:${method}`);
  let state = null, actor = null, pending = false, disposed = false;
  const pendingKinds = new Set();
  const staleKinds = new Set();
  const pendingBarn = {};
  const optimistic = new Map();
  function display() {
    if (!state || disposed) return;
    const visible = structuredClone(state);
    for (const [kind, entry] of optimistic) {
      const key=kind==='barn'?'barns':kind;
      const rows=visible[key],index=rows.findIndex(row=>row.id===entry.id);
      if(index<0)rows.push(entry);else rows[index]=entry;
    }
    view.state(visible, actor);
  }
  function showPending(kind,draft,barnId) {
    optimistic.set(kind,{...draft,id:draft.id||`pending-${kind}`,barnId:kind==='barn'?(draft.id||''):barnId,pending:true});
    display();
  }
  const editing = Object.fromEntries(Object.keys(FORMS).map(kind => [kind, null]));
  const remove = [];
  const feedback = (kind, type, value) => view.feedback({ kind, type, value });
  async function run(kind, operation, saving = false) {
    if (pending || pendingKinds.has(kind) || disposed) return false;
    if(saving)pendingKinds.add(kind);else pending = true;
    view.busy(true, {kind, saving});
    try { return await operation(); }
    catch (error) { if(error.code==='record_changed')staleKinds.add(kind);if (!disposed) feedback(kind, 'error', error.code || error.message); return false; }
    finally { if(saving)pendingKinds.delete(kind);else pending = false; if (!disposed) view.busy(false, {kind, saving}); }
  }
  function apply(next) {
    if (disposed) return;
    state = next;
    display();
  }
  function applySaved(next, kind, record) {
    // WEC/HPS: merge the confirmed record, not an unrelated response snapshot.
    if(!state){apply(next);return;}
    const merged=structuredClone(state),key=kind==='barn'?'barns':kind;
    const index=merged[key].findIndex(row=>row.id===record.id);
    if(index<0)merged[key].push(record);else if((record.revision||0)>=(merged[key][index].revision||0))merged[key][index]=record;
    if(kind==='barn')merged.selectedBarn=record.id;
    apply(merged);
  }
  async function initialize() {
    return run('barn', async () => {
      try {
        actor = await api.access(); // Same verified cookie contract as existing onboarding.
      } catch (error) {
        // Only an absent/expired identity enters Recognize. Storage failures stay
        // on this page, and failed saves never discard an existing draft.
        if (error.code !== 'authentication_required') throw error;
        if (!disposed) navigate('/rs-recognize');
        return false;
      }
      const cached=await api.cachedState?.();if(cached)apply(cached);
      let next,loadError;
      try{next=await api.state();}catch(error){loadError=error;}
      if (disposed) return false;
      if(next)apply(next);
      const barn = state?.barns.find(r => r.id === state.selectedBarn);
      editing.barn = barn || null;
      fillDraft(targets.forms.barn.fields, barn);
      for (const kind of ['barn','riders']) {
        const saved=await api.pendingDraft?.(kind);
        const draft=saved?.draft;
        if(draft){
          pendingBarn[kind]=saved.barnId;
          if(next&&saved.barnId&&next.selectedBarn!==saved.barnId){const restored=await api.state(saved.barnId);apply(restored);}
showPending(kind,draft,saved.barnId);
editing[kind]=draft.id?draft:null;fillDraft(targets.forms[kind].fields,draft);view.editor({kind,record:editing[kind],open:true});feedback(kind,'error','retry');}
      }
      if(loadError)throw loadError;
      return true;
    });
  }
  async function selectBarn(id) {
    if(pendingKinds.size)return false;
    return run('barn', async () => {
      if (!state?.barns.some(r => r.id === id)) throw new Error('barn_not_found');
      const next = await api.state(id);
      if (disposed) return false;
      apply(next);
      for (const kind of Object.keys(FORMS)) {
        const record = kind === 'barn' ? next.barns.find(r => r.id === next.selectedBarn) : null;
        editing[kind] = record || null;
        fillDraft(targets.forms[kind].fields, record || {});
      }
      api.resetDraftRequest();
      return true;
    });
  }
  async function beginEdit(kind, id) {
    return run(kind, async () => {
      if (!FORMS[kind] || !state) throw new Error('records_not_ready');
      const restored=await api.pendingDraft?.(kind);
      if(restored?.draft && (restored.draft.id===id || id===`pending-${kind}`)){
        pendingBarn[kind]=restored.barnId;editing[kind]=restored.draft.id?restored.draft:null;
        fillDraft(targets.forms[kind].fields,restored.draft);view.editor({kind,record:editing[kind],open:true});feedback(kind,'error','retry');return true;
      }
      const next = staleKinds.has(kind) ? await api.state(state.selectedBarn) : state;
      staleKinds.delete(kind);
      const record = list(next, kind).find(r => r.id === id);
      if (!record || !Number.isInteger(record.revision)) throw new Error('record_not_found');
      if (disposed) return false;
      apply(next);
      editing[kind] = record;
      fillDraft(targets.forms[kind].fields, record);
      api.resetDraftRequest(kind);
      view.editor({ kind, record, open: true });
      return true;
    });
  }
  async function save(kind, another = false) {
    return run(kind, async () => {
      if (!FORMS[kind] || !state) throw new Error('records_not_ready');
      const draft = readDraft(targets.forms[kind].fields, editing[kind]);
      validate(kind, draft, state);
      showPending(kind,draft,pendingBarn[kind] ?? state.selectedBarn);
      feedback(kind,'success','Saving');
      const result = await api.record(kind, draft, pendingBarn[kind] ?? state.selectedBarn);
      delete pendingBarn[kind];
      optimistic.delete(kind);
      if (disposed) return false;
      applySaved(result.state, kind, result.record);
      editing[kind] = kind === 'barn' ? result.record : null;
      fillDraft(targets.forms[kind].fields, kind === 'barn' ? result.record : kind === 'horses' ? { locationId: draft.locationId } : {});
      view.editor({ kind: kind === 'barn' ? 'users' : kind, open: kind === 'barn' || another, record: null, reveal: false });
      feedback(kind, 'success', 'Saved');
      return true;
    }, true);
  }
  async function profileLink() {
    return run('users', async () => {
      if (!state?.selectedBarn) throw new Error('barn_required');
      const result = await api.profileLink(state.selectedBarn);
      if (disposed) return false;
      applySaved(result.state, 'users', result.record);
      feedback('users', 'success', `${result.record.name} linked to this barn.`);
      return true;
    });
  }
  function add(kind) {
    if (pending || pendingKinds.has(kind) || disposed || !state || !FORMS[kind]) return false;
    editing[kind] = null;
    fillDraft(targets.forms[kind].fields);
    api.resetDraftRequest(kind);
    view.editor({ kind, record: null, open: true });
    return true;
  }
  function cancel(kind) {
    if (pending || pendingKinds.has(kind) || disposed || !state || !FORMS[kind]) return false;
    editing[kind] = null;
    fillDraft(targets.forms[kind].fields);
    api.resetDraftRequest(kind);
    view.editor({ kind, record: null, open: false });
    return true;
  }
  async function saveLinked(kind, draft) {
    return run(kind, async () => {
      if (!state || !FORMS[kind]) throw new Error('records_not_ready');
      validate(kind, draft, state);
      showPending(kind,draft,pendingBarn[kind] ?? state.selectedBarn);
      feedback(kind,'success','Saving');
      const result = await api.record(kind, draft, pendingBarn[kind] ?? state.selectedBarn);
      delete pendingBarn[kind];
      optimistic.delete(kind);
      if (disposed) return false;
      applySaved(result.state, kind, result.record);
      if (kind === 'barn') {
        editing.barn = result.record;
        fillDraft(targets.forms.barn.fields, result.record);
        // Existing independent drafts remain untouched when this save completes.
      }
      feedback(kind,'success','Saved');
      return result;
    }, true);
  }
  function on(node, event, handler) { node.addEventListener(event, handler, true); remove.push(() => node.removeEventListener(event, handler, true)); }
  function keyboardAction(node, handler) {
    if (node.tagName !== 'A') return;
    node.setAttribute('role', 'button'); node.setAttribute('tabindex', '0');
    on(node, 'keydown', event => { if (event.key === ' ' || event.key === 'Enter') handler(event); });
  }
  for (const [kind, nodes] of Object.entries(targets.forms)) {
    const saveHandler = another => event => { event.preventDefault(); event.stopImmediatePropagation(); void save(kind, another); };
    on(nodes.form, 'submit', saveHandler(false));
    on(nodes.primary, 'click', saveHandler(kind !== 'barn'));
    keyboardAction(nodes.primary, saveHandler(kind !== 'barn'));
    if (nodes.secondary) { on(nodes.secondary, 'click', saveHandler(false)); keyboardAction(nodes.secondary, saveHandler(false)); }
  }
  const controller = { initialize, selectBarn, beginEdit, save, profileLink, add, cancel, saveLinked,
    getState: () => state, isBusy: (kind) => pending || (kind ? pendingKinds.has(kind) : pendingKinds.size > 0),
    dispose() { disposed = true; remove.forEach(fn => fn()); mounted.delete(targets.root); } };
  mounted.set(targets.root, controller);
  return controller;
}
