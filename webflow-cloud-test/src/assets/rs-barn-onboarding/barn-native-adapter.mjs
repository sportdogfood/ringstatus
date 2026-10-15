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
// Extend the existing native field template; keep its classes and save controls.
export function addProfileFields(form,fields,kind){
 if(!['barn','users'].includes(kind))return fields;
 const definitions=kind==='barn'?[['logo','Barn logo URL','url']]:[['displayName','Display name','text'],['sms','SMS number','tel'],['pin','PIN (leave blank to keep)','password'],['profileImg','Profile image URL','url'],['roles','Roles','select'],['membershipStatus','User access','select'],['tags','Tags (comma-separated)','text']];
 const template=fields.name.closest('.rs-barn-v24-field');if(!template)return fields;
 for(const [key,label,type]of definitions){
  let node=form.querySelector('[data-rs-profile-field="'+key+'"]');
  if(!node){const wrapper=template.cloneNode(true);wrapper.removeAttribute('id');const old=wrapper.querySelector('input');const id=(form.id||kind+'-sheet')+'-'+key;
   node=type==='select'?form.ownerDocument.createElement('select'):old.cloneNode(true);node.className=old.className;node.id=id;node.name=key;node.removeAttribute('required');node.removeAttribute('value');node.removeAttribute('data-name');node.value='';node.dataset.rsProfileField=key;
   if(type==='select'){node.multiple=key==='roles';if(key==='membershipStatus')for(const [value,text]of [['Enabled','Active'],['Disabled','Inactive']]){const option=form.ownerDocument.createElement('option');option.value=value;option.textContent=text;node.appendChild(option);}}else {node.type=type;node.placeholder=label;node.maxLength=key==='pin'?4:2048;}
   if(key==='pin')node.autocomplete='new-password';
   old.replaceWith(node);const caption=wrapper.querySelector('label')||wrapper.querySelector('div');if(caption)caption.textContent=label;if(caption?.tagName==='LABEL')caption.htmlFor=id;if(wrapper.tagName==='LABEL')wrapper.htmlFor=id;template.parentNode.insertBefore(wrapper,form.querySelector('.rs-barn-v24-error')||template.nextSibling);
  }fields[key]=node;
 }return fields;
}
export function inspectMainForms(document) {
  if (document.documentElement.getAttribute('data-wf-page') !== PAGE_ID) throw new Error('native_page_mismatch');
  const root = one(document, ROOT);
  const forms = Object.fromEntries(Object.entries(FORMS).map(([kind, spec]) => {
    const form = one(root, spec.form);
    const fields = Object.fromEntries(Object.entries(spec.fields).map(([field, selector]) => [field, one(form, selector)]));
    addProfileFields(form,fields,kind);
    const primary = one(form, '.rs-barn-v24-primary');
    const secondary = kind === 'barn' ? null : one(form, '.rs-barn-v24-secondary');
    return [kind, { form, fields, primary, secondary }];
  }));
  return { root, forms };
}
export function readDraft(fields, editing) {
  const draft = blank();
  for (const [key, node] of Object.entries(fields)) draft[key] = node.multiple?[...node.selectedOptions].map(o=>o.value).sort().join(','):node.value.trim();
  if (editing) {
    if (!editing.id || !Number.isInteger(editing.revision)) throw new Error('record_revision_missing');
    draft.id = editing.id;
    draft.revision = editing.revision;
  }
  return draft;
}
export function fillDraft(fields, record = {}) {
  for (const [key, node] of Object.entries(fields)){const value=key==='pin'?'':typeof record[key]==='string'?record[key]:key==='membershipStatus'?'Enabled':'';if(node.multiple){for(const o of node.options)o.selected=value.split(',').includes(o.value);}else node.value=value;}
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
  let state = null, actor = null, pending = false, disposed = false, accessInvalid=false;
  const pendingKinds = new Set();
  const staleKinds = new Set();
  const pendingBarn = {};
  const optimistic = new Map();
  const confirmedIds=new Map();
  function display() {
    if (!state || disposed) return;
    const visible = structuredClone(state);
    for (const {kind,entry:local} of optimistic.values()) {
      const entry={...local,id:confirmedIds.get(local.id)||local.id};
      const key=kind==='barn'?'barns':kind;
      const rows=visible[key],index=rows.findIndex(row=>row.id===entry.id);
      if(index<0)rows.push(entry);else rows[index]=entry;
    }
    view.state(visible, actor);
  }
  function showPending(kind,draft,barnId,requestId='restored') {
    const id=draft.id||`pending-${kind}-${requestId}`;
    const key=kind+':'+id;
    const entry={...draft,id,revision:draft.revision??0,barnId:kind==='barn'?(draft.id||''):barnId,pending:true};
    optimistic.set(key,{kind,entry});
    display();
    return {key,entry};
  }
  const editing = Object.fromEntries(Object.keys(FORMS).map(kind => [kind, null]));
  const remove = [];
  const feedback = (kind, type, value) => view.feedback({ kind, type, value });
  async function run(kind, operation, saving = false) {
    if (pending || disposed || accessInvalid) return false;
    if(saving)pendingKinds.add(kind);else pending = true;
    view.busy(true, {kind, saving});
    try { return await operation(); }
    catch (error) { if(error.code==='authentication_required'){accessInvalid=true;optimistic.clear();if(state)apply({...state,barns:[],users:[],riders:[],horses:[],locations:[],selectedBarn:'',canManage:false});for(const kind of Object.keys(FORMS)){fillDraft(targets.forms[kind].fields);view.editor({kind,open:false,reveal:false});}}if(error.code==='record_changed')staleKinds.add(kind);if (!disposed) feedback(kind, 'error', error.code || error.message); return false; }
    finally { if(saving)pendingKinds.delete(kind);else pending = false; if (!disposed) view.busy(false, {kind, saving}); }
  }
  function apply(next) {
    if (disposed) return;
    state = next;
    display();
  }
  function applySaved(next, kind, record) {
    if(accessInvalid)return;
    // WEC/HPS: merge the confirmed record, not an unrelated response snapshot.
    if(!state){apply(next);return;}
    const merged=structuredClone(state),key=kind==='barn'?'barns':kind;
    const index=merged[key].findIndex(row=>row.id===record.id);
    if(index<0)merged[key].push(record);else if((record.revision||0)>=(merged[key][index].revision||0))merged[key][index]=record;
    if(kind==='barn'&&!merged.selectedBarn)merged.selectedBarn=record.id;
    apply(merged);
  }
  async function initialize() {
    const initialized=await run('barn', async () => {
      try {
        actor = await api.access(); // Same verified cookie contract as existing onboarding.
      } catch (error) {
        // Only an absent/expired identity enters Recognize. Storage failures stay
        // on this page, and failed saves never discard an existing draft.
        if (error.code !== 'authentication_required') throw error;
        if (!disposed) navigate('/rs-recognize?return_to=%2Frs-barn-onboarding-setup-v24');
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
      for (const kind of ['barn','riders','users']) {
        const drafts=await api.pendingDrafts?.(kind)||[];
        for(const saved of drafts)showPending(kind,saved.draft,saved.barnId,saved.requestId);
        const saved=await api.pendingDraft?.(kind);
        const draft=saved?.draft;
        // Retain another Barn's pending work without changing the recognized selection.
        if(draft && saved.barnId===state?.selectedBarn){
          pendingBarn[kind]=saved.barnId;
showPending(kind,draft,saved.barnId,saved.requestId);
editing[kind]=draft.id?draft:null;fillDraft(targets.forms[kind].fields,draft);view.editor({kind,record:editing[kind],open:true});feedback(kind,'error','retry');}
      }
      if(loadError)throw loadError;
      return true;
    });
    if(initialized&&new URLSearchParams(document.location?.search||'').get('profile')==='me')await profileLink();
    return initialized;
  }
  async function selectBarn(id) {
    return run('barn', async () => {
      if (!state?.barns.some(r => r.id === id)) throw new Error('barn_not_found');
      const next = await api.state(id);
      if (disposed) return false;
      apply(next);
      for (const kind of Object.keys(FORMS)) {
        delete pendingBarn[kind];
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
      const restored=await api.pendingDraft?.(kind,id);
      if(restored?.draft && (restored.draft.id===id || id===`pending-${kind}-${restored.requestId}`)){
        pendingBarn[kind]=restored.barnId;editing[kind]={...restored.draft,id,revision:restored.draft.revision??0};
        fillDraft(targets.forms[kind].fields,restored.draft);view.editor({kind,record:editing[kind],open:true});feedback(kind,'error','retry');return true;
      }
      const next = staleKinds.has(kind) ? await api.state(state.selectedBarn) : state;
      staleKinds.delete(kind);
      const local=[...optimistic.values()].find(v=>v.kind===kind&&(v.entry.id===id||confirmedIds.get(v.entry.id)===id))?.entry;
      const record = local?{...local,id}:list(next, kind).find(r => r.id === id);
      if (!record || !Number.isInteger(record.revision)) throw new Error('record_not_found');
      if (disposed) return false;
      apply(next);
      delete pendingBarn[kind];
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
      const editAtSave=editing[kind];
      const barnId=pendingBarn[kind] ?? state.selectedBarn;
      validate(kind, draft, state);
      let shown;
      const result = await api.record(kind, draft, barnId,operation=>{
        if(accessInvalid)return;
        shown=showPending(kind,operation.body.draft,barnId,operation.requestId);
        delete pendingBarn[kind];
        if(editing[kind]===editAtSave&&JSON.stringify(readDraft(targets.forms[kind].fields,editing[kind]))===JSON.stringify(draft)){
          editing[kind]=kind==='barn'?draft:null;
          fillDraft(targets.forms[kind].fields,kind==='barn'?draft:{});
          view.editor({kind,open:another,record:null,reveal:false});
        }
        feedback(kind,'success','Saving');
      });
      if(shown&&optimistic.get(shown.key)?.entry===shown.entry)optimistic.delete(shown.key);
      if(shown)confirmedIds.set(shown.entry.id,result.record.id);
      if (disposed||accessInvalid) return false;
      applySaved(result.state, kind, result.record);
      feedback(kind, 'success', [...optimistic.values()].some(v=>v.kind===kind)?'Saving':'Saved');
      return true;
    }, true);
  }
  async function profileLink() {
    const own=state?.users.filter(row=>row.barnId===state.selectedBarn&&row.recognitionPersonId===actor?.id)||[];
    if(own.length===1)return beginEdit('users',own[0].id);
    if(own.length>1){feedback('users','error','ambiguous_tenant_mapping');return false;}
    return run('users', async () => {
      if (!state?.selectedBarn) throw new Error('barn_required');
      const result = await api.profileLink(state.selectedBarn);
      if (disposed) return false;
      applySaved(result.state, 'users', result.record);
      editing.users=result.record;fillDraft(targets.forms.users.fields,result.record);view.editor({kind:'users',record:result.record,open:true});
      feedback('users', 'success', 'Your profile is ready to edit.');
      return true;
    });
  }
  function add(kind) {
    if (pending || disposed || accessInvalid || !state || !FORMS[kind]) return false;
    delete pendingBarn[kind];
    editing[kind] = null;
    fillDraft(targets.forms[kind].fields,kind==='users'?{roles:state.roleOptions?.find(r=>r.name==='user')?.id||''}:{});
    api.resetDraftRequest(kind);
    view.editor({ kind, record: null, open: true });
    return true;
  }
  function cancel(kind) {
    if (pending || disposed || accessInvalid || !state || !FORMS[kind]) return false;
    delete pendingBarn[kind];
    editing[kind] = null;
    fillDraft(targets.forms[kind].fields);
    api.resetDraftRequest(kind);
    view.editor({ kind, record: null, open: false });
    return true;
  }
  async function saveLinked(kind, draft, prepared) {
    return run(kind, async () => {
      if (!state || !FORMS[kind]) throw new Error('records_not_ready');
      validate(kind, draft, state);
      const barnId=pendingBarn[kind] ?? state.selectedBarn;
      let shown;
      const result = await api.record(kind, draft, barnId,operation=>{
        if(accessInvalid)return;
        shown=showPending(kind,operation.body.draft,barnId,operation.requestId);
        delete pendingBarn[kind];feedback(kind,'success','Saving');prepared?.();
      });
      if(shown&&optimistic.get(shown.key)?.entry===shown.entry)optimistic.delete(shown.key);
      if(shown)confirmedIds.set(shown.entry.id,result.record.id);
      if (disposed||accessInvalid) return false;
      applySaved(result.state, kind, result.record);
      feedback(kind,'success',[...optimistic.values()].some(v=>v.kind===kind)?'Saving':'Saved');
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
    getState: () => state, isBusy: () => pending,
    dispose() { disposed = true; remove.forEach(fn => fn()); mounted.delete(targets.root); } };
  mounted.set(targets.root, controller);
  return controller;
}
