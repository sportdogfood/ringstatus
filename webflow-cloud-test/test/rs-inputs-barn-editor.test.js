// Exercises the actual component event handlers and API client in Node.
// The hook host is isolated; this is not rendered/browser acceptance.
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';
import { build } from 'esbuild';

const sourcePath = fileURLToPath(new URL('../src/components/rs-onboarding/Prototype.tsx', import.meta.url));
const { outputFiles } = await build({
  stdin: { contents: `export {Onboarding} from ${JSON.stringify(sourcePath)}`, resolveDir: fileURLToPath(new URL('../', import.meta.url)) },
  write: false, bundle: true, format: 'cjs', platform: 'node', jsx: 'automatic',
  plugins: [{ name: 'component-hook-host', setup(b) {
    b.onLoad({ filter: /Prototype\.tsx$/ }, async () => ({ contents: `${await readFile(sourcePath, 'utf8')}\nexport {Onboarding};`, loader: 'tsx' }));
    b.onResolve({ filter: /^(react|react\/jsx-runtime|@radix-ui\/react-icons|\.\/mobile)$|\.css$/ }, args => ({ path: args.path, namespace: 'fixture' }));
    b.onLoad({ filter: /.*/, namespace: 'fixture' }, ({ path }) => {
      if (path === 'react') return { contents: `export const useState=(...a)=>globalThis.hooks.state(...a),useRef=(...a)=>globalThis.hooks.ref(...a),useMemo=(...a)=>globalThis.hooks.memo(...a),useEffect=(...a)=>globalThis.hooks.effect(...a),useId=()=>globalThis.hooks.id();` };
      if (path === 'react/jsx-runtime') return { contents: `export const Fragment='fragment';export const jsx=(type,props)=>({type,props});export const jsxs=jsx;` };
      if (path === './mobile') return { contents: `export const MobileScroll=p=>({type:'div',props:p}),KeyboardInput=p=>({type:'input',props:p}),BottomSheet=()=>null,useKeyboard=()=>({hide(){}}),useKeyboardInsets=()=>({}),useScreenPortal=()=>({screenRef:{current:null}});` };
      if (path === '@radix-ui/react-icons') return { contents: `export const Pencil1Icon=()=>null,PlusIcon=()=>null,Cross1Icon=()=>null,ChevronDownIcon=()=>null,ArrowLeftIcon=()=>null,CheckIcon=()=>null;` };
      return { contents: '' };
    });
  } }]
});

async function fixture() {
  const slots = [], effects = []; let cursor = 0, uid = 0, tree;
  const hooks = {
    state(initial) { const i = cursor++; if (!(i in slots)) slots[i] = typeof initial === 'function' ? initial() : initial; return [slots[i], value => { slots[i] = typeof value === 'function' ? value(slots[i]) : value; }]; },
    ref(initial) { const i = cursor++; return slots[i] ||= { current: initial }; },
    memo(fn, deps) { const i = cursor++; if (!slots[i] || deps.some((v, n) => !Object.is(v, slots[i].deps[n]))) slots[i] = { deps, value: fn() }; return slots[i].value; },
    effect(fn, deps) { const i = cursor++; if (!slots[i] || deps.some((v, n) => !Object.is(v, slots[i][n]))) { slots[i] = deps; effects.push(fn); } },
    id() { return `field-${++uid}`; }
  };
  const saved = { id: 'rs_' + 'a'.repeat(32), barnId: 'rs_' + 'a'.repeat(32), name: 'Original barn', revision: 1 };
  const writes = []; let reads = 0, failedRead = false, failedSave = false;
  const state = () => ({ version: 1, barns: [structuredClone(saved)], selectedBarn: saved.id, users: [], riders: [], horses: [], locations: [] });
  const fetch = async (url, init = {}) => {
    if (url.startsWith('/test/rs-inputs/state')) { reads++; if (failedRead) { failedRead = false; return Response.json({ ok: false, error: 'storage_unavailable' }, { status: 503 }); } return Response.json({ ok: true, state: state() }); }
    assert.equal(url, '/test/rs-inputs/record');
    const body = JSON.parse(init.body); writes.push(body);
    if (failedSave) { failedSave = false; return Response.json({ ok: false, error: 'storage_unavailable' }, { status: 503 }); }
    assert.equal(body.draft.id, saved.id); assert.equal(body.expectedRevision, saved.revision);
    Object.assign(saved, { name: body.draft.name, revision: saved.revision + 1 });
    return Response.json({ ok: true, record: saved, state: state() });
  };
  const context = vm.createContext({ module: {exports: {}}, hooks, window: { location: { search: '' } }, URLSearchParams, crypto, fetch });
  vm.runInContext(outputFiles[0].text, context);
  function render() { cursor = 0; tree = context.module.exports.Onboarding({ active: true, profile: null, apiBase: '/test/rs-inputs' }); }
  async function settle() { for (let n = 0; n < 4; n++) { render(); while (effects.length) effects.shift()(); await new Promise(resolve => setImmediate(resolve)); } render(); }
  function nodes(node = tree) {
    if (!node || typeof node !== 'object') return [];
    if (Array.isArray(node)) return node.flatMap(nodes);
    if (typeof node.type === 'function') return node.type.name === 'RecordSurface' ? [] : nodes(node.type(node.props) ?? null);
    return [node, ...nodes(node.props?.children ?? null)];
  }
  const text = node => Array.isArray(node) ? node.map(text).join('') : typeof node === 'object' && node ? text(node.props?.children) : node == null || typeof node === 'boolean' ? '' : String(node);
  function button(name) { const found = nodes().find(n => n.type === 'button' && (n.props['aria-label'] === name || text(n.props.children) === name)); assert.ok(found, `Missing button: ${name}`); return found; }
  const input = () => nodes().find(n => n.type === 'input' && n.props.placeholder === 'Enter barn name');
  await settle();
  return { saved, writes, reads: () => reads, nodes, input, button, settle,
    failRead() { failedRead = true; }, failSave() { failedSave = true; },
    async click(name) { button(name).props.onClick({ preventDefault() {} }); await settle(); },
    async type(value) { assert.ok(input(), 'Barn input exists'); input().props.onChange({ target: { value } }); await settle(); },
    async submit() { const form = nodes().find(n => n.type === 'form' && n.props.className === 'rings-inline-form'); assert.ok(form); await form.props.onSubmit({ preventDefault() {} }); await settle(); }
  };
}

test('Barn pencil and Barn tab load fresh state; Cancel discards only draft with no mutation', async () => {
  const f = await fixture(); const reads = f.reads();
  f.nodes().find(n => n.type === 'input' && n.props.placeholder === 'Enter horse name').props.onChange({ target: {value: 'Unrelated horse draft'} });
  await f.settle();
  Object.assign(f.saved, { name: 'Newer barn', revision: 2 });
  await f.click('Edit barn');
  assert.equal(f.input().props.value, 'Newer barn'); assert.ok(f.reads() > reads);
  await f.type('Discard this draft'); await f.click('Cancel');
  assert.equal(f.input(), undefined); assert.equal(f.writes.length, 0); assert.equal(f.saved.name, 'Newer barn');
  await f.click('Horses');
  assert.equal(f.nodes().find(n => n.type === 'input' && n.props.placeholder === 'Enter horse name').props.value, 'Unrelated horse draft');
  Object.assign(f.saved, { name: 'Latest barn', revision: 3 });
  await f.click('Barn'); assert.equal(f.input().props.value, 'Latest barn');
  await f.type('Saved name'); await f.submit();
  assert.equal(f.writes.length, 1); assert.equal(f.writes[0].expectedRevision, 3); assert.equal(f.saved.revision, 4);
  await f.click('Edit barn'); assert.equal(f.input().props.value, 'Saved name');
});

test('failed Barn read/save keeps draft; identical save retry retains request identity', async () => {
  const f = await fixture(); await f.click('Edit barn'); await f.type('Keep this draft');
  f.failRead(); await f.click('Edit barn');
  assert.equal(f.input().props.value, 'Keep this draft'); assert.equal(f.writes.length, 0);
  f.failSave(); await f.submit();
  assert.equal(f.input().props.value, 'Keep this draft'); assert.equal(f.saved.name, 'Original barn');
  await f.submit();
  assert.equal(f.writes.length, 2); assert.equal(f.writes[0].requestId, f.writes[1].requestId);
  assert.equal(f.saved.name, 'Keep this draft'); assert.equal(f.saved.revision, 2);
});
