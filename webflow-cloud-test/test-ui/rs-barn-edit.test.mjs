// Local browser interaction checks with synthetic API state; no provider requests.
// Requires the existing Playwright Chromium installation; no live credentials.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { build } from 'esbuild';
const require = createRequire(import.meta.url);
const { chromium } = require('playwright');
const root = fileURLToPath(new URL('../', import.meta.url));

test('Barn edit reloads revision, cancel never writes, failed saves retain the draft and retry identity', async () => {
  const { outputFiles } = await build({
    stdin: { contents: `import React from 'react'; import {createRoot} from 'react-dom/client'; import App from './src/components/rs-onboarding/App'; import './src/components/rs-onboarding/styles.css'; createRoot(document.getElementById('root')).render(<App apiBase='/test/rs-inputs'/>);`, resolveDir: root, loader: 'tsx' },
    bundle: true, write: false, outdir: '/virtual-barn-test', format: 'iife', platform: 'browser', jsx: 'automatic',
    nodePaths: (process.env.NODE_PATH || '').split(':').filter(Boolean),
    define: { 'process.env.NODE_ENV': '"test"', 'import.meta.env.BASE_URL': '"/test/"' },
    loader: { '.woff': 'dataurl', '.woff2': 'dataurl', '.svg': 'dataurl', '.png': 'dataurl' }
  });
  const assets = new Map(outputFiles.map(file => [file.path.split('/').at(-1), file.contents]));
  const server = createServer(async (req, res) => {
    if (req.url === '/') { res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><html><head><title>Barn interaction fixture</title><link rel="stylesheet" href="/stdin.css"></head><body><div id="root"></div><script src="/stdin.js"></script></body></html>'); return; }
    const name = req.url?.slice(1);
    if (assets.has(name)) { res.setHeader('Content-Type', name.endsWith('.css') ? 'text/css' : 'text/javascript'); res.end(assets.get(name)); return; }
    res.statusCode = 204; res.end();
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({ viewport: { width: 1280, height: 1100 } });
    const browserErrors = [];
    page.on('pageerror', error => browserErrors.push(error.message));
    const origin = `http://127.0.0.1:${server.address().port}`;
    let saved = { id: 'rs_' + 'a'.repeat(32), barnId: 'rs_' + 'a'.repeat(32), name: 'QA Barn original', revision: 1 };
    let failNextSave = true;
    let stateReads = 0;
    const writes = [];
    const state = () => ({ version: 1, barns: [saved], selectedBarn: saved.id, users: [], riders: [], horses: [], locations: [] });
    await page.route('**/test/rs-inputs/**', async route => {
      assert.equal(new URL(route.request().url()).origin, origin);
      const path = new URL(route.request().url()).pathname.split('/').at(-1);
      const json = (body, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
      if (path === 'access') return json({ ok: true, actor: { id: 'qa_barn_person', profile: { personUid: 'qa_barn_person', name: 'QA Barn Person', email: '' } } });
      if (path === 'recognition') return json({ ok: true, recognized: false, profile: null, device: 'unknown' });
      if (path === 'state') { stateReads++; return json({ ok: true, state: state() }); }
      assert.equal(path, 'record');
      const body = route.request().postDataJSON();
      writes.push(body);
      assert.equal(body.kind, 'barn');
      assert.equal(body.draft.id, saved.id);
      assert.equal(body.expectedRevision, saved.revision);
      if (failNextSave) { failNextSave = false; return json({ ok: false, error: 'storage_unavailable' }, 503); }
      saved = { ...saved, name: body.draft.name, revision: saved.revision + 1 };
      return json({ ok: true, record: saved, state: state() });
    });
    await page.goto(origin);
    await page.getByRole('button', { name: 'Close recognition', exact: true }).click();
    await page.getByRole('button', { name: 'Barn setup', exact: true }).click();
    await page.getByRole('button', { name: 'QA Barn original', exact: true }).waitFor();
    // A separate saved edit occurred after the initial state load.
    saved = { ...saved, name: 'QA Barn latest', revision: 2 };
    const readsBeforeEdit = stateReads;
    await page.getByRole('button', { name: 'Edit barn', exact: true }).click();
    await page.getByRole('textbox', { name: 'Barn name', exact: true }).waitFor();
    await page.waitForFunction(() => document.querySelector('input[placeholder="Enter barn name"]')?.value === 'QA Barn latest');
    assert.ok(stateReads > readsBeforeEdit);
    await page.getByRole('textbox', { name: 'Barn name', exact: true }).fill('Unsaved cancel draft');
    await page.getByRole('button', { name: 'Cancel', exact: true }).click();
    assert.equal(writes.length, 0);
    assert.equal(saved.name, 'QA Barn latest');
    assert.equal(await page.getByRole('textbox', { name: 'Barn name', exact: true }).count(), 0);
    // Reopening through the Barn tab also reloads the current saved revision.
    saved = { ...saved, name: 'QA Barn newer', revision: 3 };
    await page.getByRole('navigation', { name: 'Barn setup' }).getByRole('button', { name: 'Barn', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('input[placeholder="Enter barn name"]')?.value === 'QA Barn newer');
    await page.getByRole('textbox', { name: 'Barn name', exact: true }).fill('QA Barn saved');
    await page.getByRole('button', { name: 'Save barn & continue', exact: true }).click();
    await page.getByRole('alert').first().waitFor();
    assert.equal(await page.getByRole('textbox', { name: 'Barn name', exact: true }).inputValue(), 'QA Barn saved');
    assert.equal(saved.name, 'QA Barn newer');
    await page.getByRole('button', { name: 'Save barn & continue', exact: true }).click();
    await page.getByRole('button', { name: 'QA Barn saved', exact: true }).waitFor();
    assert.equal(writes.length, 2);
    assert.equal(writes[0].requestId, writes[1].requestId);
    assert.equal(writes[1].expectedRevision, 3);
    assert.equal(saved.revision, 4);
    await page.reload();
    await page.getByRole('button', { name: 'Close recognition', exact: true }).click();
    await page.getByRole('button', { name: 'Barn setup', exact: true }).click();
    await page.getByRole('button', { name: 'Edit barn', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('input[placeholder="Enter barn name"]')?.value === 'QA Barn saved');
    assert.deepEqual(browserErrors, []);
  } finally {
    if (browser) await browser.close();
    await new Promise(resolve => server.close(resolve));
  }
});
