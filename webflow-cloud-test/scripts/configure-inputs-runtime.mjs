// Scoped configuration only. Never logs values, starts OAuth, or deploys code.
// Requires a Webflow CLI with `apps env-vars` support; use its prerelease channel
// if the installed stable version lacks that command.
import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { pathToFileURL } from 'node:url';

export const TARGET = Object.freeze({
  appId: 'd7d97751-20e1-4148-a5cf-ee58671c128a',
  environmentId: '110f06dd-c1ea-4839-98af-d829cbe77941',
  siteId: '6982268b7543ac3c80151266', branch: 'main', mount: '/test',
});
const BASE = 'app9kOZdIaGyKk5uG';
const FLAGS = ['--app-id', TARGET.appId, '--environment-id', TARGET.environmentId];
const COMMON = ['--no-input', '--skip-update-check', '--region', 'us', '--json'];

export function configuration(env = {}) {
  const storage = env.RS_INPUTS_BARN_STORAGE || 'airtable';
  if (!['airtable', 'zoho-crm'].includes(storage)) throw new Error('Invalid RS_INPUTS_BARN_STORAGE');
  return {
    plain: {
      RS_INPUTS_BASE_ID: BASE, RS_INPUTS_RECOGNITION_BASE_ID: BASE,
      RS_INPUTS_WRITE_MODE: 'isolated-trial', RS_INPUTS_BARN_STORAGE: storage,
    },
    secrets: ['AIRTABLE_TOKEN', 'RS_INPUTS_SESSION_SECRET',
      ...(storage === 'zoho-crm' ? ['ZOHO_CLIENT_ID', 'ZOHO_CLIENT_SECRET', 'ZOHO_REFRESH_TOKEN'] : [])],
  };
}

function list(response, key) {
  if (response?.nextCursor || response?.pagination?.nextCursor) throw new Error(`Incomplete CLI ${key} metadata; refusing partial configuration`);
  const value = Array.isArray(response) ? response : response?.[key];
  if (!Array.isArray(value)) throw new Error(`Unrecognized CLI ${key} response; no inferred metadata`);
  return value;
}

export async function configure({ env = process.env, apply = false, run = cli, generateSecret = () => randomBytes(32).toString('hex') } = {}) {
  const { plain, secrets } = configuration(env);
  const summary = { target: TARGET, mode: apply ? 'apply' : 'dry-run', keys: [...Object.keys(plain), ...secrets] };
  if (!apply) return { ...summary, note: 'Offline preview only; no CLI calls, writes, or deployment. --apply verifies target first.' };

  const appResult = await run(['apps', 'get', TARGET.appId, ...COMMON]);
  const app = appResult?.app || appResult;
  if (app?.id !== TARGET.appId || app?.siteId !== TARGET.siteId || app?.appPath !== 'webflow-cloud-test') {
    throw new Error('Webflow app identity does not match the approved Inputs target');
  }
  const environments = list(await run(['apps', 'environments', 'list', TARGET.appId, ...COMMON]), 'environments');
  const target = environments.filter(item => item.id === TARGET.environmentId);
  if (target.length !== 1 || target[0].branch !== TARGET.branch || target[0].mount !== TARGET.mount) {
    throw new Error('Webflow environment does not match approved main /test target');
  }
  const existing = list(await run(['apps', 'env-vars', 'list', ...FLAGS, ...COMMON]), 'variables');
  const names = new Map();
  for (const item of existing) {
    if (typeof item.key !== 'string' || names.has(item.key)) throw new Error('Invalid or duplicate variable metadata');
    names.set(item.key, item);
  }
  const values = new Map();
  for (const key of secrets) {
    if (names.has(key)) {
      if (names.get(key).isSecret !== true) throw new Error(`${key} exists without secret protection; repair explicitly`);
      continue; // Preserve existing credentials and the signing key on every retry.
    }
    if (key === 'RS_INPUTS_SESSION_SECRET') continue;
    if (typeof env[key] !== 'string' || !env[key].trim()) throw new Error(`Missing runtime credential: ${key}; no variables changed`);
    values.set(key, env[key]);
  }
  if (!names.has('RS_INPUTS_SESSION_SECRET')) {
    const secret = env.RS_INPUTS_SESSION_SECRET || generateSecret();
    if (typeof secret !== 'string' || !/^[a-fA-F0-9]{64}$/.test(secret)) throw new Error('Session signing secret must contain exactly 64 hexadecimal characters');
    values.set('RS_INPUTS_SESSION_SECRET', secret);
  }

  const updated = [];
  for (const [key, value] of [...values, ...Object.entries(plain)]) {
    // Values use stdin, including public values. They never appear in argv.
    await run(['apps', 'env-vars', 'set', key, ...FLAGS,
      ...(values.has(key) ? ['--secret'] : []), ...COMMON], value);
    updated.push(key);
  }
  const saved = list(await run(['apps', 'env-vars', 'list', ...FLAGS, '--fields', 'key,isSecret,value', ...COMMON]), 'variables');
  for (const key of summary.keys) {
    const matches = saved.filter(item => item.key === key);
    if (matches.length !== 1 || (secrets.includes(key) && matches[0].isSecret !== true)) {
      throw new Error(`Configuration metadata readback failed for ${key}; inspect before deploying`);
    }
    if (Object.hasOwn(plain, key) && (matches[0].isSecret !== false || matches[0].value !== plain[key])) {
      throw new Error(`Public configuration readback failed for ${key}; verify through Webflow metadata before deploying`);
    }
  }
  return { ...summary, updated, preservedSecrets: secrets.filter(key => names.has(key)),
    note: 'Metadata verified. Secret scope and application behavior still require runtime tests. No deployment triggered.' };
}

function cli(args, input) {
  // CLI_PATH optionally identifies a local JS entrypoint, including on Windows.
  const entry = process.env.WEBFLOW_CLI_PATH;
  const result = spawnSync(entry ? process.execPath : 'webflow', entry ? [entry, ...args] : args, {
    input, encoding: 'utf8', timeout: 60000, maxBuffer: 1024 * 1024,
    // npm's Windows shim needs cmd.exe; args contain only fixed metadata, never values.
    shell: !entry && process.platform === 'win32', windowsHide: true,
    env: { ...process.env, CI: '1' },
  });
  if (result.error || result.status !== 0) {
    // Do not relay CLI stdout/stderr: third-party failures may contain secrets.
    throw new Error('Webflow CLI failed. A current authenticated CLI or WEBFLOW_API_TOKEN is required; no login was started. Prior successful configuration writes may remain.');
  }
  try { return JSON.parse(result.stdout); } catch { throw new Error('Webflow CLI returned invalid JSON; response withheld'); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2);
  if (args.some(arg => !['--apply', '--dry-run'].includes(arg)) || (args.includes('--apply') && args.includes('--dry-run'))) {
    console.error('Usage: node scripts/configure-inputs-runtime.mjs [--dry-run|--apply]');
    process.exitCode = 1;
  } else {
    configure({ apply: args.includes('--apply') }).then(result => console.info(JSON.stringify(result, null, 2)))
      .catch(error => { console.error(error.message); process.exitCode = 1; });
  }
}
