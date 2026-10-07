import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {paths, read} from '../.codex/hooks/reliability.mjs';
const source = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rs-start-test-'));
try {
  spawnSync('git', ['init', '--quiet', root]);
  for (const session of ['first', 'second']) {
    const p = spawnSync(process.execPath, [path.join(source,'.codex/hooks/session_start.mjs')], {
      cwd:root, encoding:'utf8', input:JSON.stringify({cwd:root, session_id:session, hook_event_name:'SessionStart', source:'startup'})
    });
    assert.equal(p.status, 0, p.stderr);
    assert.match(JSON.parse(p.stdout).hookSpecificOutput.additionalContext,/No task is bound/);
    assert.equal(read(paths(root,session).state).session_id,session);
  }
  console.log('PASS isolated session startup');
} finally { fs.rmSync(root,{recursive:true,force:true}); }
