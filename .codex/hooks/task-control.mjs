// Local bookkeeping only. Does not run tests, business processes or external calls.
import fs from 'node:fs';
import { repoRoot } from './common.mjs';
import { paths, stateFor, read, save, hash, event, validateContract, evaluate } from './reliability.mjs';

const [command, session, file] = process.argv.slice(2);
try {
  const root = repoRoot();
  const p = paths(root, session);
  const state = stateFor(p, session);
  if (command === 'bind' || command === 'new-task') {
    if (fs.existsSync(p.contract) && command === 'bind') throw new Error('A task is already bound; use amend for a correction or new-task for an explicitly replaced goal');
    const c = read(file);
    validateContract(c);
    if (c.session_id !== session) throw new Error('Session mismatch');
    if (fs.existsSync(p.contract)) {
      if (!c.change_reason || c.source_prompt_revision !== state.prompt_revision || state.prompt_revision <= state.bound_prompt_revision || c.task_id === read(p.contract).task_id)
        throw new Error('New task needs a different task ID, newer instruction revision and explicit replacement reason');
      save(`${p.contract}.previous-${state.events.length}.json`, read(p.contract));
      if (fs.existsSync(p.receipt)) save(`${p.receipt}.previous-${state.events.length}.json`, read(p.receipt));
    }
    save(p.contract, c);
    state.bound_prompt_revision = state.prompt_revision;
    state.baseline = hash(JSON.stringify(c));
    event(p, state, 'bound', { task_id: c.task_id, contract_hash: state.baseline, prompt_revision: state.prompt_revision });
  } else if (command === 'amend') {
    const c = read(file);
    validateContract(c);
    const old = read(p.contract);
    if (hash(JSON.stringify(old)) !== state.baseline) throw new Error('Existing contract was changed outside the recorded amendment path');
    if (c.session_id !== session || c.task_id !== old.task_id) throw new Error('Amend cannot switch tasks or sessions');
    if (!c.change_reason || c.source_prompt_revision !== state.prompt_revision || state.prompt_revision <= state.bound_prompt_revision)
      throw new Error('Amend needs the newer user instruction revision and an explicit change reason');
    // Version history records interpretation, not proof of human authorization.
    save(`${p.contract}.revision-${state.events.length}.json`, old);
    save(p.contract, c);
    state.baseline = hash(JSON.stringify(c));
    state.bound_prompt_revision = state.prompt_revision;
    event(p, state, 'amended', { task_id: c.task_id, reason: c.change_reason, prompt_revision: state.prompt_revision });
  } else if (command === 'receipt') {
    const receipt = read(file);
    if (receipt.session_id !== session) throw new Error('Session mismatch');
    save(p.receipt, receipt);
    event(p, state, 'receipt-recorded', { task_id: receipt.task_id });
  } else if (command !== 'status') throw new Error('Usage: task-control.mjs bind|amend|new-task|receipt|status SESSION [JSON_FILE]');
  const current = stateFor(p, session);
  console.log(JSON.stringify({ ...evaluate(root, { session_id: session }), contract_path: p.contract, receipt_path: p.receipt,
    contract_hash: current.baseline, prompt_revision: current.prompt_revision }, null, 2));
} catch (error) { console.error(error.message); process.exitCode = 1; }
