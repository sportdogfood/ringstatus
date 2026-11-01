import pathlib,hashlib,json,datetime,subprocess
root=pathlib.Path.cwd(); parent=root/'wef-results/evidence/workflow-20261101'; out=parent/'correction-1/review'; skill=pathlib.Path(r'C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.8/skills/code-review')
def item(p):
 p=pathlib.Path(p);return {'path':str(p.resolve()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
baseline=[item(parent/'correction-1/workflow-before.js'),item(parent/'correction-1/workflow-test-before.js')]
candidate=[item(root/'wef-results/workflow.js'),item(root/'wef-results/test/workflow.test.js')]
assert baseline[0]['sha256']=='6a448a972a97ead8ff788d58a9a60fe6093a4470bb58d392239d5b6475783c40'
assert baseline[1]['sha256']=='f5071efccd5e335cd32c4d72d5aeac1a59752cc2beddcacd171b38023921da99'
protected=[]
for rel in ['wef-results/package.json','wef-results/schema.json','wef-results/runner.js','wef-results/reader.js']:
 b=subprocess.check_output(['git','show','00f4271b8d4be7eba9639b7a711119f2104d17ac:'+rel]);p=root/rel
 protected.append({**item(p),'baseline_sha256':hashlib.sha256(b).hexdigest(),'unchanged':p.read_bytes()==b})
assert all(p['unchanged'] for p in protected)
(out/'source-manifest.json').write_text(json.dumps({'baseline':baseline,'candidate':candidate,'protected':protected,'dependencies':[item(parent/'task.md'),item(parent/'correction-1/authorization.md'),item(parent/'airtable-schema-before.json'),item(parent/'automation-definitions.json'),item(out/'counterexamples.cjs')]},indent=2),encoding='utf8')
report='''# Correction 1 independent review

Reviewer /root/workflow_review. Review COMPLETE; candidate PASS for the approved correction delta and bounded offline scope. No actionable findings. Original initial review FAIL and logs remain unchanged in ../../review/.

## Findings closed with independent evidence

- PASS: clock-only result refresh retains X/expectation Complete and preserves original saved observation evidence. Business comparison strips observedAt and orders instances by record ID; saved historical payload is not rewritten.
- PASS: existing output is checked even when current qualification is false. Reverted flags or zero-only measurements flag Conflict while preserving accepted payload.
- PASS: reconciliation also traverses saved X/Y/expectation keys whose trip is absent, filters by matching dynamic priority-zero show-day, and flags absent-source/duplicate conflicts without creating result rows or deleting/repairing source/output.

## Execution and boundary

Independently executed `node --test test/workflow.test.js` once, Node v22.22.2: 15/15 PASS. Independently executed the original three counterexamples once, with the original diagnostic code copied unchanged: unchanged clock X Complete; zero/reverted Y Conflict; both missing-trip duplicate X records Conflict. command-log.json contains actual cwd, runtime, timestamps, exits and separate stdout/stderr. Diagnostic exit 0 describes successful observation; the printed semantic outcomes close the original failures.

Inspected the full exact diff against preserved workflow-before.js and workflow-test-before.js. Both preserved hashes match the previously failed frozen candidate. Only the three approved reconciliation fixes and focused regression tests were added. Existing original twelve tests were retained. Parent coordinator supplied direct owner approval; authorization.md records that exact scope. No new table/schema/runtime/package/dependency/gate or alternate production route was added. package.json/schema.json/runner.js/reader.js byte hashes still match HEAD 00f4271b8d4be7eba9639b7a711119f2104d17ac. Other-owner changes and concurrent README status edits are excluded.

The captured native schema, five prepared definitions, producer/consumer/error/readback trace from the initial review remain applicable because those interfaces and logging path are unchanged. Snapshot writes still read back; reconciliation never creates X/Y results; present and absent-source conflicts retain saved evidence. This is an offline proof using mock Airtable state, not native service behavior.

## Outstanding downstream acceptance

UNVERIFIED: native automation draft saves and readback, exact saved script/config, and disabled state. Native Airtable runtime compatibility, delivery/order/concurrency and actual ten-minute cadence remain unverified while inactive. Draft save/disabled readback is the coordinator's already-authorized next step; this reviewer performed no connector or business calls. Activation and business execution remain prohibited. Candidate PASS here does not claim complete workflow delivery or live acceptance.

Evidence: command-log.json, counterexamples.cjs, source-manifest.json, review-receipt.json, verifier-output.json. No product edits performed by reviewer.
'''
(out/'review.md').write_text(report,encoding='utf8')
receipt={'reviewer':'/root/workflow_review','reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'skill_sha256':item(skill/'SKILL.md')['sha256'],'objective':'Independent bounded re-review of exactly three owner-approved corrections and focused offline checks.','target':'Preserved original failed workflow/test hashes compared with corrected workflow.js/test; no activation or business calls.','integration_coverage':'Exact approved delta, original counterexamples, fifteen offline cases, preserved dependency hashes and schema/interface continuity. Native saves/disabled readback and live delivery/cadence remain unverified coordinator-owned acceptance.','baseline':baseline,'candidate':candidate,'report':item(out/'review.md'),'logs':[item(out/'command-log.json')],'review_status':'COMPLETE','candidate_status':'PASS','required_checks':[{'name':'independent focused fifteen-test suite','requirement':'execute','status':'executed','command_ids':['offline-suite']},{'name':'original three counterexamples','requirement':'execute','status':'executed','command_ids':['counterexamples']},{'name':'approval-boundary and frozen delta inspection','requirement':'inspect','status':'inspected','reason':'Inspected exact original-versus-current code/test diff, direct approval conveyed by coordinator and recorded scope; no new gate, schema, runtime or dependency change.','evidence':[item(parent/'correction-1/authorization.md'),item(out/'source-manifest.json')]},{'name':'integration/schema/failure-path continuity','requirement':'inspect','status':'inspected','reason':'Reused actual unchanged schema/interfaces and original read-only dependency trace; checked corrections preserve source clocks, identity and write/readback behavior. Live checks outside reviewer authority.','evidence':[item(parent/'airtable-schema-before.json'),item(parent/'review/review.md'),item(out/'source-manifest.json')]}]}
(out/'review-receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf8');print(json.dumps(candidate,indent=2))
