import pathlib,json,hashlib,datetime
root=pathlib.Path.cwd();ev=root/'evidence/events-20261107';out=ev/'correction-1/review';skill=pathlib.Path(r'C:/Users/gombc/.agents/skills/code-review')
def item(p):
 p=pathlib.Path(p);return {'path':str(p.resolve()),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
prior=json.loads((ev/'review/review-receipt.json').read_text());candidate=json.loads((out/'candidate-before.json').read_text())
assert all(item(c['path'])['sha256']==c['sha256'] for c in candidate)
oldhash={pathlib.Path(c['path']).name:c['sha256'] for c in prior['candidate']}
assert item(ev/'correction-1/cleaner.js')['sha256']==oldhash['cleaner.js'];assert item(ev/'correction-1/events.test.js')['sha256']==oldhash['events.test.js']
for c in candidate:
 if pathlib.Path(c['path']).name not in ['cleaner.js','events.test.js']:assert c['sha256']==oldhash[pathlib.Path(c['path']).name]
protected=[]
for c in json.loads((ev/'baseline.json').read_text(encoding='utf-8-sig')):
 if pathlib.Path(c['Path']).name=='README.md':continue
 p=item(c['Path']);protected.append({**p,'unchanged':p['sha256']==c['Hash'].lower()})
assert all(p['unchanged'] for p in protected)
manifest={'candidate':candidate,'baseline_correction':[item(ev/'correction-1/cleaner.js'),item(ev/'correction-1/events.test.js')],'protected_originals':protected,'dependencies':[item(ev/p) for p in ['task.md','schema-after.json','cleanup-field-configs.json','review/review.md','correction-1/authorization.md']],'unchanged_counterexamples':item(out/'counterexamples.cjs')}
assert item(out/'counterexamples.cjs')['sha256']==item(ev/'review/counterexamples.cjs')['sha256']
(out/'source-manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf8')
report='''# Cleaner correction 1 independent review

Reviewer /root/workflow_review. Review COMPLETE; candidate PASS for the approved two corrections and bounded offline scope. No actionable findings in the correction diff. Initial FAIL report/candidate/logs remain preserved in ../../review/ and ../cleaner.js / ../events.test.js.

## Findings closed

- PASS: priority-zero show IDs now guard completed run-log and matching error eligibility for daily/cap/new-show, using show ID because run logs do not carry customer/date. This conservatively preserves a show with any matching protected day. Existing snapshot day-specific protection remains unchanged.
- PASS: proven original terminal Success status is recognized alongside Complete/Failed, with valid prior-day completion still required. Partial/Running/unknown remain protected. The retained original runtime is unchanged.

Independently executed24-test suite once on Node v22.22.2. Original22 tests retained, with2 appended regression groups exercising both corrections under all3 cleaner reasons, including >1000 cap tables. Result24/24 PASS. Independently executed the unchanged original2counterexamples once: priority-zero run/error/snapshot all retained and no deletes; prior-day legacy Success log removed. Exact commands, cwd, UTC timestamps, runtime, exits and stdout/stderr are in command-log.json. Mock deletion observations are offline proof only.

## Approval boundary and integration trace

Read authorization.md and exact saved-failed-versus-current diffs. Production delta is only cleaner.js: deriving currentShows from existing priority-zero day keys, recognizing known Success, and checking protected show before all log/error deletion reasons. Test delta only appends the2focused groups; no original assertions/fixtures changed. Saved failed cleaner/test hashes match original failed review receipt. All4other candidate sources remain unchanged against that receipt. All original module/config/package/schema files in baseline.json remain byte-identical except separately authorized README edits excluded from frozen implementation review. No extra schema, runtime, dependency, gate, source acquisition, retry or old implementation retrofit introduced.

Inspected current changes through cleaner inputs, terminal/time/scope filters, existing delete batching/readback, error propagation and cleanup native write fields. Existing owned-table allowlist, protected tables, current-day/active/ambiguous states, snapshot Expected/Conflict/current-show protection, oldest500eligible cap behavior and no ambiguous-delete retry remain intact. Actual prior schema/config trace remains applicable: cleanup supports Success single-select and failure null+error; five native snapshot links and inputs are unchanged. Initial event/listener/X/Y/linker/readback/source identity review is reused as unchanged evidence; this correction does not claim a fresh native execution of it.

## Outstanding downstream

Native9draft saves, exact saved source/config and disabled-state readback remain coordinator-owned acceptance. Native service execution, event order/concurrency, deletion readback on actual records, schedules and downstream consumption remain UNVERIFIED while inactive. Administrative schema/setup evidence does not prove business operation. No live source/API/business calls, product edits, draft saves or activation were performed by reviewer.

Ready for already-authorized disabled draft preparation/readback by coordinator. This offline PASS is not a business run or activation permission. Evidence packet includes review.md, command-log.json, source-manifest.json, review-receipt.json and verifier-output.json. Packet verifier establishes integrity only.
'''
(out/'review.md').write_text(report,encoding='utf8')
receipt={'reviewer':'/root/workflow_review','reviewed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'skill_sha256':item(skill/'SKILL.md')['sha256'],'objective':'Independent correction review of exactly two separately owner-approved cleaner defects; execute focused24suite and original2counterexamples once.','target':'Failed cleaner/test from original review compared with frozen corrected candidate; original HEAD73699ab4cd2801b8642cc1ff25b8f4290e0b4330 protected.','integration_coverage':'Priority-zero all-reason run/error protection, known terminal producer compatibility, cap/scope/time/error/readback continuity, unchanged event/linker/native schema dependencies; live drafts/cadence unverified.','baseline':manifest['baseline_correction'],'candidate':candidate,'report':item(out/'review.md'),'logs':[item(out/'command-log.json')],'review_status':'COMPLETE','candidate_status':'PASS','required_checks':[{'name':'independent24test suite','requirement':'execute','status':'executed','command_ids':['events-suite']},{'name':'original2counterexamples','requirement':'execute','status':'executed','command_ids':['cleaner-counterexamples']},{'name':'approval boundary and exact delta','requirement':'inspect','status':'inspected','reason':'Inspected direct owner approval as conveyed by coordinator and recorded authorization, actual minimal diffs, frozen failed candidate hashes and original baseline; no product changes by reviewer.','evidence':[item(ev/'correction-1/authorization.md'),item(out/'source-manifest.json')]},{'name':'integration and protected behavior continuity','requirement':'inspect','status':'inspected','reason':'Reviewed new predicates through unchanged native schema/readback/error/deletion path; prior unchanged source review reused, not claimed as live execution.','evidence':[item(ev/'review/review.md'),item(out/'source-manifest.json'),item(ev/'schema-after.json'),item(ev/'cleanup-field-configs.json')]}]}
(out/'review-receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf8');print(json.dumps(candidate,indent=2))
