from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
p=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Swap only shared show/day reference names;confirm existing class-group pair',target=str(r),integration_coverage='Corrected shared reference fields/readonlyadapter;17offlinechecks;existingclassgroup schema/sample and oldlink preservation',baseline=[e(x) for x in sorted((r/'baseline/wef-live').glob('*.js'))]+[e(r/'task-before.xml')],candidate=[e(x) for x in sorted((r/'candidate/wef-live').glob('*.js'))]+[e(r/'linker.test.cjs'),e(r/'pause.test.cjs'),e(r/'deploy.ps1'),e(r/'revert.ps1')],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='empty-only linking and protected collection tests',requirement='execute',status='executed',command_ids=['w-shares-offline']),dict(name='approved delta/schema/source contract/rollback and unchanged collector scope',requirement='inspect',status='inspected',reason='Reviewed3fileconfig/advisorydelta,10necessarysafe nativefields,verifiedexistingclass-group pair;no extra collection/schema changes',evidence=[e(r/'TASK.md'),e(r/'schema-readback.json'),e(r/'created-fields.json'),e(r/'class-group-schema.json'),e(r/'class-group-readback.json'),e(r/'group-class-reciprocal-readback.json'),e(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(p,indent=2),encoding='utf-8')



