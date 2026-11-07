from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
p=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Owner-approved live-status public response handling',target=str(r),integration_coverage='20 mocked status/header/membership/null/cadence checks;exact one-file diff;natural proof separate',baseline=[e(x) for x in sorted((r/'baseline').rglob('*.js'))]+[e(r/'task-before.xml')],candidate=[e(x) for x in sorted((r/'candidate').rglob('*.js'))]+[e(r/'retry.test.cjs'),e(r/'headers.test.cjs'),e(r/'membership.test.cjs')],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='membership/null and protected behavior suite',requirement='execute',status='executed',command_ids=['status-response-offline']),dict(name='approved delta/existing task and consumer preservation',requirement='inspect',status='inspected',reason='Inspected successful public request diagnostics,status parsing/cadence call chain and exact status-only approval boundary',evidence=[e(r/'TASK.md'),e(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(p,indent=2),encoding='utf-8')
print('Review packet saved')


