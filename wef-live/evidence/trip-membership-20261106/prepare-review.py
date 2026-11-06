from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
p=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Owner-approved missing-trip membership and widget null handling',target=str(r),integration_coverage='18 mocked membership/widget/header/cadence checks and exact two-file diff;natural proof separate',baseline=[e(x) for x in sorted((r/'baseline').rglob('*.js'))]+[e(r/'task-before.xml')],candidate=[e(x) for x in sorted((r/'candidate').rglob('*.js'))]+[e(r/'retry.test.cjs'),e(r/'headers.test.cjs'),e(r/'membership.test.cjs')],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='membership/null and protected behavior suite',requirement='execute',status='executed',command_ids=['trip-membership-offline']),dict(name='approved delta/existing task and consumer preservation',requirement='inspect',status='inspected',reason='Inspected membership status-only patches,source projection completeness,null link clearing and exact two-file approval boundary',evidence=[e(r/'TASK.md'),e(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(p,indent=2),encoding='utf-8')
print('Review packet saved')

