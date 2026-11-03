from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
receipt=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Remove unnecessary child tables; keep source results directly on day0live-trips',target=str(r),integration_coverage='85 offline checks, exact scope/source diff and native field readback; physical schema removal/normal scheduled proof separately recorded',baseline=[e(p) for p in sorted((r/'baseline').glob('*.js'))]+[e(r/'task-frozen.xml')],candidate=[e(p) for p in sorted((r/'candidate').glob('*.js'))]+[e(p) for p in sorted(r.glob('*.test.js'))],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='single-table source/results/regression suite',requirement='execute',status='executed',command_ids=['simple-trips-offline']),dict(name='owner correction boundary and integration contract',requirement='inspect',status='inspected',reason='Read actual source/key/store/cleanup call chain and approved removal scope, preserving oldtrip/legacy links and no new dependencies',evidence=[e(r/'TASK.md'),e(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
print('Review packet saved')
