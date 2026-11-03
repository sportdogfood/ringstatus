from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
receipt=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Approved daily full-log clear, three-hour threshold check and new-show full-clear in existing task',target=str(r),integration_coverage='75 offline tests plus adapter/task/source contract inspection; actual scheduled proof separately pending',baseline=[e(p) for p in sorted((r/'baseline').glob('*.js'))],candidate=[e(p) for p in sorted((r/'candidate').glob('*.js'))]+[e(p) for p in sorted(r.glob('*.test.js'))],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='offline revised behavior and collection regression suite',requirement='execute',status='executed',command_ids=['revised-cleanup-offline']),dict(name='approved change boundary and exact downstream deletion/protection contract',requirement='inspect',status='inspected',reason='Reviewed actual frozen source/adapter/scheduled call chain against direct approval; no task/schema/dependency changes',evidence=[e(r/'TASK.md'),e(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
print('Review packet saved')
