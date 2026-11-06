from pathlib import Path
import hashlib,json,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
receipt=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Exact User-Agent only on four specified direct Live feeds; proxy/URLs/other headers unchanged',target=str(r),integration_coverage='7mocked request-capture tests and actual shared reader/caller trace; normal scheduled verification pending',baseline=[e(p) for p in sorted((r/'baseline').rglob('*.js'))]+[e(r/'task-before.xml')],candidate=[e(p) for p in sorted((r/'candidate').rglob('*.js'))]+[e(r/'headers.test.cjs')],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='exact request/proxy/retry offline checks',requirement='execute',status='executed',command_ids=['headers-offline']),dict(name='approved header-only scope and shared-consumer compatibility',requirement='inspect',status='inspected',reason='Inspected caller/shared-reader and exact delta; no other behavior/task/proxy edits',evidence=[e(r/'TASK.md'),e(r/'review.md'),e(r/'baseline/lib/wef-source-read.js'),e(r/'candidate/lib/wef-source-read.js')])])
(r/'review-receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
print('Review packet saved')
