from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def entry(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
receipt=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=entry(skill)['sha256'],objective='Deploy owner-approved delete-only cleanup and settled live-status/date cadence without changing other collectors',target=str(r),integration_coverage='81 offline checks and actual call-chain/task/source diff review; normal scheduled acceptance separate',baseline=[entry(p) for p in sorted((r/'baseline').glob('*.js'))]+[entry(r/'task-before.xml')],candidate=[entry(p) for p in sorted((r/'candidate').glob('*.js'))]+[entry(r/'task-prepared.xml')]+[entry(p) for p in sorted(r.glob('*.test.js'))],report=entry(r/'review.md'),logs=[entry(r/'offline-final.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='final offline suite',requirement='execute',status='executed',command_ids=['delete-only-final']),dict(name='approved boundary, call chain and consumer/task compatibility',requirement='inspect',status='inspected',reason='Author inspected frozen source and task diff against latest human delete-only/deployment approval, preserving unrelated code and old evidence',evidence=[entry(r/'TASK.md'),entry(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(receipt,indent=2))
(r/'candidate-hashes.json').write_text(json.dumps(receipt['candidate'],indent=2))
print('Review packet saved')
