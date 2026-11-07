from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
p=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Status decision uses published Shows Eastern time and priority0',target=str(r),integration_coverage='Canonical lookup/context/decision;9offlinechecks;onefileboundary;normal acceptance separate',baseline=[e(x) for x in sorted((r/'baseline').rglob('*.js'))]+[e(r/'task-before.xml')],candidate=[e(x) for x in sorted((r/'candidate').rglob('*.js'))]+[e(r/'shows-time.test.cjs'),e(r/'retry.test.cjs')],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='Shows time and preserved retry suite',requirement='execute',status='executed',command_ids=['shows-time-offline']),dict(name='approved delta,source and native link contract',requirement='inspect',status='inspected',reason='Inspected actual1file diff, published lookup and decision fallback, preserved status false/error cadence and paused collection',evidence=[e(r/'TASK.md'),e(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(p,indent=2))

