from pathlib import Path
import json,hashlib,datetime
r=Path(__file__).resolve().parent
def e(p):return {'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
skill=Path('C:/Users/gombc/.agents/skills/code-review/SKILL.md')
p=dict(reviewer='wef-live author',reviewed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),skill_sha256=e(skill)['sha256'],objective='Independent class live details with decoded acquisition paused',target=str(r),integration_coverage='Source/model/store and exact two-file boundary;6offlinechecks;scheduled acceptance separate',baseline=[e(x) for x in sorted((r/'baseline').rglob('*.js'))]+[e(r/'task-before.xml')],candidate=[e(x) for x in sorted((r/'candidate').rglob('*.js'))]+[e(r/'pause.test.cjs')],report=e(r/'review.md'),logs=[e(r/'offline.json')],review_status='COMPLETE',candidate_status='PASS',required_checks=[dict(name='paused decoded and independent live details suite',requirement='execute',status='executed',command_ids=['pause-decoded-offline']),dict(name='approved delta,source and native link contract',requirement='inspect',status='inspected',reason='Inspected actual2file diff, explicit null model branch, model prior behavior, lane-store PATCH semantics and preserved routes/task/cadence/cleanup',evidence=[e(r/'TASK.md'),e(r/'review.md')])])
(r/'review-receipt.json').write_text(json.dumps(p,indent=2))
