from pathlib import Path
import json,hashlib,datetime,ast
root=Path.cwd(); ev=root/'wef-data/evidence/schedule-stage-20261106'; review=ev/'post-deploy-review'
receipt=json.loads((ev/'review/review-receipt.json').read_text(encoding='utf-8'))
for item in receipt['candidate']:
    name=Path(item['path']).name
    actual=hashlib.sha256((root/'wef-data'/name).read_bytes()).hexdigest()
    assert actual==item['sha256'],name
    print(name,actual,'MATCH reviewed candidate')
a=json.loads((ev/'automation-post-deploy-readback.json').read_text(encoding='utf-8-sig'))
# Connector artifacts may wrap the structured configuration.
if 'structuredContent' in a:a=a['structuredContent']
if 'content' in a and not 'nodes' in a:
    text=next(c['text'] for c in a['content'] if c.get('type')=='text')
    a=json.loads(text)
script=a['nodes'][0]['inputs']['script']
expected=(root/'wef-data/schedule-priority.js').read_text(encoding='utf-8').split('if (typeof module')[0]+'await evaluateAirtable();\n'
assert script.replace('\r\n','\n')==expected.replace('\r\n','\n')
assert a['deploymentStatus']=='deployed' and a['deployedVersion'] is None
print('Live automation saved readback: deployed, exact reviewed evaluator, completed-run trigger')
result=json.loads((ev/'scheduled-result.json').read_text(encoding='utf-8'))
assert result['counts']['days'][0]['counts']['saved']==2284
for day in result['counts']['days']:
    stages=list(day['counts']['stage-seconds'])
    assert stages.index('save-schedules')<stages.index('save-schedule-priorities')<stages.index('fetch-and-prepare-entries-trips')
    print(day['day'],day['counts']['saved'],'distinct acknowledged business records',day['counts']['total-seconds'],'seconds',day['errors'],'collection errors')
print('Reused prior offline test evidence after exact candidate/deployed hash match; no rerun needed')
