import json,hashlib
from pathlib import Path
root=Path('wef-live');m=json.loads((root/'evidence/shows-time-20261107/deployed.json').read_text(encoding='utf-8-sig'))
for x in m['hashes']:
 assert hashlib.sha256((root/x['file']).read_bytes()).hexdigest()==x['sha256'],x['file']
for name in ['lanes.js','model.js','lane-store.js','schema.js','airtable.js']:
 assert (root/name).read_bytes()==(root/'evidence/pause-decoded-20261107/candidate/wef-live'/name).read_bytes(),name
old=json.loads((root/'evidence/pause-decoded-20261107/offline.json').read_text(encoding='utf-8-sig'));assert old['commands'][0]['exit_code']==0;assert 'ok 6 - completed:null is an empty list without warnings; record and other ring links remain' in old['commands'][0]['stdout']
print(json.dumps({'installed_runtime_hashes_matched':len(m['hashes']),'null_path_exact_frozen_dependencies':5,'prior_null_test_output':'PASS;inspected reuse,not rerun','new_runtime_change':False,'new_deployment_needed':False}))
