"""Package the approved Schedule V2 implementation; do not publish or activate feeds."""
from pathlib import Path
import argparse, hashlib, json, shutil

parser=argparse.ArgumentParser()
parser.add_argument('--source', required=True, type=Path)
args=parser.parse_args()
source=args.source.resolve()
target=Path(__file__).resolve().parents[1]/'public'/'wef-schedule-v2'
files=['schedule.html','day.html','app.js','comments.js','components.js','data.js',
       'data-diff.js','day.js','device-data.js','device-location.js','drag-scroll.js',
       'favicon.svg','fonts.css','package-store.js','rings.css','schedule-data.js',
       'theme-mode.js','theme.css','v2.css']
files += [str(p.relative_to(source)).replace('\\','/') for p in (source/'assets').rglob('*') if p.is_file()]
target.mkdir(parents=True,exist_ok=True)
manifest={}
for name in files:
    content=(source/name).read_bytes()
    if name in ['schedule.html','day.html']:
        content=content.decode('utf-8').replace('<body class="rings-app"','<body data-trainer-filter class="rings-app"').encode('utf-8')
    output=target/name
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_bytes(content)
    manifest[name]=hashlib.sha256(content).hexdigest()
(target/'package-manifest.json').write_text(json.dumps({'source':'Schedule V2; October30 seed; prepared two-lane client','feedActive':False,'files':manifest},indent=2),encoding='utf-8')
print(f'Prepared {len(files)} files in {target}. Feed inactive; nothing deployed.')
