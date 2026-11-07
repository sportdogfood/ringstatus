from pathlib import Path
import json,hashlib,collections,subprocess,datetime
root=Path.cwd();ev=root/'wef-data/evidence/cleanup-handoff-20261106'
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
receipt=load(ev/'review/review-receipt.json')
for item in receipt['candidate']:
 p=root/'wef-data'/Path(item['path']).name
 assert hashlib.sha256(p.read_bytes()).hexdigest()==item['sha256']
removals=load(ev/'scheduled-local-removals.json');totals={k:sum(r['counts'][k] for r in removals) for k in ('runs','errors','acks')}
for file in sorted({r['file'] for r in removals}):
 lines=(root/file).read_bytes().replace(b'\x00',b'').decode('utf-8',errors='replace').splitlines();actual=[]
 for line in lines:
  if line.startswith('{'):
   try:d=json.loads(line)
   except ValueError:continue
   if 'localLogCleanup' in d:actual.append(d['localLogCleanup'])
 wanted=collections.Counter(json.dumps(r['counts'],sort_keys=True) for r in removals if r['file']==file)
 found=collections.Counter(json.dumps(r,sort_keys=True) for r in actual)
 assert all(found[k]>=v for k,v in wanted.items()),file
before=load(ev/'local-before.json');after=load(ev/'local-after.json')
expected=[Path(p).stem for value in before['workflows'].values() for p in value['pending']]
runs=load(ev/'prior-pending-delivered.json')['records'];errors=load(ev/'prior-pending-errors-delivered.json')['records']
found={r['cellValuesByFieldId']['fldHstpbMoewwGrQ1'] for r in runs}|{r['cellValuesByFieldId']['fldIrMLgl8Dp9ZHw0'] for r in errors}
assert set(expected)<=found
retained=[]
for workflow,value in after['workflows'].items():
 for relative in value['pending']:
  p=root/'wef-data/runtime/logging'/workflow/relative
  entry={'workflow':workflow,'file':relative,'exists':p.exists()}
  if p.exists():
   r=load(p);entry.update(status=r.get('status'),completed=r.get('completed'))
  retained.append(entry)
p=subprocess.run(['node',str(ev/'inspect-local-logs.js')],capture_output=True,text=True,encoding='utf-8',check=True)
current=json.loads(p.stdout)
summary={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'deployed_hashes_match_review':True,'scheduled_invocations':len(removals),'confirmed_removal_totals':totals,'initial_pending':len(expected),'pending_readback_runs':len(runs),'pending_readback_errors':len(errors),'after_snapshot_pending_files':retained,'current_inventory_at':current['at'],'current_workflows':{w:{'counts':v['counts'],'pending':v['pending']} for w,v in current['workflows'].items()}}
print(json.dumps(summary,indent=2))
