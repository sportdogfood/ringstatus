import sys,json,pathlib,tempfile,copy
root=pathlib.Path(sys.argv[1]);sys.path[:0]=[str(root/'wef-data'),str(root/'wef-schedules/tests')]
from day_counts import saved_day_summaries
from data_contract import packed,digest
from archive_evidence import EvidenceStore
fixture=json.loads((root/'wef-shows/tests/fixtures/day-summary.json').read_text());context={**fixture['context'],'canonical-days':[fixture['context']['selected-date']]}
with tempfile.TemporaryDirectory() as tmp:
 runtime=pathlib.Path(tmp);store=EvidenceStore(runtime/'retained-evidence',runtime/'archive-repository');state=json.loads(fixture['runs'][fixture['head']]['state-json']);state.update({'publication-day':context['selected-date'],'phase':'Complete','pending':None});state.pop('day-work',None)
 state['observations']=store.stage(context['show-key'],[fixture['collections'][ref['key']] for ref in state['observations']])
 body={'format':'wef-data-day-save-v1','run-key':fixture['head'],'state':state};file=runtime/'completed-days/test.json';file.parent.mkdir();file.write_text(packed(body));head=runtime/'day-heads'/context['show-key'].replace('|','-')/(context['selected-date']+'.json');head.parent.mkdir(parents=True);head.write_text(json.dumps({'path':'completed-days/test.json','sha256':digest(packed(body))}))
 first=saved_day_summaries(runtime,context);assert first['mode']=='saved-day' and len(first['days'])==1
 for name in ['unique-entry-count','proven-trip-count','accepted-as-of']:assert first['days'][0][name]==fixture['expected'][0]['body'][name],name
 assert saved_day_summaries(runtime,context)==first
 bad=copy.deepcopy(body);bad['run-key']='tampered';file.write_text(packed(bad))
 try:saved_day_summaries(runtime,context)
 except ValueError:pass
 else:raise AssertionError('Changed receipt accepted')
 file.write_text(packed(body));ref=state['observations'][0];store.local_path(ref).write_bytes(b'corrupt')
 try:saved_day_summaries(runtime,context)
 except Exception:pass
 else:raise AssertionError('Changed evidence accepted')
print('PASS saved-day counts match frozen original, repeat unchanged, corrupt receipt/evidence rejected; no network or business writes')
