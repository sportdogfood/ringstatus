from pathlib import Path
import json,datetime
r=Path(__file__).resolve().parent
a=json.loads((r/'normal-1521.json').read_text());b=json.loads((r/'normal-1524.json').read_text());ta=json.loads((r/'task-1521.json').read_text(encoding='utf-8-sig'));tb=json.loads((r/'task-1524.json').read_text(encoding='utf-8-sig'))
assert a['result']==b['result']=='PASS' and ta['result']==tb['result']==0
assert '15:21:' in ta['last'] and '15:24:' in tb['last']
assert any('15:24:' in x and 'exit=0' in x for x in b['taskLog'])
assert a['controls']==b['controls']
receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'correction':'Removed three empty unnecessary tables and their obsolete cleanup controls; no business records removed','current_model':'live-trips only; placing,position,results-verified,class-status,results-json directly on trip; existing trip identities/native links retained','schema':'GET-confirmed absent deleted tables and obsolete controls; new result fields verified; former reciprocal fields converted by Airtable to empty plain text,not linked','offline':'85PASS; author review,not independent','deployment':'25installed JS hashes match reviewed candidate;21other JS/task unchanged','scheduled':'15:21 and15:24Eastern normal dispatcher wakes exit0,correctly skipped notdue work; single live-trips cleanup control retained','population':'Still unverified under natural populated day0 source; current live=false, no manual run/forced event/backfill','manual_runs':False,'record_repairs':False,'new_schedule':False}
(r/'acceptance.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
note='**Normal scheduled verification:** November3 **3:21 and3:24p.m.Eastern** wakes completed exit0. Quiet collection/cleanup correctly skipped; exactly one live-trips cleanup control remains.25runtime hashes and task match. New class-source population remains naturally pending while live=false;85offline checks passed. [Acceptance](evidence/live-trips-simple-20261103/acceptance.json).\n\n'
for name in ('README.md','scope.md','HANDOFF.md'):
 p=r.parents[1]/name;s=p.read_text(encoding='utf-8');marker='## Earlier dated documentation — historical where superseded above\n\n';s=s.replace(marker,note+marker,1);p.write_text(s,encoding='utf-8')
print(json.dumps(receipt))
