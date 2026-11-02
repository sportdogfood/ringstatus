from pathlib import Path
import json,datetime
r=Path(__file__).resolve().parent
first=json.loads((r/'normal-1300.json').read_text())
receipt={'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'deployment':'PASS: installed24 JS hashes and semantic existing-task readback','offline':'PASS:81','review':'PASS: author review; not independent','first_normal_check':{'result':first['result'],'run_id':first['check']['run_id'],'started':first['check']['started'],'live_status':first['check']['live_status'],'heartbeat_record':first['heartbeat'][0]['id'],'run_log_record':first['run_logs'][0]['id'],'errors':len(first['errors']),'cleanup_run_id':first['cleanup']['run_id'],'cleanup_result':first['cleanup']['result'],'cleanup_actions':len(first['cleanup']['actions'])},'second_hourly_check':{'status':'UNVERIFIED: not yet due','due':first['check']['next_check_at']},'natural_cleanup_branches':'UNVERIFIED: no new show or Eastern day occurred after deployment; offline proof is not natural deletion proof','populated_activity':'UNVERIFIED: source live=false','manual_runs':False,'record_repairs':False,'observers':False,'existing_archives':'Preserved; new cleanup code does not write archives or deleted-record field backups','known_findings':'Two preexisting conditional cleanup-reporting P2 findings retained in review.md'}
(r/'acceptance.json').write_text(json.dumps(receipt,indent=2))
root=r.parents[1]
note='**Scheduled readback:** November2,1:00p.m.Eastern check `0de37502-dff3-4d90-8d06-66c4e487c709` completed with task exit0. Source live=false; heartbeat/run-log both Skipped/completed, no errors. Cleanup correctly Skipped with zero actions on unchanged show/day. Next actual hourly check is2:00p.m.; second hourly check and future event deletion remain unverified. See [acceptance](evidence/delete-only-20261102/acceptance.json).\n\n'
for name in ('README.md','scope.md','HANDOFF.md'):
 p=root/name;s=p.read_text(encoding='utf-8');marker='## Prior dated documentation — historical only\n\n';s=s.replace(marker,note+marker,1);p.write_text(s,encoding='utf-8')
print(json.dumps(receipt['first_normal_check']))
