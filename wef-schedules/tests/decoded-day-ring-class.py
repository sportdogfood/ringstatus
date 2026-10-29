"""Isolated GET-only test using existing decoded schedule/class routes.
No production collector imports, Airtable writes, configuration or task changes.
"""
import argparse, concurrent.futures, datetime, hashlib, json, pathlib, statistics, subprocess, time, urllib.parse
from wef_data_context import load_context, validate_source_show
from wef_data_airtable import Airtable, canonical_fence

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--date');parser.add_argument('--output',required=True);parser.add_argument('--show-context');parser.add_argument('--capture',action='store_true');args=parser.parse_args()
    authority_client=None if args.show_context else Airtable()
    authority_before=None if args.show_context else authority_client.canonical_context()
    context=load_context(args.show_context) if args.show_context else next(r for r in authority_before['records'] if r['default'])
    requested_day=datetime.date.fromisoformat(args.date) if args.date else None
    OUT=pathlib.Path(args.output).resolve() if args.output else pathlib.Path(__file__).resolve().parents[1]/'evidence/decoded-day-ring-class-20261024'
    if OUT.exists() and any(OUT.iterdir()):raise ValueError('Refusing to overwrite an existing source capture')
    OUT.mkdir(parents=True,exist_ok=True)
    BASE='https://wef-schedules-decoded.gombcg.workers.dev'
    run_start=time.perf_counter()
    report={'started':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'Running','requests':[],'days':[],'errors':[],'concurrency':4,'details':[],'ring_timings':[],'requested_date':args.date}
    entries=[]
    def source_safe(value):
        if isinstance(value,dict):return {k:source_safe(v) for k,v in value.items() if k.lower() not in {'_','_t','token','cookie','authorization','access_token','refresh_token'}}
        if isinstance(value,list):return [source_safe(v) for v in value]
        return value
    def capture(name,payload):
        if args.capture:
            target=OUT/'source'/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(source_safe(payload),separators=(',',':')))
    def save():
        (OUT/'results.json').write_text(json.dumps(report,indent=2))
        (OUT/'entries.json').write_text(json.dumps(entries,separators=(',',':')))
    def request(route,params):
        url=BASE+route+'?'+urllib.parse.urlencode(params);start=time.perf_counter();requested_at=datetime.datetime.now(datetime.timezone.utc).isoformat()
        p=subprocess.run(['curl','-sS','--max-time','30','-w','\n%{http_code}',url],capture_output=True)
        body,_,status=p.stdout.rpartition(b'\n')
        metric={'route':route,'params':params,'http':status.decode(),'seconds':round(time.perf_counter()-start,4),'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'requested_at':requested_at,'completed_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
        if p.returncode or status!=b'200':return None,metric,'HTTP/transport failure'
        try:return json.loads(body),metric,None
        except Exception:return None,metric,'Invalid JSON'
    def acquire(route,params):
        p,m,error=request(route,params);report['requests'].append(m)
        if error:raise ValueError(error+' '+str(m))
        return p
    def detail(job):
        day,ring,c=job;cid=int(c['class_id'])
        p,m,error=request('/classes/'+str(cid),{'show_id':sid,'customer_id':15})
        result={'day':day,'ring':ring['ring_number'],'ring_name':ring['ring_name'],'class_id':cid,'class_number':c['class_number'],'class_name':c['class_name'],'schedule_group_id':c.get('class_group_id'),'ok':False}
        rows=[]
        try:
            if error:raise ValueError(error)
            if int(p.get('show_id',0))!=sid or int(p.get('class',{}).get('class_id',0))!=cid:raise ValueError('Wrong show/class identity')
            trips=p.get('trips')
            if not isinstance(trips,list):raise ValueError('Missing explicit trips array')
            total=p.get('total_entry_trips')
            if total is None or int(total)!=len(trips):raise ValueError('Trip count mismatch: declared '+str(total)+' actual '+str(len(trips)))
            related=p.get('class_related_data',{});actual_day=str(related.get('date',''))[:10]
            result.update(source_day=actual_day,source_ring=related.get('ring'),source_group_id=related.get('class_group_id'),rows=len(trips),status=related.get('status'),declared_total=int(total))
            if actual_day!=day:raise ValueError('Class detail date differs from scheduled occurrence')
            if str(related.get('ring'))!=str(ring['ring_number']):raise ValueError('Class detail ring differs from schedule')
            seen=set()
            for t in trips:
                eid=t.get('entry_id');horse=t.get('horse_id');rider=t.get('rider_id')
                if not eid or not horse or not rider:raise ValueError('Missing entry/horse/rider identity')
                key=f'{sid}|{day}|{cid}|{eid}|{t.get("entryxclasses_uuid") or t.get("trip_id") or ""}'
                if key in seen:raise ValueError('Duplicate trip identity')
                seen.add(key)
                rows.append({'key':key,'show_id':sid,'day':day,'ring':ring['ring_number'],'ring_name':ring['ring_name'],'class_id':cid,'classNumber':c['class_number'],'className':c['class_name'],'classStatus':related.get('status'),'entry_id':eid,'number':t.get('number'),'horse_id':horse,'horse':t.get('horse'),'rider_id':rider,'rider':t.get('rider_name') or t.get('rider'),'trainer':str(t.get('trainer_id') or ''),'trainer_name':t.get('trainer'),'order_of_go':t.get('order_of_go'),'actual_order':t.get('actual_order'),'gone_in':t.get('gone_in'),'scratch':t.get('scratch_trip'),'placing':t.get('placing'),'estimated_go_time':t.get('estimated_go_time')})
            result['ok']=True
            capture(f'classes/{day}-{ring["ring_number"]}-{cid}.json',p)
        except Exception as e:result['error']=str(e);rows=[]
        return result,m,rows
    try:
        discovery=acquire('/schedule',{'date':'00/00/00','customer_id':15,'show_id':context['show-id']})
        show=validate_source_show(discovery,context);sid=int(show['show_id']);report['show']=show
        if discovery.get('show_date')!=context['selected-date'][:10]:raise ValueError('Source-selected date disagrees with canonical context')
        report['authority']=context
        capture('discovery.json',discovery)
        start=datetime.date.fromisoformat(show['start_date'][:10]);end=datetime.date.fromisoformat(show['end_date'][:10])
        if requested_day:
            if not start<=requested_day<=end:raise ValueError('Requested date outside source show range')
            start=end=requested_day
        schedules=[]
        for i in range((end-start).days+1):
            day=(start+datetime.timedelta(days=i)).isoformat();p=acquire('/schedule',{'date':day,'show_id':sid,'customer_id':15})
            if int(p['show']['show_id'])!=sid or p.get('show_date')!=day or not isinstance(p.get('rings'),list):raise ValueError('Invalid dated schedule')
            for r in p['rings']:
                for c in r.get('classes',[]):
                    if not c.get('class_id') and not c.get('schedule_break'):raise ValueError('Non-break class missing source ID')
            schedules.append((day,p));report['days'].append({'day':day,'rings':len(p['rings']),'class_rows':sum(len(r.get('classes',[])) for r in p['rings'])})
            capture('schedule-'+day+'.json',p)
            print('Schedule complete',report['days'][-1],flush=True);save()
        report['schedule_complete_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
        # Finish every schedule before detail requests. Days and rings are sequential;
        # at most four class requests run together inside the current ring.
        cache={};report['cache_hits']=0
        for day,p in schedules:
            for ring in p['rings']:
                ring_start=time.perf_counter();jobs=[];known=set()
                for c in ring.get('classes',[]):
                    if c.get('schedule_break'):continue
                    k=(day,ring['ring_number'],c['class_id'])
                    if k in known:report['cache_hits']+=1;continue
                    known.add(k);jobs.append((day,ring,c))
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    for result,m,rows in pool.map(detail,jobs):
                        report['requests'].append(m);report['details'].append(result);entries.extend(rows)
                        if not result['ok']:report['errors'].append({k:result[k] for k in ['day','ring','class_id','error']})
                        save()
                timing={'day':day,'ring':ring['ring_number'],'classes':len(jobs),'entries':sum(d.get('rows',0) for d in report['details'] if d['day']==day and d['ring']==ring['ring_number']),'seconds':round(time.perf_counter()-ring_start,4)}
                report['ring_timings'].append(timing);save();print('Ring complete',timing,'errors',len(report['errors']),flush=True)
        if authority_client:
            authority_after=authority_client.canonical_context()
            if canonical_fence(authority_before)!=canonical_fence(authority_after):raise ValueError('Canonical scope changed during acquisition; candidate not publishable')
            report['authority-confirmed-at']=next(r['last-checked'] for r in authority_after['records'] if r['default'])
        report['status']='PASS' if not report['errors'] else 'FAIL'
    except Exception as e:report['status']='FAIL';report['errors'].append(str(e));print('Failure',str(e),flush=True)
    finally:
        report['completed']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['entries']=len(entries)
        report['elapsed_seconds']=round(time.perf_counter()-run_start,4)
        report['unique_entries']=len(set(e['entry_id'] for e in entries));report['tenant_8778_trips']=sum(e['trainer']=='8778' for e in entries)
        report['request_seconds_sum']=round(sum(r['seconds'] for r in report['requests']),3);report['bytes']=sum(r['bytes'] for r in report['requests'])
        if report['requests']:report['request_seconds_median']=statistics.median(r['seconds'] for r in report['requests'])
        save();print('FINISHED',report['status'],'requests',len(report['requests']),'entries',len(entries),'errors',len(report['errors']),flush=True)
    raise SystemExit(0 if report['status']=='PASS' else 1)

if __name__=='__main__':
    main()
