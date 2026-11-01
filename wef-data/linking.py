"""Independently scheduled native linking from saved Data records only."""
import datetime,json,os,pathlib,sys,time
ROOT=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'wef-schedules/tests'))
from wef_data_airtable import Airtable
from wef_data_store import DATA_BASE,DataStore
from record_mappings import trip_records

def epoch(value):
    return datetime.datetime.fromisoformat(str(value).replace('Z','+00:00')).timestamp()

def run(schema_file):
    started=time.perf_counter();errors=[];missing=set();changes={};linked={}
    schema=json.loads(pathlib.Path(schema_file).read_text(encoding='utf-8'))['tables'];api=Airtable()
    def read(table,names=None,formula=None):
        fields={f['name']:f['id'] for f in schema[table]['fields']}
        if names is not None:fields={n:i for n,i in fields.items() if n in names}
        return [{'id':r['id'],'fields':{n:r['fields'].get(i) for n,i in fields.items()}}
            for r in api.records(DATA_BASE,schema[table]['id'],fields=list(fields.values()),formula=formula)]
    def primary(table):
        return next(f['name'] for f in schema[table]['fields'] if f['id']==schema[table]['primaryFieldId'])
    def keyed(table,rows):
        output={}
        for row in rows:
            key=str(row['fields'].get('run-key' if table=='run-logs' else primary(table)))
            output.setdefault(key,[]).append(row)
        return output
    def key_filter(table,keys):
        return 'OR('+','.join('{'+primary(table)+'}='+json.dumps(str(k)) for k in keys)+')'
    def read_keys(table,keys):
        rows=[];keys=sorted(set(keys))
        for start in range(0,len(keys),20):rows.extend(read(table,formula=key_filter(table,keys[start:start+20])))
        return rows
    def fail(target,error):errors.append({'key':target,'error':type(error).__name__+': '+str(error)})
    try:
        shows=read('wef-shows.shows');owner=next(r for r in shows if r['fields'].get('is_default') is True)
        show=owner['fields']['show-key'];sid=str(owner['fields']['show_id'])
        source_days=read('wef-shows.show-days')
        days={r['fields']['date'][:10]:r for r in source_days if str(r['fields'].get('show-day-key','')).startswith(show+'|')
              and type(r['fields'].get('day-priority')) in (int,float) and r['fields']['day-priority'] in (0,1,2,3)}
        data={'wef-shows.shows':keyed('wef-shows.shows',shows),
              'wef-shows.show-days':keyed('wef-shows.show-days',source_days)}
        for table in ('shows','days'):
            data[table]=keyed(table,read(table,formula='LEFT({'+primary(table)+'},'+str(len(show))+')='+json.dumps(show)))
        for table in ('rings','groups','classes','schedules','trips','results'):
            prefixes=[show+'|'+day+'|' for day in days]
            formula='OR('+','.join('LEFT({'+primary(table)+'},'+str(len(p))+')='+json.dumps(p) for p in prefixes)+')'
            names={primary(table),'last-success'}|{f['name'] for f in schema[table]['fields'] if f['type']=='multipleRecordLinks'}
            if table=='rings':names.add('ring-id')
            if table=='classes':names.add('status')
            if table in ('trips','schedules'):names|={'source-json','record-status'}
            data[table]=keyed(table,read(table,names,formula)) if prefixes else {}
        facts=[];entity_keys={t:set() for t in ('entries','horses','riders','trainers','owners')}
        for key,rows in data['trips'].items():
            if len(rows)!=1:continue
            row=rows[0];f=row['fields']
            if f.get('record-status')=='dropped' or not f.get('source-json'):continue
            try:
                class_key=key.rsplit('|',2)[0];raw=json.loads(f['source-json'])
                mapped=trip_records(raw,class_key,None,f.get('last-success'))
                if mapped['trips'][0]['trip-key']!=key:raise ValueError('Saved trip identity does not match existing key')
                facts.append(mapped)
                for table in entity_keys:
                    for value in mapped[table]:entity_keys[table].add(str(value[primary(table)]))
            except Exception as error:fail('trips '+key,error)
        store=DataStore(api,{'base':DATA_BASE,'tables':schema},cache=True)
        for table,keys in entity_keys.items():
            data[table]=keyed(table,read_keys(table,keys))
            absent=sorted(keys-set(data[table]))
            for start in range(0,len(absent),10):
                try:
                    for key,identity in store.reserve(table,absent[start:start+10]).items():
                        data[table][key]=[{'id':identity,'fields':{primary(table):key}}]
                except Exception as error:fail(table+' link target keys',error)
        for table,target in [('horses','w-horses'),('riders','w-riders'),('trainers','w-trainers')]:
            data[target]=keyed(target,read_keys(target,entity_keys[table]))
        master_rings=read('w-rings');data['w-rings']=keyed('w-rings',master_rings)
        day_run_keys={}
        try:
            cadence=json.loads((pathlib.Path(schema_file).parent/'runtime/day-cadence.json').read_text(encoding='utf-8'))
            day_run_keys={day:cadence[show+'|'+day]['run-key'] for day in days if cadence.get(show+'|'+day,{}).get('run-key')}
        except Exception as error:fail('Saved collection run metadata',error)
        runs=read_keys('runs',day_run_keys.values())
        data['runs']=keyed('runs',runs)
        def owning_run(observed,day):
            rows=data['runs'].get(day_run_keys.get(day),[])
            if not observed or len(rows)!=1:return None
            row=rows[0];f=row['fields'];at=epoch(observed)
            return row if f.get('started') and f.get('completed') and epoch(f['started'])<=at<=epoch(f['completed']) else None
        schedule_runs={}
        for key,rows in data['schedules'].items():
            if len(rows)==1:
                owned=owning_run(rows[0]['fields'].get('last-success'),key.split('|')[2])
                if owned:schedule_runs[key]=owned
        log_keys={r['fields']['run-key'].split('|')[0] for r in schedule_runs.values()}
        logs=[]
        for start in range(0,len(log_keys),20):
            keys=sorted(log_keys)[start:start+20]
            logs.extend(read('run-logs',formula='OR('+','.join('{run-key}='+json.dumps(k) for k in keys)+')'))
        data['run-logs']=keyed('run-logs',logs)

        def edge(table,key,field,targets,replace=False,optional=False):
            definition=next((f for f in schema[table]['fields'] if f['name']==field and f['type']=='multipleRecordLinks'),None)
            rows=data.get(table,{}).get(str(key),[])
            if not definition or not targets or len(rows)!=1:return
            ids=[]
            for target,identity in targets:
                values=data.get(target,{}).get(str(identity),[])
                if len(values)!=1:
                    if not optional:missing.add(target+' '+str(identity))
                    return
                if values[0]['id'] not in ids:ids.append(values[0]['id'])
            row=rows[0];pending=changes.setdefault(table,{}).setdefault(row['id'],{})
            current=pending.get(definition['id'],row['fields'].get(field) or [])
            desired=ids if replace else list(dict.fromkeys([*current,*ids]))
            if set(current)!=set(desired):pending[definition['id']]=desired
        def parents(table,key,day,ring=None):
            edge(table,key,'show',[('shows',show)],optional=True)
            edge(table,key,'day',[('days',show+'|'+day)],optional=True)
            edge(table,key,'wef-shows.shows',[('wef-shows.shows',show)])
            edge(table,key,'wef-shows.show-days',[('wef-shows.show-days',show+'|'+day)])
            if ring:edge(table,key,'ring',[('rings',ring)])
        for table in ('rings','classes','groups'):
            for key,rows in data[table].items():
                if len(rows)!=1:continue
                day=key.split('|')[2];ring=key.rsplit('|',1)[0] if table!='rings' else None
                parents(table,key,day,ring)
                if table=='rings':
                    f=rows[0]['fields'];prefix=str(owner['fields']['customer_id'])+'|'+str(owner['fields']['facility_id'])+'|'
                    matches=[r for r in master_rings if str(r['fields'].get('ring-key','')).startswith(prefix) and r['fields'].get('ring-id')==f.get('ring-id')]
                    if len(matches)==1:edge('rings',key,'w-rings',[('w-rings',matches[0]['fields'][primary('w-rings')])])
                    else:missing.add('w-rings '+prefix+str(f.get('ring-id')))
        for key,rows in data['schedules'].items():
            if len(rows)!=1:continue
            try:
                f=rows[0]['fields'];raw=json.loads(f['source-json']);ring=key.rsplit('|',1)[0];day=key.split('|')[2]
                parents('schedules',key,day,ring)
                gid=raw.get('class_group_id');cid=raw.get('class_id')
                if gid is not None:edge('schedules',key,'group',[('groups',ring+'|'+str(gid))])
                if cid is not None and not raw.get('schedule_break'):
                    ck=ring+'|'+str(cid);edge('schedules',key,'class',[('classes',ck)])
                    if gid is not None:edge('classes',ck,'group',[('groups',ring+'|'+str(gid))])
                owned=schedule_runs.get(key)
                if owned:
                    rk=owned['fields']['run-key'];edge('schedules',key,'runs',[('runs',rk)],replace=True)
                    edge('schedules',key,'run-logs',[('run-logs',rk.split('|')[0])],replace=True)
                    edge('runs',rk,'shows',[('shows',show)],optional=True)
                    edge('runs',rk,'days',[('days',show+'|'+day)],optional=True)
            except Exception as error:fail('schedules '+key,error)
        for mapped in facts:
            for table in ('entries','trips','results'):
                for row in mapped[table]:
                    key=row[primary(table)]
                    for field,value in row.items():
                        if isinstance(value,dict) and '$links' in value:edge(table,key,field,value['$links'],optional=field=='show')
        for table,target in [('horses','w-horses'),('riders','w-riders'),('trainers','w-trainers')]:
            for key in entity_keys[table]:edge(table,key,target,[(target,key)])
        for table in ('rings','classes','trips','results'):
            for key,rows in data[table].items():
                if len(rows)!=1:continue
                owned=owning_run(rows[0]['fields'].get('last-success'),key.split('|')[2])
                if owned:edge(table,key,'last-run',[('runs',owned['fields']['run-key'])],replace=True)
        for table,rows in changes.items():
            records=[{'id':identity,'fields':fields} for identity,fields in rows.items() if fields]
            for start in range(0,len(records),10):
                batch=records[start:start+10]
                try:
                    result=api.request('PATCH',DATA_BASE,schema[table]['id'],body={'records':batch})
                    acknowledged={r['id'] for r in result['records']}
                    for row in batch:
                        if row['id'] not in acknowledged:raise ValueError('Link write was not acknowledged: '+row['id'])
                    linked[table]=linked.get(table,0)+len(batch)
                except Exception as error:fail(table,error)
    except Exception as error:fail('Linking',error)
    for target in sorted(missing):errors.append({'key':target,'error':'No unique existing link target; existing links preserved'})
    return {'ok':not errors,'state':'Partial' if errors else 'Linked','show-key':locals().get('show'),
        'run-key':'linking-'+(os.environ.get('WEF_LOG_ATTEMPT_ID') or datetime.datetime.now(datetime.timezone.utc).isoformat()),
        'counts':{'saved':sum(linked.values()),'linked-by-table':linked,'unresolved':len(missing),'total-seconds':round(time.perf_counter()-started,6)},'item-errors':errors}
