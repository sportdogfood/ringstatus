"""Hourly Season add/upsert/dedup from existing Data records. No collection."""
import copy,json,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parent
PARENT=ROOT.parent
sys.path[:0]=[str(PARENT/'wef-data'),str(PARENT/'wef-schedules/tests')]
from data_runner import atomic,stamp
from data_contract import packed
from wef_data_airtable import Airtable,TransportDeferred
from season_store import SeasonStore,epoch
from ring_helpers import sync as sync_ring_helpers
from horse_associations import sync as sync_horse_associations

ENTITIES=('horses','riders','trainers','rings')
# Only business facts needed for existing entity fields and show/day links.
SOURCE_FIELDS={
    'shows':('show-key','show-id','customer-id','facility-id','show-name','start-date','end-date','selected-date','last-success','is-default'),
    'days':('day-key','show','date','last-success'),
    'rings':('ring-key','show','day','ring-id','ring-number','ring-name','last-success'),
    'classes':('class-key','show','day'),
    'entries':('entry-key','show','horse','riders','trainers','last-success'),
    'trips':('trip-key','class','entry','horse','rider','trainer','last-success'),
    'horses':('horse-key','horse-id','horse-name','last-success'),
    'riders':('rider-key','rider-id','rider-name','last-success'),
    'trainers':('trainer-key','trainer-id','trainer-name','last-success'),
}

def sources(client,canonical=False):
    schema=json.loads((PARENT/'wef-data/schema.json').read_text(encoding='utf-8'))
    facts={};native={};definitions={}
    for table,names in SOURCE_FIELDS.items():
        source={'shows':'w-shows','days':'w-days'}.get(table,table) if canonical else table
        definition=schema['tables'][source]
        fields=copy.deepcopy(definition['fields'])
        if canonical:
            for field in fields:
                field['name']={'show-day-key':'day-key'}.get(field['name'],field['name'].replace('_','-'))
            overrides={
                **{name:{logical:next(f['id'] for f in schema['tables'][name]['fields'] if f['name']==actual) for logical,actual in [('show','w-shows'),('day','w-days')]} for name in ['classes','rings','entries']},
            }.get(table,{})
            for name,identity in overrides.items():
                fields=[f for f in fields if f['name']!=name]
                fields.append({'name':name,'id':identity,'type':'multipleRecordLinks'})
        fields=[f for f in fields if f['name'] in names]
        definitions[table]=fields
        rows=client.records(schema['base'],definition['id'],fields=[f['id'] for f in fields])
        facts[table]={}
        primary=next(f for f in fields if f['id']==definition['primaryFieldId'])
        for record in rows:
            values=record['fields'];key=str(values.get(primary['id'],''))
            if not key or key in facts[table]:raise ValueError('Data missing/duplicate existing '+primary['name'])
            facts[table][key]={f['name']:copy.deepcopy(values[f['id']]) for f in fields if f['id'] in values}
            native[record['id']]=(table,key)
    for table,rows in facts.items():
        links=[f for f in definitions[table] if f['type']=='multipleRecordLinks']
        for row in rows.values():
            for field in links:
                # A missing relationship withholds that membership, never the entity.
                row[field['name']]={'$links':[list(native[v['id'] if isinstance(v,dict) else v])
                    for v in row.get(field['name'],[]) if (v['id'] if isinstance(v,dict) else v) in native]}
    if canonical:
        for row in facts['days'].values():
            if isinstance(row.get('show'),str):
                row['show']={'$links':[['shows',row['show']]] if row['show'] in facts['shows'] else []}
    return facts

def linked(row,field,target):
    return [key for table,key in row.get(field,{}).get('$links',[]) if table==target]

def project(facts,entity):
    stem=entity[:-1]
    graph={'shows':copy.deepcopy(facts['shows']),'show-days':{},entity:{}}
    for key,row in facts['days'].items():
        shows=linked(row,'show','shows')
        if len(shows)==1:
            graph['show-days'][key]={'day-key':key,'date':row['date'],'show':shows,
                **({'last-success':row['last-success']} if row.get('last-success') else {})}
    fields=(stem+'-key',stem+'-id',stem+'-name','last-success')+(('ring-number',) if entity=='rings' else ())
    for key,row in facts[entity].items():
        graph[entity][key]={f:copy.deepcopy(row[f]) for f in fields if f in row and row[f] is not None}
        graph[entity][key].update({'shows':[],'show-days':[]})
    def add(key,shows,days=()):
        if key not in graph[entity]:return
        row=graph[entity][key]
        row['shows']=sorted(set(row['shows'])|{s for s in shows if s in graph['shows']})
        row['show-days']=sorted(set(row['show-days'])|{d for d in days if d in graph['show-days']
            and set(graph['show-days'][d]['show'])<=set(shows)})
    if entity=='rings':
        for key,row in facts['rings'].items():
            add(key,linked(row,'show','shows'),linked(row,'day','days'))
            # Missing context withholds only that link; retain the ring facts.
            # Omitted fields preserve any previously supported native links.
            for field in ('shows','show-days'):
                if not graph[entity][key][field]:
                    del graph[entity][key][field]
    else:
        field={'horses':'horse','riders':'riders','trainers':'trainers'}[entity]
        for entry in facts['entries'].values():
            for key in linked(entry,field,entity):add(key,linked(entry,'show','shows'))
        for trip in facts['trips'].values():
            # Read the existing single trip role. Never manufacture an entry assignment.
            keys=linked(trip,stem,entity)
            if len(keys)!=1:continue
            for ck in linked(trip,'class','classes'):
                cls=facts['classes'][ck]
                add(keys[0],linked(cls,'show','shows'),linked(cls,'day','days'))
    return graph

def pending_bytes(root):
    try:return (root/'pending-create.json').read_bytes()
    except FileNotFoundError:return None

def release_rejected_create(root,error,before=None):
    # Only a definite HTTP rejection can release its create reservation.
    # TransportDeferred/unknown outcomes retain the original reconciliation state.
    if str(error) in ('Airtable HTTP 400','Airtable HTTP 422'):
        pending=root/'pending-create.json'
        if pending.exists() and pending.read_bytes()!=before:pending.unlink()

def apply_records(store,graph,errors):
    before=pending_bytes(store.root)
    try:return store.apply(graph)
    except (ValueError,RuntimeError) as error:
        if not isinstance(error,ValueError) and str(error) not in ('Airtable HTTP 400','Airtable HTTP 422'):raise
        release_rejected_create(store.root,error,before)
        # The normal healthy batch stays unchanged. Isolate rejected rows only.
        counts={'records-added':0,'records-updated':0,'records-unchanged':0}
        for table,rows in graph.items():
            for key,row in rows.items():
                item={name:{} for name in graph};item[table]={key:row}
                before=pending_bytes(store.root)
                try:
                    result=store.apply(item)
                    for name,value in result.items():counts[name]+=value
                except TransportDeferred:raise
                except Exception as error:
                    release_rejected_create(store.root,error,before)
                    errors.append({'category':'Record','target':table+' '+key,'message':str(error),'write-outcome':'Unknown'})
        return counts

def process(entity,config,client,facts):
    runtime=ROOT/'runtime'/entity
    store=SeasonStore(client,config,runtime)
    graph=project(facts,entity);unmatched=0
    if config.get('synced-context'):
        # Context is owned by Shows. Only entity links are authored here.
        for table,field in [('shows','shows'),('show-days','show-days')]:
            available=store.inventory(table)
            for row in graph[entity].values():
                supported=[key for key in row[field] if key in available]
                unmatched+=len(row[field])-len(supported);row[field]=supported
            graph[table]={}
    errors=[]
    # Withhold a conflicting native identity before the existing batch writer.
    for table,rows in graph.items():
        existing=store.inventory(table)
        for key,row in list(rows.items()):
            old=existing.get(key,{}).get('fields',{})
            conflicts=[name for name,value in row.items() if name.endswith('-id') and old.get(store.fields[table][name]['id']) not in (None,value)]
            try:
                for value in [row.get('last-success'),old.get(store.fields[table].get('last-success',{}).get('id'))]:
                    if value:epoch(value)
            except (ValueError,TypeError,AttributeError):conflicts.append('last-success')
            if conflicts:
                errors.append({'category':'Record','target':table+' '+key,'message':'Source ID conflict: '+','.join(conflicts),'write-outcome':'NotSent'})
                del rows[key]
    counters=apply_records(store,graph,errors)
    counters['records-skipped']=len(errors)
    if config.get('synced-context'):counters['memberships-unmatched']=unmatched
    if entity=='horses':
        supported=copy.deepcopy(facts)
        supported['horses']={k:v for k,v in facts['horses'].items() if k in graph[entity] and not any(e['target']==entity+' '+k for e in errors)}
        counters.update(sync_horse_associations(client,config,supported,runtime,excluded=set(facts['horses'])-set(supported['horses'])))
    if config.get('ring-helpers'):
        source=[]
        for show in facts['shows']:
            rings={k:r for k,r in facts['rings'].items() if k in graph[entity] and not any(e['target']==entity+' '+k for e in errors) and linked(r,'show','shows')==[show]}
            if rings:source.append(({'shows':facts['shows'],'rings':rings},{'show-key':show}))
        before=pending_bytes(runtime/'ring-helpers')
        try:
            helpers=sync_ring_helpers(client,config['ring-helpers'],source,runtime)
            counters.update({'ring-helper-'+name:value for name,value in helpers.items()})
        except TransportDeferred:raise
        except (ValueError,RuntimeError) as error:
            if not isinstance(error,ValueError) and str(error) not in ('Airtable HTTP 400','Airtable HTTP 422'):raise
            release_rejected_create(runtime/'ring-helpers',error,before)
            # A malformed helper source must not suppress unrelated helpers.
            for batch,reference in source:
                for key,ring in batch['rings'].items():
                    before=pending_bytes(runtime/'ring-helpers')
                    try:
                        result=sync_ring_helpers(client,config['ring-helpers'],[({'shows':batch['shows'],'rings':{key:ring}},reference)],runtime)
                        for name,value in result.items():counters['ring-helper-'+name]=counters.get('ring-helper-'+name,0)+value
                    except TransportDeferred:raise
                    except Exception as error:
                        release_rejected_create(runtime/'ring-helpers',error,before)
                        errors.append({'category':'Record','target':'ring-helper '+key,'message':str(error),'write-outcome':'Unknown'})
        counters['records-skipped']=len(errors)
    result={'status':'Succeeded','stage':'Upsert','counts':counters,'errors':errors}
    selected=[key for key,row in facts['shows'].items() if row.get('is-default') is True]
    if len(selected)==1:result['show-key']=selected[0]
    return result

def run():
    config=json.loads((ROOT/'schema.json').read_text(encoding='utf-8'))
    client=Airtable();results={}
    canonical=any(v.get('synced-context') for v in config['entities'].values())
    source_error=None
    try:facts=sources(client,canonical=canonical)
    except Exception as error:
        if not canonical:
            if not isinstance(error,TransportDeferred):raise
            return {entity:{'status':'Deferred','stage':'SourceRead','error':{'category':'AirtableRead','message':str(error),'write-outcome':'NotApplicable'}} for entity in config['entities']}
        source_error=error
    for entity,schema in config['entities'].items():
        runtime=ROOT/'runtime'/entity
        try:
            if schema.get('synced-context') and source_error is not None:raise source_error
            # Preserve the separately owned Rings context path during this migration.
            entity_facts=sources(client) if canonical and not schema.get('synced-context') else facts
            results[entity]=process(entity,schema,client,entity_facts)
        except TransportDeferred as error:
            results[entity]={'status':'Deferred','stage':'SourceRead' if error is source_error else 'Persistence','error':{'category':'Transport','operation':error.operation['method'],'target':error.operation['table'],'message':str(error),'write-outcome':error.operation.get('write-outcome','Unknown')}}
        except Exception as error:
            result={'status':'Deferred','stage':'SourceRead' if error is source_error else 'Apply','error':{'category':'Validation','message':type(error).__name__+': '+str(error),'write-outcome':'Unknown'}}
            results[entity]=result
    return results

if __name__=='__main__':
    try:print(packed(run()))
    except Exception as error:print(packed({'error':type(error).__name__+': '+str(error)}));raise SystemExit(1)

