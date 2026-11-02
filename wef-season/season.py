"""Hourly Season add/upsert/dedup from existing Data records. No collection."""
import copy,json,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parent
PARENT=ROOT.parent
sys.path[:0]=[str(PARENT/'wef-data'),str(PARENT/'wef-schedules/tests')]
from data_runner import atomic,stamp
from data_contract import packed
from wef_data_airtable import Airtable,TransportDeferred
from season_store import SeasonStore
from ring_helpers import sync as sync_ring_helpers
from horse_associations import sync as sync_horse_associations

ENTITIES=('horses','riders','trainers','rings')
# Only business facts needed for existing entity fields and show/day links.
SOURCE_FIELDS={
    'shows':('show-key','show-id','customer-id','facility-id','show-name','start-date','end-date','selected-date','last-success'),
    'days':('day-key','show','date','last-success'),
    'rings':('ring-key','show','day','ring-id','ring-number','ring-name','last-success'),
    'classes':('class-key','show','day'),
    'entries':('entry-key','show','horse','riders','trainers','last-success'),
    'trips':('trip-key','class','entry','horse','rider','trainer','last-success'),
    'horses':('horse-key','horse-id','horse-name','last-success'),
    'riders':('rider-key','rider-id','rider-name','last-success'),
    'trainers':('trainer-key','trainer-id','trainer-name','last-success'),
}

def sources(client):
    schema=json.loads((PARENT/'wef-data/schema.json').read_text(encoding='utf-8'))
    facts={};native={};definitions={}
    for table,names in SOURCE_FIELDS.items():
        definition=schema['tables'][table]
        fields=[f for f in definition['fields'] if f['name'] in names]
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

def process(entity,config,client,facts):
    runtime=ROOT/'runtime'/entity
    counters=SeasonStore(client,config,runtime).apply(project(facts,entity))
    if entity=='horses':counters.update(sync_horse_associations(client,config,facts,runtime))
    if config.get('ring-helpers'):
        source=[]
        for show in facts['shows']:
            rings={k:r for k,r in facts['rings'].items() if linked(r,'show','shows')==[show]}
            if rings:source.append(({'shows':facts['shows'],'rings':rings},{'show-key':show}))
        helpers=sync_ring_helpers(client,config['ring-helpers'],source,runtime)
        counters.update({'ring-helper-'+name:value for name,value in helpers.items()})
    return {'status':'Succeeded','stage':'Upsert','counts':counters}

def run():
    config=json.loads((ROOT/'schema.json').read_text(encoding='utf-8'))
    client=Airtable();results={}
    try:facts=sources(client)
    except TransportDeferred as error:
        return {entity:{'status':'Deferred','stage':'SourceRead','error':{'category':'AirtableRead','message':str(error),'write-outcome':'NotApplicable'}} for entity in config['entities']}
    for entity,schema in config['entities'].items():
        runtime=ROOT/'runtime'/entity
        if (runtime/'stopped.json').exists():
            results[entity]={'status':'Skipped','stage':'StopGuard','reason':'Season entity stopped; owner approval required'}
            continue
        try:results[entity]=process(entity,schema,client,facts)
        except TransportDeferred as error:
            results[entity]={'status':'Deferred','stage':'Persistence','error':{'category':'Transport','operation':error.operation['method'],'target':error.operation['table'],'message':str(error),'write-outcome':error.operation.get('write-outcome','Unknown')}}
        except Exception as error:
            result={'status':'Failed','stage':'Apply','error':{'category':'Validation','message':type(error).__name__+': '+str(error),'write-outcome':'Unknown'}}
            atomic(runtime/'stopped.json',{'at':stamp(),**result});results[entity]=result
    return results

if __name__=='__main__':
    try:print(packed(run()))
    except Exception as error:print(packed({'error':type(error).__name__+': '+str(error)}));raise SystemExit(1)

