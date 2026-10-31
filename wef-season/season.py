"""Scheduled Season consumer. No acquisition and no Data-owned writes."""
import copy,datetime,json,pathlib,sys,time,traceback
ROOT=pathlib.Path(__file__).resolve().parent;PARENT=ROOT.parent
sys.path[:0]=[str(PARENT/'wef-data'),str(PARENT/'wef-schedules/tests')]
from data_runner import atomic,stamp
from data_contract import packed,digest
from wef_data_airtable import Airtable,canonical_fence,TransportDeferred
from wef_data_handoff import assignment_evidence,TABLE_KEYS
from retained_feed import load
from day_counts import retained_units
from archive_evidence import EvidenceStore
from season_projection import project
from season_store import SeasonStore
from ring_helpers import sync as sync_ring_helpers

ENTITIES=('horses','riders','trainers','rings')
FACT_TABLES={'shows','days','rings','classes','entries','horses','riders','trainers','trips'}

class SourcePending(RuntimeError):
    """Required completed local evidence is not available; never fabricate it."""

def merge_projection(target,graph):
    # Keep intermediate memberships even if a later source observation drops one.
    for table,rows in graph.items():
        for key,row in rows.items():
            old=target.setdefault(table,{}).setdefault(key,{})
            previous=old.get('last-success');incoming=row.get('last-success')
            older=bool(previous and (not incoming or datetime.datetime.fromisoformat(incoming.replace('Z','+00:00'))<datetime.datetime.fromisoformat(previous.replace('Z','+00:00'))))
            for field,value in row.items():
                if field in ('shows','show-days'):old[field]=sorted(set(old.get(field,[]))|set(value))
                elif not older or field not in old:old[field]=copy.deepcopy(value)

def complete_dependencies(facts):
    needed={'days':('show',),'rings':('show','day'),'classes':('show','day','ring'),
            'entries':('show','horse','riders','trainers'),'trips':('class','entry','horse','rider','trainer')}
    for table,fields in needed.items():
        for row in facts.get(table,{}).values():
            for field in fields:
                for target,key in row.get(field,{}).get('$links',[]):
                    if key not in facts.get(target,{}):raise SourcePending('Waiting for verified local dependency: '+target+' '+key)

def sources(context):
    runtime=PARENT/'wef-data/runtime';show=context['show-key']
    authority=[context[k] for k in ('show-key','start-date','end-date')]
    cache_path=ROOT/'runtime'/('day-reader-'+show.replace('|','-')+'.json')
    if cache_path.exists():
        envelope=json.loads(cache_path.read_text(encoding='utf-8'));state=envelope['body']
        if digest(packed(state))!=envelope['sha256'] or state['authority']!=authority:raise ValueError('Season day reader state changed')
    else:
        state={'authority':authority,'sequence':0,'seen':{},'facts':{},'graphs':{},'reference':None}
        seed=runtime/'handoff'/('head-'+show.replace('|','-')+'.json')
        if seed.exists():
            pointer=json.loads(seed.read_text(encoding='utf-8'))
            if pointer.get('authority',[])[:3]!=authority:raise SourcePending('Existing local seed has a different show/date scope')
            facts,proof=load(runtime/'handoff',pointer)
            state['facts']={t:rows for t,rows in facts.items() if t in FACT_TABLES}
            state['sequence']=proof['sequence']
    pending=[]
    for path in sorted((runtime/'completed-days').glob('*.json')):
        if path.name in state['seen']:continue
        body=json.loads(path.read_text(encoding='utf-8'));saved=body.get('state',{})
        if saved.get('show-key')!=show:continue
        sha=digest(packed(body))
        if path.stem!=sha or body.get('format')!='wef-data-day-save-v1':raise ValueError('Completed day receipt hash/format mismatch')
        if type(saved.get('sequence')) is not int or saved['sequence']<=0:raise ValueError('Invalid completed day sequence')
        if saved['sequence']<=state['sequence']:
            if str(saved['sequence']) in state['seen'].values():raise ValueError('Conflicting completed day sequence')
            # Seed history was already accepted; never replay it as new input.
            if state.get('reference') and saved['sequence']>state.get('seed-sequence',0):raise ValueError('Late or conflicting completed day receipt')
            continue
        day=saved.get('publication-day','');fence=saved.get('authority',[])
        if fence[:3]!=authority or saved.get('authority-before')!=fence or saved.get('phase')!='Complete' or saved.get('pending') or day in saved.get('day-work',{}):raise ValueError('Unverified completed day scope/state')
        if not context['start-date'][:10]<=day<=context['end-date'][:10]:raise ValueError('Completed day outside show')
        if saved.get('publication-day') not in saved.get('canonical-days',[]) or not saved.get('day-passes',{}).get(day,{}).get('completed'):raise ValueError('Completed day lacks verified pass')
        pending.append((saved['sequence'],sha,path,body))
    if len({s for s,h,p,b in pending})!=len(pending):raise ValueError('Conflicting completed day sequence')
    if not pending and not state.get('reference'):return None
    state.setdefault('seed-sequence',state['sequence'])
    store=EvidenceStore(runtime/'retained-evidence',runtime/'archive-repository')
    for sequence,sha,path,body in sorted(pending):
        saved=body['state'];rows={ref['key']:store.read(ref) for ref in saved['observations']}
        for unit,refs in retained_units(saved,rows):
            if unit['show-key']!=show:raise ValueError('Cross-show completed day observation')
            proof=unit.get('verification',{}).get('record-map')
            if not isinstance(proof,dict):raise ValueError('Completed day missing native readback proof')
            for table,records in unit['graph'].items():
                if table not in FACT_TABLES:continue
                for row in records:
                    key=str(row[TABLE_KEYS[table]])
                    if not proof.get(table,{}).get(key):raise ValueError('Unmapped completed day fact')
                    state['facts'].setdefault(table,{}).setdefault(key,{}).update(copy.deepcopy(row))
        complete_dependencies(state['facts'])
        assessment=assignment_evidence(state['facts'])
        for entity in ENTITIES:merge_projection(state['graphs'].setdefault(entity,{}),project(state['facts'],entity,assessment))
        state['sequence']=sequence;state['seen'][path.name]=str(sequence)
        state['reference']={'show-key':show,'kind':'saved-day','sequence':sequence,'revision':sha,
                            'run-key':body['run-key'],'source-path':path.relative_to(runtime).as_posix(),
                            'assignment-summary':assessment['summary']}
    if pending:
        cache_path.parent.mkdir(parents=True,exist_ok=True)
        atomic(cache_path,{'body':state,'sha256':digest(packed(state))})
    facts={**state['facts'],'_season-graphs':state['graphs']}
    return [(facts,state['reference'])]

def process(entity,config,client,source,authority,assessments=None):
    runtime=ROOT/'runtime'/entity;runtime.mkdir(parents=True,exist_ok=True)
    if (runtime/'stopped.json').exists():return {'status':'Skipped','stage':'StopGuard','reason':'Season entity stopped; owner approval required'}
    heartbeat=config['heartbeat'];workflow='wef-season-'+entity
    rows=client.records(config['base'],heartbeat['id'],formula='{heartbeat-key}='+json.dumps(workflow))
    if len(rows)!=1:raise ValueError('Season heartbeat missing/ambiguous')
    row=rows[0];field=heartbeat['checkpoint-field'];checkpoints=json.loads(row['fields'].get(field) or '{}')
    assessments={} if assessments is None else assessments
    counters={'records-added':0,'records-updated':0,'records-unchanged':0};changed=False;last_ref=None;assignment_counts={}
    helper_sources=[];helper_stale=False
    for facts,reference in source:
        show=reference['show-key'];last_ref=reference
        old=checkpoints.get(show,{})
        if reference.get('kind') in ('retained','saved-day') and old.get('kind') in ('retained','saved-day'):
            if reference['sequence']<old.get('sequence',0):helper_stale=True;continue
        helper_sources.append((facts,reference))
        if old.get('revision')==reference['revision']:continue
        if reference.get('kind') in ('retained','saved-day') and old.get('kind') in ('retained','saved-day'):
            if reference['sequence']==old.get('sequence',0):raise ValueError('Conflicting Season source revision at same sequence')
        identity=(show,reference['revision'])
        if '_season-graphs' in facts:
            graph=facts['_season-graphs'][entity];summary=reference['assignment-summary']
        else:
            if identity not in assessments:assessments[identity]=assignment_evidence(facts)
            assessment=assessments[identity];summary=assessment['summary'];graph=project(facts,entity,assessment)
        assignment_counts={**summary,'show-key':show,'source-revision':reference['revision']}
        result=SeasonStore(client,config,runtime).apply(graph)
        if canonical_fence(client.canonical_context())!=canonical_fence(authority):raise TransportDeferred('GET','appCaSmL819KK7UFJ','tblJA5CUon5s1sWib')
        for name,count in result.items():counters[name]+=count
        updated={**checkpoints,show:{**reference,'accepted-at':stamp()}}
        client.request('PATCH',config['base'],heartbeat['id'],body={'records':[{'id':row['id'],'fields':{field:packed(updated)}}]})
        actual=client.records(config['base'],heartbeat['id'],fields=[field],formula='{heartbeat-key}='+json.dumps(workflow))
        if len(actual)!=1 or actual[0]['id']!=row['id'] or json.loads(actual[0]['fields'].get(field) or '{}')!=updated:raise ValueError('Season checkpoint readback mismatch')
        checkpoints=updated;changed=True
    if config.get('ring-helpers') and not helper_stale:
        # Independent additive projection also backfills already-accepted revisions.
        helpers=sync_ring_helpers(client,config['ring-helpers'],helper_sources,runtime)
        if canonical_fence(client.canonical_context())!=canonical_fence(authority):raise TransportDeferred('GET','appCaSmL819KK7UFJ','tblJA5CUon5s1sWib')
        counters.update({'ring-helper-'+name:value for name,value in helpers.items()})
        changed=changed or bool(helpers['records-added'] or helpers['records-updated'])
    if config.get('ring-helpers') and helper_stale:
        return {'status':'Deferred','stage':'RingHelpers','reason':'Helper updates withheld: retained input older than accepted checkpoint','counts':counters,'source-revision':last_ref['revision'] if last_ref else None,'show-key':last_ref['show-key'] if last_ref else None}
    if assignment_counts:
        counters['assignment-source']={'show-key':assignment_counts['show-key'],'revision':assignment_counts['source-revision']}
        counters.update({'complete-trip-assignments':assignment_counts['complete-trips'],'incomplete-trip-assignments':assignment_counts['incomplete-trips'],'withheld-entry-assignments':assignment_counts['withheld-entries']})
    return {'status':'Succeeded' if changed else 'Skipped','stage':'Verified' if changed else 'Checkpoint','reason':('Parent assignments withheld; verified trip roles retained' if assignment_counts.get('withheld-entries') else None) if changed else 'All available verified revisions already applied','counts':counters,'assignment-counts':assignment_counts,'source-revision':last_ref['revision'] if last_ref else None,'show-key':last_ref['show-key'] if last_ref else None}

def run():
    config=json.loads((ROOT/'schema.json').read_text(encoding='utf-8'));client=Airtable();results={};assessments={}
    if all((ROOT/'runtime'/e/'stopped.json').exists() for e in config['entities']):return {e:{'status':'Skipped','stage':'StopGuard','reason':'Season entity stopped; owner approval required'} for e in config['entities']}
    try:
        authority=client.canonical_context();context=next(r for r in authority['records'] if r['default']);source=sources(context)
    except SourcePending as error:return {e:{'status':'Deferred','stage':'SourceRead','reason':str(error)} for e in config['entities']}
    except FileNotFoundError:return {e:{'status':'Deferred','stage':'SourceRead','reason':'Waiting for required completed local Data evidence'} for e in config['entities']}
    except TransportDeferred as error:return {e:{'status':'Deferred','stage':'SourceRead','error':{'category':'AirtableRead','message':str(error),'write-outcome':'NotApplicable'}} for e in config['entities']}
    if source is None:return {e:{'status':'Deferred','stage':'SourceRead','reason':'Waiting for verified completed-day Data input'} for e in config['entities']}
    for entity,schema in config['entities'].items():
        try:results[entity]=process(entity,schema,client,source,authority,assessments)
        except TransportDeferred as error:results[entity]={'status':'Deferred','stage':'Readback' if error.operation['method']=='GET' else 'Persistence','error':{'category':'Transport','operation':error.operation['method'],'target':error.operation['table'],'message':str(error),'write-outcome':error.operation.get('write-outcome','Unknown')}}
        except Exception as error:
            result={'status':'Failed','stage':'Apply','error':{'category':'Validation','message':type(error).__name__+': '+str(error),'write-outcome':'Unknown'}}
            atomic(ROOT/'runtime'/entity/'stopped.json',{'at':stamp(),**result});results[entity]=result
    return results

if __name__=='__main__':
    try:print(packed(run()))
    except Exception as error:print(packed({'error':type(error).__name__+': '+str(error)}));raise SystemExit(1)
