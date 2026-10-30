"""Scheduled Season consumer. No acquisition and no Data-owned writes."""
import json,pathlib,sys,time,traceback
ROOT=pathlib.Path(__file__).resolve().parent;PARENT=ROOT.parent
sys.path[:0]=[str(PARENT/'wef-data'),str(PARENT/'wef-schedules/tests')]
from data_runner import atomic,stamp
from data_contract import packed,digest
from wef_data_airtable import Airtable,canonical_fence,TransportDeferred
from wef_data_handoff import verify_snapshot,assignment_evidence
from retained_feed import load
from season_projection import project
from season_store import SeasonStore

def sources(context):
    runtime=PARENT/'wef-data/runtime';show=context['show-key'];path=runtime/'handoff'/('head-'+show.replace('|','-')+'.json')
    if not path.exists():return None
    pointer=json.loads(path.read_text(encoding='utf-8'))
    if pointer.get('authority')!=[context[k] for k in ('show-key','start-date','end-date','selected-date')]:return None
    current,proof=load(runtime/'handoff',pointer);output=[]
    receipts=runtime/'outgoing-receipts'
    for file in sorted(receipts.glob('*.json')):
        receipt=json.loads(file.read_text(encoding='utf-8'));folder=runtime/'archive-repository'/receipt['path']
        manifest,facts=verify_snapshot(folder)
        if manifest['show_key']!=receipt['show-key'] or not receipt.get('verified_remote_commit'):raise ValueError('Unverified historical archive receipt')
        if manifest['show_key']==show:continue
        historical=runtime/'handoff'/('head-'+manifest['show_key'].replace('|','-')+'.json')
        if historical.exists():
            old_facts,old_proof=load(runtime/'handoff',json.loads(historical.read_text(encoding='utf-8')))
            output.append((old_facts,{**old_proof,'revision':old_proof['state-sha256'],'kind':'retained','archive-path':receipt['path'],'archive-commit':receipt['verified_remote_commit']}))
        else:output.append((facts,{'show-key':manifest['show_key'],'revision':folder.name,'kind':'archive','path':receipt['path'],'commit':receipt['verified_remote_commit']}))
    output.append((current,{**proof,'revision':proof['state-sha256'],'kind':'retained'}));return output

def process(entity,config,client,source,authority,assessments=None):
    runtime=ROOT/'runtime'/entity;runtime.mkdir(parents=True,exist_ok=True)
    if (runtime/'stopped.json').exists():return {'status':'Skipped','stage':'StopGuard','reason':'Season entity stopped; owner approval required'}
    heartbeat=config['heartbeat'];workflow='wef-season-'+entity
    rows=client.records(config['base'],heartbeat['id'],formula='{heartbeat-key}='+json.dumps(workflow))
    if len(rows)!=1:raise ValueError('Season heartbeat missing/ambiguous')
    row=rows[0];field=heartbeat['checkpoint-field'];checkpoints=json.loads(row['fields'].get(field) or '{}')
    assessments={} if assessments is None else assessments
    counters={'records-added':0,'records-updated':0,'records-unchanged':0};changed=False;last_ref=None;assignment_counts={}
    for facts,reference in source:
        show=reference['show-key'];last_ref=reference
        if checkpoints.get(show,{}).get('revision')==reference['revision']:continue
        old=checkpoints.get(show,{})
        if reference.get('kind')=='retained' and old.get('kind')=='retained':
            if reference['sequence']<old.get('sequence',0):continue
            if reference['sequence']==old.get('sequence',0):raise ValueError('Conflicting Season source revision at same sequence')
        identity=(show,reference['revision'])
        if identity not in assessments:assessments[identity]=assignment_evidence(facts)
        assessment=assessments[identity];assignment_counts={**assessment['summary'],'show-key':show,'source-revision':reference['revision']}
        graph=project(facts,entity,assessment)
        result=SeasonStore(client,config,runtime).apply(graph)
        if canonical_fence(client.canonical_context())!=canonical_fence(authority):raise TransportDeferred('GET','appCaSmL819KK7UFJ','tblJA5CUon5s1sWib')
        for name,count in result.items():counters[name]+=count
        updated={**checkpoints,show:{**reference,'accepted-at':stamp()}}
        client.request('PATCH',config['base'],heartbeat['id'],body={'records':[{'id':row['id'],'fields':{field:packed(updated)}}]})
        actual=client.records(config['base'],heartbeat['id'],fields=[field],formula='{heartbeat-key}='+json.dumps(workflow))
        if len(actual)!=1 or actual[0]['id']!=row['id'] or json.loads(actual[0]['fields'].get(field) or '{}')!=updated:raise ValueError('Season checkpoint readback mismatch')
        checkpoints=updated;changed=True
    if assignment_counts:
        counters['assignment-source']={'show-key':assignment_counts['show-key'],'revision':assignment_counts['source-revision']}
        counters.update({'complete-trip-assignments':assignment_counts['complete-trips'],'incomplete-trip-assignments':assignment_counts['incomplete-trips'],'withheld-entry-assignments':assignment_counts['withheld-entries']})
    return {'status':'Succeeded' if changed else 'Skipped','stage':'Verified' if changed else 'Checkpoint','reason':('Parent assignments withheld; verified trip roles retained' if assignment_counts.get('withheld-entries') else None) if changed else 'All available verified revisions already applied','counts':counters,'assignment-counts':assignment_counts,'source-revision':last_ref['revision'] if last_ref else None,'show-key':last_ref['show-key'] if last_ref else None}

def run():
    config=json.loads((ROOT/'schema.json').read_text(encoding='utf-8'));client=Airtable();results={};assessments={}
    if all((ROOT/'runtime'/e/'stopped.json').exists() for e in config['entities']):return {e:{'status':'Skipped','stage':'StopGuard','reason':'Season entity stopped; owner approval required'} for e in config['entities']}
    try:
        authority=client.canonical_context();context=next(r for r in authority['records'] if r['default']);source=sources(context)
    except TransportDeferred as error:return {e:{'status':'Deferred','stage':'SourceRead','error':{'category':'AirtableRead','message':str(error),'write-outcome':'NotApplicable'}} for e in config['entities']}
    if source is None:return {e:{'status':'Deferred','stage':'SourceRead','reason':'Waiting for a verified current-show Data handoff'} for e in config['entities']}
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
