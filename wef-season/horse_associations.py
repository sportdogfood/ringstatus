"""Additive horse/person associations. Existing source keys only; conflicts are rows."""
import copy,json
from data_contract import packed
from season_store import SeasonStore

FIELDS={'rider':'rider-associations','trainer':'trainer-associations'}
LIBRARIES={
    'rider':{'table':'tblVarKFns19Neoo8','key':'fldU0qvodyDV3jgcY','field':'fldpCLtnCRHMrZcxp'},
    'trainer':{'table':'tblowMYNMQQeoJiqY','key':'fldEflWVfT1Nd7Ca0','field':'fldSnVmvciJqU2OhD'},
}

def native_ids(value):
    return sorted({v['id'] if isinstance(v,dict) else v for v in value or []})

def sync_library_links(client,config,store,inventory,rows,conflicts):
    libraries={}
    for role,definition in LIBRARIES.items():
        index={}
        for record in client.records(config['base'],definition['table'],fields=[definition['key']]):
            key=record['fields'].get(definition['key'])
            if isinstance(key,str) and key:index.setdefault(key,[]).append(record['id'])
        libraries[role]=index
    patches=[];wanted={};unmatched=0
    def issue(horse,reason,values):
        row=conflicts.setdefault(horse,{'source-key':horse,'source-type':'horses','horse-key':horse,'details':[]})
        value={'reason':reason,'values':values}
        if value not in row['details']:row['details'].append(value)
    for horse,record in inventory.items():
        changes={}
        for role,field in FIELDS.items():
            definition=LIBRARIES[role]
            value=rows.get(horse,{}).get(field,record['fields'].get(store.fields['horses'][field]['id']))
            try:
                associations=json.loads(value or '{}')
                if not isinstance(associations,dict) or any(not isinstance(v,dict) or any(not isinstance(v.get(n,[]),list) or any(not isinstance(k,str) for k in v.get(n,[])) for n in ('shows','show-days','sources')) for v in associations.values()):raise ValueError()
            except (ValueError,TypeError):
                issue(horse,'Unrecognized existing '+field,{'stored':value});continue
            previous=native_ids(record['fields'].get(definition['field']))
            supported=set(previous)
            for person in associations:
                matches=libraries[role].get(person,[])
                if len(matches)==1:supported.add(matches[0])
                else:
                    unmatched+=1
                    issue(horse,'Missing or ambiguous synced '+role+' key',{'person-key':person,'matches':sorted(matches)})
            if sorted(supported)!=previous:changes[definition['field']]=sorted(supported)
        if changes:
            patches.append({'id':record['id'],'fields':changes})
            wanted[horse]=(record['id'],changes)
    table=config['tables']['horses']['id']
    for first in range(0,len(patches),10):
        client.request('PATCH',config['base'],table,body={'records':patches[first:first+10]})
    if wanted:
        actual=store.inventory('horses')
        for horse,(identity,values) in wanted.items():
            if actual.get(horse,{}).get('id')!=identity:raise ValueError('Horse library link record ID changed')
            for field,expected in values.items():
                if native_ids(actual[horse]['fields'].get(field))!=expected:raise ValueError('Horse library link readback mismatch')
    return {'library-link-records-updated':len(patches),'library-link-records-unchanged':len(inventory)-len(patches),'library-link-unmatched':unmatched}

def links(row,field,table):
    return [key for target,key in row.get(field,{}).get('$links',[]) if target==table]

def prepare(facts,previous):
    state={};index={};conflicts={};invalid=set()
    def conflict(source,key,horse,reason,values):
        row=conflicts.setdefault(key,{'source-key':key,'source-type':source,'horse-key':horse or '', 'details':[]})
        issue={'reason':reason,'values':values}
        if issue not in row['details']:row['details'].append(issue)
    for horse,old in previous.items():
        state[horse]={}
        for role,field in FIELDS.items():
            try:
                value=json.loads(old.get(field) or '{}')
                if not isinstance(value,dict):raise ValueError()
                for person,association in value.items():
                    if not isinstance(association,dict) or any(not isinstance(association.get(n,[]),list) or any(not isinstance(v,str) for v in association.get(n,[])) for n in ('shows','show-days','sources')):raise ValueError()
                state[horse][field]=copy.deepcopy(value)
                for person,association in value.items():
                    for source in association.get('sources',[]):index.setdefault((source,role),set()).add((horse,person))
            except (ValueError,TypeError):
                invalid.add((horse,field));state[horse][field]={}
                conflict('horses',horse,horse,'Unrecognized existing '+field,{'stored':old.get(field)})
    def observe(kind,key,row,shows,days):
        horses=links(row,'horse','horses')
        if len(horses)!=1:
            if len(horses)>1:conflict(kind,key,None,'Multiple horses on one source',horses)
            return
        horse=horses[0]
        if horse not in facts['horses']:return
        if len(shows)!=1 or any(s not in facts['shows'] for s in shows):
            conflict(kind,key,horse,'Missing or contradictory show context',shows);return
        for day in days:
            if day not in facts['days'] or links(facts['days'][day],'show','shows')!=shows:
                conflict(kind,key,horse,'Day/show disagreement',{'shows':shows,'days':days});return
        for role,field in FIELDS.items():
            people=links(row,role if kind=='trips' else role+'s',role+'s')
            if len(people)>1:
                conflict(kind,key,horse,'Multiple '+role+' assignments on one source',people);continue
            if not people or (horse,field) in invalid:continue
            person=people[0]
            if person not in facts[role+'s']:
                conflict(kind,key,horse,'Unresolved '+role+' key',person);continue
            prior=index.get((key,role),set())
            if prior and prior!={(horse,person)}:
                conflict(kind,key,horse,'Changed '+role+' assignment on the same source',{'stored':sorted(prior),'incoming':[horse,person],'source-at':row.get('last-success')});continue
            association=state.setdefault(horse,{}).setdefault(field,{}).setdefault(person,{'shows':[],'show-days':[],'sources':[]})
            for name,values in [('shows',shows),('show-days',days),('sources',[key])]:
                association[name]=sorted(set(association.get(name,[]))|set(values))
            index.setdefault((key,role),set()).add((horse,person))
    for key,row in facts['trips'].items():
        classes=links(row,'class','classes')
        if len(classes)!=1 or classes[0] not in facts['classes']:
            if links(row,'horse','horses'):conflict('trips',key,links(row,'horse','horses')[0],'Missing or ambiguous trip class',classes)
            continue
        context=facts['classes'][classes[0]]
        observe('trips',key,row,links(context,'show','shows'),links(context,'day','days'))
    output={}
    for horse in facts['horses']:
        row={'horse-key':horse}
        for field in FIELDS.values():
            if (horse,field) not in invalid:row[field]=packed(state.get(horse,{}).get(field,{}))
        output[horse]=row
    return output,conflicts

def sync(client,config,facts,runtime,excluded=()):
    definition=copy.deepcopy(config['tables']['horses'])
    definition['fields']=[f for f in definition['fields'] if f['name'] in ('horse-key',*FIELDS.values())]
    store=SeasonStore(client,{'base':config['base'],'tables':{'horses':definition}},runtime/'associations')
    inventory={k:v for k,v in store.inventory('horses').items() if k not in excluded};fields=store.fields['horses']
    previous={key:{name:record['fields'].get(field['id']) for name,field in fields.items()} for key,record in inventory.items()}
    rows,conflicts=prepare(facts,previous)
    result=store.apply({'horses':rows})
    library_result=sync_library_links(client,config,store,inventory,rows,conflicts)
    if conflicts:
        logger=SeasonStore(client,config['conflicts'],runtime/'conflicts')
        existing=logger.inventory('conflicts');cf=logger.fields['conflicts']
        for key,row in conflicts.items():
            old=existing.get(key,{}).get('fields',{});details=json.loads(old.get(cf['details']['id']) or '[]')
            for issue in row['details']:
                if issue not in details:details.append(issue)
            row['details']=packed(details);row['status']='Open'
        logger.apply({'conflicts':conflicts})
    return {'association-'+name:value for name,value in result.items()}|{'association-conflicts':len(conflicts)}|library_result

