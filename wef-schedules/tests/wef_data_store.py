"""Preserve Data keys/IDs and verify staged linked writes.

This layer never deletes records or publishes a completed revision. A caller
must publish its revision fence only after stage() returns verified receipts.
"""
import datetime
import json

DATA_BASE = 'appdl16HJ5ETm8N69'


class DataStore:
    def __init__(self, client, schema, cache=False):
        if schema.get('base') != DATA_BASE:
            raise ValueError('Wrong destination base')
        self.client=client
        self.cache_enabled=cache;self.cache={}
        self.schema=schema['tables']
        self.fields={t:{f['name']:f for f in s['fields']} for t,s in self.schema.items()}
        self.primary={t:next(f['name'] for f in s['fields'] if f['id']==s['primaryFieldId'])
                      for t,s in self.schema.items()}

    def inventory(self,table,refresh=False,keys=None):
        if self.cache_enabled and table in self.cache and not refresh:return self.cache[table]
        fields=self.fields[table]
        if keys is not None and self.cache_enabled:
            rows=[];keys=list(keys)
            for start in range(0,len(keys),20):
                terms=['{'+self.primary[table]+'}='+json.dumps(str(k)) for k in keys[start:start+20]]
                formula='OR('+','.join(terms)+')'
                rows.extend(self.client.records(DATA_BASE,self.schema[table]['id'],formula=formula))
        else:rows=self.client.records(DATA_BASE,self.schema[table]['id'])
        primary_id=fields[self.primary[table]]['id']
        indexed={}
        for row in rows:
            key=row['fields'].get(primary_id)
            if key is None or str(key) in indexed:
                raise ValueError('Missing/duplicate stable key: '+table)
            indexed[str(key)]=row
        if self.cache_enabled:
            if keys is None:self.cache[table]=indexed
            else:
                self.cache.setdefault(table,{})
                for key in keys:self.cache[table].pop(key,None)
                self.cache[table].update(indexed)
        return indexed

    def fields_for(self,table,row,mapping):
        output={}
        for name,value in row.items():
            if name not in self.fields[table]:
                raise ValueError('Unknown authored field: '+table+'.'+name)
            if isinstance(value,dict) and '$links' in value:
                targets=[]
                for target,key in value['$links']:
                    if key not in mapping.get(target,{}):
                        raise ValueError('Unresolved linked key')
                    targets.append(mapping[target][key])
                value=targets
            output[self.fields[table][name]['id']]=value
        return output

    def reserve(self,table,keys):
        """Idempotent server-side reservation using only the unchanged primary key."""
        keys=list(map(str,keys));primary=self.fields[table][self.primary[table]]['id']
        if not keys or len(keys)>10 or len(set(keys))!=len(keys):raise ValueError('Invalid reservation batch')
        result=self.client.request('PATCH',DATA_BASE,self.schema[table]['id'],body={
            'performUpsert':{'fieldsToMergeOn':[primary]},
            'records':[{'fields':{primary:key}} for key in keys]})
        returned={str(r.get('fields',{}).get(primary,r.get('fields',{}).get(self.primary[table]))):r.get('id') for r in result.get('records',[])}
        if len(result.get('records',[]))!=len(keys) or set(returned)!=set(keys) or not all(returned.values()):raise ValueError('Ambiguous reservation response')
        actual=self.inventory(table,refresh=True,keys=keys)
        if any(actual.get(key,{}).get('id')!=returned[key] for key in keys):raise ValueError('Reservation readback mismatch')
        return returned

    def stage(self,graph):
        # Validate all proposed keys/fields/links before the first write.
        keys={}
        for table,rows in graph.items():
            if table not in self.schema:raise ValueError('Unknown Data table')
            key=self.primary[table]
            values=[str(r[key]) for r in rows]
            if len(set(values))!=len(values):raise ValueError('Duplicate candidate key')
            keys[table]=set(values)
            for row in rows:
                if set(row)-set(self.fields[table]):raise ValueError('Unknown authored fields')
                if any(isinstance(v,str) and len(v.encode('utf-8'))>80000 for v in row.values()):
                    raise ValueError('Authored cell exceeds accepted byte budget')
        targets=set(graph)
        for rows in graph.values():
            for row in rows:
                for value in row.values():
                    if isinstance(value,dict) and '$links' in value:
                        targets.update(t for t,k in value['$links'])
        inventory={t:self.inventory(t) for t in targets}
        for table,rows in graph.items():
            for row in rows:
                old=inventory[table].get(str(row[self.primary[table]]))
                if not old:continue
                if table=='collections' and 'Verified' in str(old['fields'].get(self.fields[table]['status']['id'],'')):
                    raise ValueError('Completed collection is immutable')
                if table in ('horses','riders','trainers','owners','entries') and row.get('last-success'):
                    prior=old['fields'].get(self.fields[table]['last-success']['id'])
                    if prior and datetime.datetime.fromisoformat(prior.replace('Z','+00:00'))>datetime.datetime.fromisoformat(row['last-success'].replace('Z','+00:00')):
                        raise ValueError('Stale shared projection; prior source facts retained')
                for name,value in row.items():
                    if name.endswith('-id') and name in self.fields[table]:
                        prior=old['fields'].get(self.fields[table][name]['id'])
                        if prior is not None and value is not None and prior!=value:
                            raise ValueError('Conflicting existing source identity: '+table)
        mapping={t:{k:r['id'] for k,r in rows.items()} for t,rows in inventory.items()}
        for table,rows in graph.items():
            for row in rows:
                for value in row.values():
                    if isinstance(value,dict) and '$links' in value:
                        for target,key in value['$links']:
                            if key not in keys.get(target,set()) and key not in mapping.get(target,{}):
                                raise ValueError('Candidate contains unresolved links')
        runs=graph.get('runs',[])
        if len(runs)!=1:raise ValueError('One owning run required per staged revision')
        runkey=str(runs[0][self.primary['runs']])
        # A previous successful run must not be overwritten by a staged replay.
        existing=inventory['runs'].get(runkey)
        status=self.fields['runs']['status']['id']
        if existing and ('Verified' in str(existing['fields'].get(status,''))):
            raise ValueError('Completed run is immutable; use a distinct revision')
        if existing:
            self.client.request('PATCH',DATA_BASE,self.schema['runs']['id'],body={'records':[{
                'id':existing['id'],'fields':{status:'Writing; readback pending'}}]})
        for table in ['runs']+[t for t in graph if t!='runs']:
            primary=self.primary[table];primary_id=self.fields[table][primary]['id']
            missing=[str(row[primary]) for row in graph[table] if str(row[primary]) not in mapping[table]]
            for start in range(0,len(missing),10):
                batch=missing[start:start+10]
                mapping[table].update(self.reserve(table,batch))
        self.client.request('PATCH',DATA_BASE,self.schema['runs']['id'],body={'records':[{
            'id':mapping['runs'][runkey],'fields':{status:'Writing; readback pending'}}]})
        expected={}
        for table,rows in graph.items():
            expected[table]={}
            for row in rows:
                fields=self.fields_for(table,row,mapping)
                if table in ('runs','collections'):
                    fields[self.fields[table]['status']['id']]='Writing; readback pending'
                key=str(row[self.primary[table]])
                expected[table][key]=fields
            records=[]
            for key,fields in expected[table].items():
                old=inventory[table].get(key,{}).get('fields',{})
                changed={fid:value for fid,value in fields.items() if not equivalent(old.get(fid),value,
                    next(f for f in self.fields[table].values() if f['id']==fid))}
                prior_status=old.get(self.fields[table].get('status',{}).get('id'),'')
                if table=='collections' and 'Verified' in str(prior_status):
                    if any(fid!=self.fields[table]['status']['id'] for fid in changed):
                        raise ValueError('Completed collection is immutable')
                    continue
                if changed:records.append({'id':mapping[table][key],'fields':changed})
            for start in range(0,len(records),10):
                self.client.request('PATCH',DATA_BASE,self.schema[table]['id'],
                    body={'records':records[start:start+10]})
        proof={}
        for table,rows in expected.items():
            observed=self.inventory(table,refresh=True,keys=rows)
            for key,fields in rows.items():
                actual=observed.get(key)
                if actual is None or actual['id']!=mapping[table][key]:
                    raise ValueError('Readback identity mismatch')
                for fid,value in fields.items():
                    got=actual['fields'].get(fid)
                    definition=next(f for f in self.fields[table].values() if f['id']==fid)
                    if not equivalent(got,value,definition):
                        raise ValueError('Readback field mismatch: '+table+'.'+definition['name'])
            proof[table]={'expected':len(rows),'collected':len(rows),'readback':len(rows)}
        return {'status':'Verified staged graph; completion not published',
                'record_map':mapping,'tables':proof}


def equivalent(got,wanted,field):
    if got in (None,'',[]) and wanted in (None,'',[]):return True
    kind=field['type']
    if kind=='multipleRecordLinks':
        return {v['id'] if isinstance(v,dict) else v for v in (got or [])}==set(wanted or [])
    if kind in ('dateTime','date') and got and wanted:
        try:
            a=datetime.datetime.fromisoformat(got.replace('Z','+00:00'))
            b=datetime.datetime.fromisoformat(wanted.replace('Z','+00:00'))
            return abs((a-b).total_seconds())<0.0011
        except (ValueError,TypeError):return False
    if kind=='number' and isinstance(got,(int,float)) and isinstance(wanted,(int,float)):
        return abs(got-wanted)<=0.5*10**(-field.get('options',{}).get('precision',0))
    return got==wanted


if __name__=='__main__':
    import argparse
    from wef_data_airtable import Airtable
    parser=argparse.ArgumentParser(description='Persist and read back a staged Data graph; never publish completion')
    parser.add_argument('--graph',required=True)
    parser.add_argument('--schema',required=True)
    args=parser.parse_args()
    with open(args.graph,encoding='utf-8') as source:graph=json.load(source)
    with open(args.schema,encoding='utf-8') as source:schema=json.load(source)
    receipt=DataStore(Airtable(),schema).stage(graph)
    print(json.dumps({'status':receipt['status'],'tables':receipt['tables']}))
