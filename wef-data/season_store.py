"""Season-only idempotent writes; exact source keys and destination-local links."""
import datetime,json,pathlib
from data_contract import packed
from wef_data_store import equivalent
from wef_data_airtable import TransportDeferred

BASES={'appzZsd3UqMRR2CNc','appkWkm9rFLrkUvaD','appETrjio9Yj9Pv17','appxMk9wfbBHEvC5e'}
def epoch(value):return datetime.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()

class SeasonStore:
    def __init__(self,client,schema,runtime):
        if schema['base'] not in BASES:raise ValueError('Not an approved Season base')
        self.client=client;self.base=schema['base'];self.schema=schema['tables'];self.root=pathlib.Path(runtime);self.root.mkdir(parents=True,exist_ok=True)
        self.fields={t:{f['name']:f for f in d['fields']} for t,d in self.schema.items()}
        self.primary={t:next(f['name'] for f in d['fields'] if f['id']==d['primaryFieldId']) for t,d in self.schema.items()}
    def inventory(self,table):
        rows=self.client.records(self.base,self.schema[table]['id']);out={};primary=self.fields[table][self.primary[table]]['id']
        for row in rows:
            key=str(row['fields'].get(primary,''))
            if not key or key in out:raise ValueError('Season missing/duplicate existing key')
            out[key]=row
        return out
    def apply(self,graph):
        if set(graph)!=set(self.schema):raise ValueError('Season table scope mismatch')
        inventory={t:self.inventory(t) for t in graph};counters={'records-added':0,'records-updated':0,'records-unchanged':0}
        pending=self.root/'pending-create.json'
        if pending.exists():
            plan=json.loads(pending.read_text());rows=inventory[plan['table']]
            if any(k not in rows for k in plan['keys']):raise TransportDeferred('POST',self.base,self.schema[plan['table']]['id'],{'reservation':'Uncertain Season create awaiting exact ID reconciliation'})
            pending.unlink()
        # Validate all source identities before any new mutation.
        for table,rows in graph.items():
            for key,row in rows.items():
                if str(row[self.primary[table]])!=key:raise ValueError('Season candidate key mismatch')
                old=inventory[table].get(key,{}).get('fields',{})
                for name,value in row.items():
                    if name not in self.fields[table]:raise ValueError('Unknown Season field')
                    if name.endswith('-id') and old.get(self.fields[table][name]['id']) not in (None,value):raise ValueError('Season source ID conflict')
        wanted={}
        for table,rows in graph.items():
            definition=self.schema[table];fields=self.fields[table];primary=self.primary[table]
            missing=sorted(set(rows)-set(inventory[table]))
            for first in range(0,len(missing),10):
                keys=missing[first:first+10];from data_runner import atomic
                atomic(pending,{'table':table,'keys':keys})
                try:result=self.client.request('POST',self.base,definition['id'],body={'records':[{'fields':{fields[primary]['id']:key}} for key in keys]})
                except TransportDeferred as error:
                    if error.operation.get('write-outcome')=='NotSent':pending.unlink(missing_ok=True)
                    raise
                for record in result['records']:
                    key=str(record['fields'].get(fields[primary]['id'],record['fields'].get(primary,'')))
                    if key not in keys or key in inventory[table]:raise ValueError('Season ambiguous created key')
                    inventory[table][key]=record
                if any(k not in inventory[table] for k in keys):raise ValueError('Season missing created ID')
                pending.unlink();counters['records-added']+=len(keys)
            patches=[];wanted[table]={}
            for key,row in rows.items():
                old=inventory[table][key]['fields'];output={}
                source_at=row.get('last-success');old_at=old.get(fields.get('last-success',{}).get('id'))
                older=bool(old_at and (not source_at or epoch(source_at)<epoch(old_at)-0.0011))
                for name,value in row.items():
                    f=fields[name];prior=old.get(f['id'])
                    if f['type']=='multipleRecordLinks':
                        target={'show':'shows','shows':'shows','show-days':'show-days'}[name]
                        ids=[inventory[target][k]['id'] for k in value]
                        previous=[v['id'] if isinstance(v,dict) else v for v in prior or []]
                        if name=='show' and previous and set(previous)!=set(ids):raise ValueError('Season day reparenting conflict')
                        value=sorted(set(ids+previous))
                        if table=='rings' and len(value)!=1:raise ValueError('Season dated ring membership conflict')
                    elif older and name!=primary:continue
                    output[f['id']]=value
                wanted[table][key]=output
                changed={fid:value for fid,value in output.items() if not equivalent(old.get(fid),value,next(f for f in fields.values() if f['id']==fid))}
                if changed:
                    patches.append({'id':inventory[table][key]['id'],'fields':changed})
                    if key not in missing:counters['records-updated']+=1
                else:counters['records-unchanged']+=1
            for first in range(0,len(patches),10):self.client.request('PATCH',self.base,definition['id'],body={'records':patches[first:first+10]})
            actual=self.inventory(table)
            for key,values in wanted[table].items():
                if actual[key]['id']!=inventory[table][key]['id']:raise ValueError('Season record ID changed')
                for fid,value in values.items():
                    if not equivalent(actual[key]['fields'].get(fid),value,next(f for f in fields.values() if f['id']==fid)):raise ValueError('Season readback mismatch')
            inventory[table]=actual
        return counters
