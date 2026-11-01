"""Offline boundaries only: no real source, Airtable, Git or business runner."""
import copy,datetime,importlib.util,json,os,pathlib,re,subprocess,sys,tempfile,unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'tests'),str(ROOT.parent/'wef-schedules/tests')]
import data_runner as dr
from test_store import MemoryAPI
from test_scheduled_contract import SCHEMA,AUTHORED,seed

SHOW='15|200000076'
AT=datetime.datetime(2026,11,10,16,tzinfo=datetime.timezone.utc).timestamp()

class API(MemoryAPI):
    def __init__(self):
        super().__init__();self.at=AT;self.reads=[]
        self.context={'records':[{'show-key':SHOW,'customer-id':15,'show-id':200000076,'show-name':'Generic test show',
            'start-date':'2026-11-07','end-date':'2026-11-13','selected-date':'2026-11-10','default':True,'last-checked':dr.stamp()}]}
        self.days=[{'id':'showday'+str(n),'fields':{'show-day-key':SHOW+'|'+(datetime.date(2026,11,10)+datetime.timedelta(days=n)).isoformat(),
            'show':['canonicalShow'],'date':(datetime.date(2026,11,10)+datetime.timedelta(days=n)).isoformat(),'day-priority':n}} for n in range(-3,4)]
        seed(self)
    def canonical_context(self):
        value=copy.deepcopy(self.context)
        value['authority-base']='appCaSmL819KK7UFJ'
        value['records'][0]['authority-checked-at']=datetime.datetime.fromtimestamp(self.at,datetime.timezone.utc).isoformat()
        return value
    def records(self,base,table,fields=None,formula=None):
        self.reads.append((base,table,formula))
        if table==dr.SYNC_SHOWS:
            value={**self.context['records'][0],'last-checked':datetime.datetime.fromtimestamp(self.at,datetime.timezone.utc).isoformat()}
            return [{'id':'canonicalShow','fields':{field:value.get(name,False if name=='default' else None) for name,field in dr.SYNC_SHOW_FIELDS.items()}}]
        if table==dr.SYNC_DAYS:
            names={'show-day-key':'fld4RBSJRXKCbVU1A','show':'fldu4VmXs9yz4mTEa','date':'fldOeAP4c7F1lxIEd','day-priority':'fldP1IRkAD0E2HIND'}
            return [{'id':row['id'],'fields':{names[k]:v for k,v in row['fields'].items()}} for row in self.days]
        rows=super().records(base,table,fields)
        defs=next(t['fields'] for t in SCHEMA['tables'].values() if t['id']==table)
        names={f['name']:f['id'] for f in defs}
        if formula:
            left=re.search(r'LEFT\(\{([^}]+)\},(\d+)\)=("[^"]*")',formula)
            ids=re.findall(r'RECORD_ID\(\)=("[^"]*")',formula)
            keys=re.findall(r'\{([^}]+)\}=("(?:[^"\\]|\\.)*")',formula)
            if left:rows=[r for r in rows if str(r['fields'].get(names[left[1]],''))[:int(left[2])]==json.loads(left[3])]
            elif ids:rows=[r for r in rows if r['id'] in [json.loads(v) for v in ids]]
            elif keys:rows=[r for r in rows if any(str(r['fields'].get(names[n]))==json.loads(v) for n,v in keys)]
            else:raise AssertionError('Unmodelled scoped formula: '+formula)
        if fields:
            wanted={names.get(f,f) for f in fields};rows=[{**r,'fields':{k:v for k,v in r['fields'].items() if k in wanted}} for r in rows]
        return rows

class Source:
    def __init__(self,api):self.api=api;self.calls=[]
    def fetch(self,route,params):
        self.calls.append((route,copy.deepcopy(params)))
        sid=self.api.context['records'][0]['show-id']
        if route=='/schedule':
            day=params['date'];cid=100+(datetime.date.fromisoformat(day)-datetime.date(2026,11,10)).days
            payload={'show':{'show_id':sid,'facility_id':500,'start_date':'2026-11-07','end_date':'2026-11-13'},'show_date':day,
                'rings':[{'ring_number':1,'ring_id':10,'ring_name':'RING','classes':[{'class_id':cid,'class_group_id':3,'class_number':cid,'class_name':'CLASS','class_groupxclasses_id':cid+1000}]}]}
        else:
            cid=int(route.rsplit('/',1)[1]);day=(datetime.date(2026,11,10)+datetime.timedelta(days=cid-100)).isoformat()
            payload={'show_id':sid,'class':{'class_id':cid},'class_related_data':{'date':day,'ring':1,'class_group_id':3,'status':'Completed'},
                'total_entry_trips':1,'trips':[{'entry_id':cid,'horse_id':cid+1000,'rider_id':50,'rider_name':'RIDER','trainer_id':60,'horse':'HORSE',
                    'trainer':'TRAINER','entryxclasses_uuid':'existing-uuid-'+str(cid),'trip_id':cid+2000,'number':cid}]}
        at=datetime.datetime.fromtimestamp(self.api.at,datetime.timezone.utc).isoformat()
        return payload,{'route':route,'params':params,'started':at,'completed':at,'elapsed_seconds':.01,'bytes':len(dr.packed(payload).encode()),'sha256':dr.digest(dr.packed(payload)),'http':200}

