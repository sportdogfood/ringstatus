"""Existing Airtable problem rows, keyed by show/day/endpoint."""
import json,pathlib,datetime
from urllib.parse import urlencode
from fast_collector import capture_safe

BASE='appCaSmL819KK7UFJ'


class Failures:
    def __init__(self,api,root,show,day):
        self.api=api;self.show=show;self.day=day
        config=json.loads((pathlib.Path(root)/'day-failure-schema.json').read_text(encoding='utf-8'))
        self.table=config['id'];self.fields={f['name']:f['id'] for f in config['fields']}
        prefix=show+'|'+day+'|'
        self.rows={r['fields'][self.fields['failure-key']]:r for r in api.records(BASE,self.table,formula='LEFT({failure-key},'+str(len(prefix))+')='+json.dumps(prefix))}
        self.links=None

    def pending(self):
        return {key.rsplit('|',1)[1]:row for key,row in self.rows.items() if row['fields'].get(self.fields['status'])!='Recovered'}

    def evidence(self,route):
        row=self.rows.get(self.show+'|'+self.day+'|'+route,{})
        try:return json.loads(row.get('fields',{}).get(self.fields['evidence'],'{}'))
        except (ValueError,TypeError):return {}

    def record(self,route,metric,runkey,error=None,save_failed=False):
        key=self.show+'|'+self.day+'|'+route;old=self.rows.get(key);fields=self.fields
        if not error and not old:return
        current=self.api.records(BASE,self.table,formula='{failure-key}='+json.dumps(key))
        old=current[0] if current else None
        observed=metric.get('completed')
        if not observed:raise ValueError('Failure observation has no source time')
        prior=old.get('fields',{}) if old else {}
        # A delayed replay must not reopen an endpoint recovered by a newer run.
        previous=prior.get(fields['last-observed-at'])
        if previous and datetime.datetime.fromisoformat(previous.replace('Z','+00:00'))>datetime.datetime.fromisoformat(observed.replace('Z','+00:00')):return
        if self.links is None:
            shows=self.api.records(BASE,'tblJA5CUon5s1sWib',formula='{show-key}='+json.dumps(self.show))
            days=self.api.records(BASE,'tbl14MaCCDw6dAywH',formula='{show-day-key}='+json.dumps(self.show+'|'+self.day))
            self.links=([r['id'] for r in shows],[r['id'] for r in days])
        url='https://wef-schedules-decoded.gombcg.workers.dev'+route+'?'+urlencode(metric.get('params',{}))
        evidence=self.evidence(route)
        evidence.update({k:v for k,v in {'capture':metric.get('saved-response'),'source-completed':observed,'run':runkey}.items() if v is not None})
        values={'failure-key':key,'status':'Save failed' if save_failed else 'Pending trouble' if error else 'Recovered',
                'show':self.links[0],'show-day':self.links[1],'endpoint':url,'run-id':runkey,
                'last-observed-at':observed,'problem':capture_safe(str(error)) if error else 'Returned data saved successfully',
                'save-status':'Failed' if save_failed else 'Unverified' if error else 'Saved',
                'trouble-status':'Pending' if error else 'Recovered','evidence':json.dumps(capture_safe(evidence)),
                'repeat-count':int(prior.get(fields['repeat-count'],0))+(1 if error else 0)}
        if not old:values['first-observed-at']=observed
        if route.startswith('/classes/'):values['class-id']=int(route.rsplit('/',1)[1])
        if metric.get('http') is not None:values['proxy-status']=metric['http']
        headers=metric.get('headers',{})
        if headers.get('x-rs-source-lane'):values['working-lane']=headers['x-rs-source-lane']
        if headers.get('x-rs-header-profile'):values['profile']=headers['x-rs-header-profile']
        result=self.api.request('PATCH',BASE,self.table,body={'performUpsert':{'fieldsToMergeOn':[fields['failure-key']]},'records':[{'fields':{fields[k]:v for k,v in values.items()}}]})
        self.rows[key]=result['records'][0]
