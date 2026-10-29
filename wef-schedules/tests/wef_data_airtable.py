"""Data's Airtable transport and canonical wef-shows reader.

Credentials are supplied by the deployed host, never stored in captured data.
Only reads are retried automatically; a failed write is never blindly replayed.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

from wef_data_context import SHOWS_BASE, validate_context

SHOWS_TABLE = 'tblJA5CUon5s1sWib'
SHOW_FIELDS = {
    'show-key':'fldjn5SUt9HBgQdBs', 'show-id':'fldzwGGWrAFMwh7mJ',
    'customer-id':'fldSFgZFCbn2BYoJG', 'show-name':'fld341FzFAHXRhORn',
    'start-date':'fld8V7t8Nx7JhawnQ', 'end-date':'fldpYLDyXGuM1BUrt',
    'selected-date':'fldhc0ngVDJ45neim', 'default':'fldqdLdrX8LLTdHJg',
    'last-checked':'flde0zgjI9fjzdZY5',
}

class TransportDeferred(RuntimeError):
    """Retain the exact unit; reads retry boundedly, uncertain writes reconcile."""
    def __init__(self,method,base,table,body=None,write_outcome='Unknown'):
        super().__init__('Airtable transport deferred: '+method+' '+table)
        self.operation={'method':method,'base':base,'table':table,'body':body,'write-outcome':write_outcome}


class Airtable:
    def __init__(self, token=None, opener=None, sleep=time.sleep):
        self.token = token or os.environ.get('AIRTABLE_TOKEN')
        if not self.token:
            raise ValueError('Host-managed AIRTABLE_TOKEN is required')
        self.opener = opener or urllib.request.urlopen
        self.sleep = sleep
        self.last_request = 0

    def request(self, method, base, table, params=None, body=None):
        if not base.startswith('app') or not table.startswith('tbl'):
            raise ValueError('Explicit Airtable base/table IDs required')
        query = urllib.parse.urlencode(params or {}, doseq=True)
        url = 'https://api.airtable.com/v0/'+base+'/'+table
        if query:
            url += '?'+query
        payload = json.dumps(body).encode() if body is not None else None
        attempts = 4 if method == 'GET' else 1
        for attempt in range(attempts):
            delay=0.22-(time.monotonic()-self.last_request)
            if delay>0:self.sleep(delay)
            self.last_request=time.monotonic()
            request = urllib.request.Request(url, data=payload, method=method,
                headers={'Authorization':'Bearer '+self.token,
                         'Content-Type':'application/json'})
            try:
                with self.opener(request, timeout=30) as response:
                    value = json.load(response)
                if not isinstance(value, dict):
                    raise ValueError('Invalid Airtable response')
                return value
            except urllib.error.HTTPError as error:
                if error.code in (429,500,502,503,504) and (method!='GET' or attempt+1==attempts):
                    raise TransportDeferred(method,base,table,body,'NotSent' if error.code==429 else 'Unknown') from None
                if error.code not in (429,500,502,503,504) or attempt+1 == attempts:
                    # Do not expose headers, request objects or response bodies.
                    raise RuntimeError('Airtable HTTP '+str(error.code)) from None
                self.sleep(min(30, 2**attempt))
            except (urllib.error.URLError,TimeoutError,ConnectionError) as error:
                reason=getattr(error,'reason',error)
                if not isinstance(reason,(TimeoutError,ConnectionError,OSError)):
                    raise
                if method!='GET' or attempt+1==attempts:raise TransportDeferred(method,base,table,body) from None
                self.sleep(min(8,2**attempt))
        raise RuntimeError('Airtable request exhausted')

    def records(self, base, table, fields=None, formula=None):
        params = {'pageSize':100, 'returnFieldsByFieldId':'true'}
        if fields:
            params['fields[]'] = list(fields)
        if formula:params['filterByFormula']=formula
        rows, ids, offsets = [], set(), set()
        while True:
            page = self.request('GET',base,table,params)
            if not isinstance(page.get('records'),list):
                raise ValueError('Missing explicit Airtable records array')
            for row in page['records']:
                identity = row.get('id')
                if not isinstance(identity,str) or not identity.startswith('rec') or identity in ids:
                    raise ValueError('Missing or repeated record identity during pagination')
                if not isinstance(row.get('fields'),dict):
                    raise ValueError('Missing record fields')
                ids.add(identity)
                rows.append(row)
            offset = page.get('offset')
            if offset is None:
                return rows
            if not isinstance(offset,str) or not offset or offset in offsets:
                raise ValueError('Invalid or repeated Airtable page offset')
            offsets.add(offset)
            params['offset'] = offset

    def canonical_context(self, now=None):
        rows = self.records(SHOWS_BASE,SHOWS_TABLE,SHOW_FIELDS.values())
        context = {'authority-base':SHOWS_BASE,'records':[
            {name:row['fields'].get(field,False if name=='default' else None)
             for name,field in SHOW_FIELDS.items()} for row in rows]}
        # Validation cannot replace successful source-authority reads with an
        # empty graph or independently discover a different show.
        validate_context(context,now)
        return context


def canonical_fence(context):
    """Selection identity for before/after comparisons; no heartbeat false changes."""
    defaults = [r for r in context['records'] if r.get('default') is True]
    if len(defaults)!=1:
        raise ValueError('Canonical default must resolve exactly once')
    row=defaults[0]
    return tuple(row[k] for k in ('show-key','start-date','end-date','selected-date'))
