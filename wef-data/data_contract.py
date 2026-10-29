"""Data completion evidence. Pure validation; no credentials or external effects."""
import base64, hashlib, json, math

def packed(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)

def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()

class DirectoryDrift(ValueError):
    """Valid scoped listing changed; requires a new coherent pass, never deletion."""
    def __init__(self,reason,expected,received):
        super().__init__(reason);self.expected=expected;self.received=received
    def evidence(self):return {'reason':str(self),'expected':self.expected,'received':self.received}

def directory_page(payload,show_id,requested,state):
    for key in ('entries','total_entries','records_per_page','page','show_id','sort','term'):
        if key not in payload:raise ValueError('Missing directory '+key)
    total,size,page=payload['total_entries'],payload['records_per_page'],payload['page']
    if any(type(x) is not int for x in (total,size,page)) or total<0 or size<=0 or page!=requested:
        raise ValueError('Invalid directory paging metadata')
    if str(payload['show_id'])!=str(show_id):raise ValueError('Wrong directory show')
    if payload['sort']!={'sort_on':'number','sort_type':'asc'} or payload['term']!='':
        raise ValueError('Directory filter/sort changed')
    rows=payload['entries']
    if not isinstance(rows,list):raise ValueError('Missing explicit entries array')
    state=state or {'total':total,'size':size,'next':1,'ids':[]}
    if state['next']!=requested:raise ValueError('Directory cursor order invalid')
    received={'total':total,'size':size,'page':page}
    previous={'total':state['total'],'size':state['size'],'page':state['next']}
    ids=list(state['ids']);seen=set(ids)
    within=set()
    for row in rows:
        if str(row.get('show_id'))!=str(show_id) or row.get('customer_id')!=15:
            raise ValueError('Directory entry context mismatch')
        identity=row.get('entry_id')
        if type(identity) is not int or identity<=0:raise ValueError('Missing directory entry identity')
        if identity in within:raise ValueError('Duplicate identity within directory page')
        within.add(identity)
    if (state['total'],state['size'])!=(total,size):raise DirectoryDrift('Directory total/size changed',previous,received)
    expected=max(1,math.ceil(total/size))
    if requested<1 or requested>expected:raise ValueError('Unexpected page')
    for row in rows:
        identity=row['entry_id']
        if identity in seen:raise DirectoryDrift('Duplicate identity across directory pages',previous,{**received,'duplicate-id':identity})
        seen.add(identity);ids.append(identity)
    wanted=0 if total==0 else size if requested<expected else total-size*(expected-1)
    if len(rows)!=wanted:raise ValueError('Directory page count mismatch')
    complete=requested==expected
    if complete and len(ids)!=total:raise ValueError('Directory unique count mismatch')
    return {'total':total,'size':size,'next':requested+1,'ids':ids,'complete':complete}

def freeze(value,budget=60000):
    # Base64 permits safe UTF-8 chunking with a strict encoded cell-byte bound.
    raw=base64.b64encode(packed(value).encode()).decode('ascii')
    width=((budget-300)//4)*4
    if width<4:raise ValueError('Payload budget too small')
    chunks=[raw[n:n+width] for n in range(0,len(raw),width)] or ['']
    output=[]
    for index,chunk in enumerate(chunks):
        text=packed({'part':index,'parts':len(chunks),'encoding':'base64-json','data':chunk})
        if len(text.encode())>budget:raise ValueError('Payload exceeds budget')
        output.append({'retained-json':text,'retained-sha256':digest(text)})
    return output

def thaw(records):
    parts=[];expected=None
    for index,row in enumerate(records):
        text=row['retained-json']
        if digest(text)!=row['retained-sha256']:raise ValueError('Frozen payload hash mismatch')
        part=json.loads(text)
        if part.get('encoding')!='base64-json' or part['part']!=index:raise ValueError('Wrong part order')
        expected=part['parts'] if expected is None else expected
        if expected!=part['parts']:raise ValueError('Mixed frozen parts')
        parts.append(part['data'])
    if expected!=len(parts):raise ValueError('Incomplete frozen payload')
    try:return json.loads(base64.b64decode(''.join(parts),validate=True))
    except Exception as error:raise ValueError('Invalid frozen payload') from error
