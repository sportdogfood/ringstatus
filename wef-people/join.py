"""Derived local People records; read shared inputs, write People only."""
import json
import pathlib
import sys
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / 'wef-schedules/tests'))
from wef_data_airtable import Airtable

BASE = 'app6uHgUYCCKTVlJE'

def issues_add(issues, category, target, message):
    issues.append({'category': category, 'target': target, 'message': message,
                   'operation': 'Join', 'write-outcome': 'NotApplicable'})

def index(rows, key):
    result = defaultdict(list)
    for row in rows:
        value = row['fields'].get(key)
        if isinstance(value, str) and value.strip():
            result[value].append(row)
    return result

def memberships(value):
    if value is None or value == '':
        return []
    if not isinstance(value, str):
        raise ValueError('Membership must be source text')
    return sorted(set(v.strip() for v in value.split(',') if v.strip()))

def project(sources):
    issues = []
    output = {}
    shows = index(sources['w-shows'], 'show-key')
    days = index(sources['w-days'], 'show-day-key')
    role_indexes = {role: index(sources['w-'+role+'s'], role+'-key') for role in ('rider', 'trainer')}
    for role in ('rider', 'trainer'):
        table = 'w-'+role+'s'
        for row in sources[table]:
            f = row['fields']; key = f.get(role+'-key'); problems = []
            valid = isinstance(key, str) and bool(key.strip())
            unique = valid and len(role_indexes[role][key]) == 1
            numeric = f.get(role+'-id')
            identity_ok = valid and unique and type(numeric) in (int, float) and float(numeric).is_integer() and str(int(numeric)) == key
            person_key = role+':'+key if identity_ok else role+':record:'+row['id']
            if not identity_ok:
                problems.append('Missing, duplicate or conflicting source identity; retained separately')
            other = 'trainer' if role == 'rider' else 'rider'
            if valid and key in role_indexes[other]:
                problems.append('Equal cross-role ID is unverified; no merge')
            linked = {'shows': [], 'days': []}
            try:
                show_keys = memberships(f.get('shows')); day_keys = memberships(f.get('show-days'))
            except ValueError as error:
                show_keys, day_keys = [], []; problems.append(str(error))
            for sk in show_keys:
                if len(shows.get(sk, [])) == 1:
                    linked['shows'].append(shows[sk][0]['id'])
                else:
                    problems.append('Unmatched/ambiguous show '+sk)
            for dk in day_keys:
                matches = days.get(dk, [])
                if len(matches) != 1:
                    problems.append('Unmatched/ambiguous day '+dk); continue
                parent = matches[0]['fields'].get('show')
                if parent not in show_keys or len(shows.get(parent, [])) != 1 or not dk.startswith(parent+'|'):
                    problems.append('Day/show membership mismatch '+dk); continue
                linked['days'].append(matches[0]['id'])
            fields = {'person-key': person_key, 'person-name': f.get(role+'-name', ''),
                'roles': role, 'source-key': str(key or ''), role+'s': [row['id']],
                'identity-status': 'Unresolved' if problems else 'Role-scoped; cross-role identity unverified',
                'source-evidence': json.dumps({'base':BASE,'table':table,'record':row['id'],'source-id':numeric,'source-key':key},sort_keys=True),
                'unresolved': '\n'.join(problems), **{k:sorted(v) for k,v in linked.items()}}
            observed = f.get('last-success')
            if observed:
                from datetime import datetime
                try:
                    parsed = datetime.fromisoformat(observed.replace('Z','+00:00'))
                    if parsed.tzinfo is None: raise ValueError('Missing timezone')
                    fields['source-observed-at'] = observed
                except (ValueError, TypeError, AttributeError):
                    problems.append('Invalid source observation timestamp')
                    fields['identity-status']='Unresolved'; fields['unresolved']='\n'.join(problems)
            for problem in problems:
                issues_add(issues,'Unresolved',table+'/'+row['id'],problem)
            output[person_key] = fields
    return output, issues

def named(client, schema, table):
    definition = schema['tables'][table]
    fields = {f['id']: f['name'] for f in definition['fields']}
    return [{'id':r['id'],'fields':{fields.get(k,k):v for k,v in r['fields'].items()}}
            for r in client.records(BASE, definition['id'], fields=list(fields))]

def same(a,b):
    if isinstance(b,list):return sorted(a or []) == sorted(b)
    if b == '':return (a or '') == ''
    if isinstance(b,str) and isinstance(a,str) and b.endswith('Z') and 'T' in b:
        from datetime import datetime
        try:return datetime.fromisoformat(a.replace('Z','+00:00')) == datetime.fromisoformat(b.replace('Z','+00:00'))
        except ValueError:pass
    return a == b

def apply(client, schema, desired, issues):
    table = schema['tables']['people']['id']
    existing = index(named(client,schema,'people'),'person-key')
    counts = {'added':0,'updated':0,'unchanged':0,'withheld':0,'source-people':len(desired)}
    changes = []
    for key, fields in desired.items():
        matches = existing.get(key,[])
        if len(matches)>1:
            issues_add(issues,'DuplicateDestination',key,'Ambiguous People key; skipped without repair')
            counts['withheld']+=1;continue
        old = matches[0] if matches else None
        expected = dict(fields)
        if old:
            prior = old['fields']
            # Preserve known memberships and native role links, never infer deletions.
            for name in ('riders','trainers','shows','days'):
                if name in expected:expected[name]=sorted(set(expected[name]+prior.get(name,[])))
            if prior.get('source-key') not in (None,'',fields['source-key']) or prior.get('roles') not in (None,'',fields['roles']):
                issues_add(issues,'IdentityConflict',key,'Existing identity differs; skipped without repair')
                counts['withheld']+=1;continue
            if prior.get('source-observed-at') and expected.get('source-observed-at'):
                from datetime import datetime
                if datetime.fromisoformat(expected['source-observed-at'].replace('Z','+00:00')) < datetime.fromisoformat(prior['source-observed-at'].replace('Z','+00:00')):
                    for field in ('source-observed-at','person-name','source-evidence'):
                        if field in prior:expected[field]=prior[field]
            patch={k:v for k,v in expected.items() if not same(prior.get(k),v)}
            if not patch:counts['unchanged']+=1;continue
            changes.append(('PATCH',{'id':old['id'],'fields':patch},key,expected))
        else:changes.append(('POST',{'fields':expected},key,expected))
    # Unknown write outcomes stop the workflow; no retry or second create.
    for method in ('POST','PATCH'):
        selected=[v for v in changes if v[0]==method]
        for start in range(0,len(selected),10):
            batch=selected[start:start+10]
            client.request(method,BASE,table,body={'records':[v[1] for v in batch]})
            counts['added' if method=='POST' else 'updated']+=len(batch)
    if changes:
        actual=index(named(client,schema,'people'),'person-key')
        for method, record, key, expected in changes:
            found=actual.get(key,[])
            if len(found)!=1 or (method=='PATCH' and found[0]['id']!=record['id']) or any(not same(found[0]['fields'].get(k),v) for k,v in expected.items()):
                raise ValueError('People readback mismatch: '+key)
    return counts

def run():
    schema=json.loads((ROOT/'schema.json').read_text(encoding='utf-8'))
    if schema['base']!=BASE:raise ValueError('Wrong People target')
    client=Airtable()
    sources={t:named(client,schema,t) for t in ('w-riders','w-trainers','w-shows','w-days','w-horses')}
    desired,issues=project(sources)
    counts=apply(client,schema,desired,issues)
    counts['issues']=len(issues)
    return {'status':'Partial' if issues else 'Succeeded','stage':'JoinReadback','counts':counts,'issues':issues,
            'reason':'Shared-input join completed; owners excluded and horses unlinked by owner'}

if __name__=='__main__':
    try:print(json.dumps(run()))
    except Exception as error:
        print(json.dumps({'status':'Failed','stage':'Join','error':{'category':type(error).__name__,'message':str(error),'write-outcome':'Unknown'}}))
        sys.exit(1)
