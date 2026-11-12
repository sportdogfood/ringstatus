"""Fill required empty Data links from saved records; no source calls."""
import json,pathlib
from wef_data_store import DATA_BASE

def fill_empty(writer):
    tables=writer.store.schema;data={t:writer.rows(t) for t in ('rings','groups','classes','schedules','entries','horses','riders','trainers','owners','trips','results')}
    ids={t:{k:r['id'] for k,r in writer.store.inventory(t).items()} for t in (*data,'w-shows','w-days','w-horses','w-riders','w-trainers')}
    plans={};norm=lambda v:' '.join(str(v or '').split()).casefold()
    def add(table,key,field,targets):
        row=data[table][key]
        if row.get(field) or field not in writer.store.fields[table]:return
        values=[ids.get(t,{}).get(str(k)) for t,k in targets]
        if values and all(values):plans.setdefault(table,{}).setdefault(key,{})[field]=list(dict.fromkeys(values))
    prefix=writer.show+'|'+writer.day+'|'
    for table,rows in data.items():
        for key,row in rows.items():
            if table in ('entries','horses','riders','trainers','owners'):
                # These shared records may belong to multiple days. Do not invent ownership.
                continue
            if not key.startswith(prefix):continue
            add(table,key,'w-shows',[('w-shows',writer.show)]);add(table,key,'w-days',[('w-days',writer.show+'|'+writer.day)])
            parts=key.split('|');ring='|'.join(parts[:4]);ck='|'.join(parts[:5])
            if table!='rings':add(table,key,'ring',[('rings',ring)])
            if table in ('trips','results'):add(table,key,'class',[('classes',ck)])
            if table=='schedules':
                raw=json.loads(row.get('source-json') or '{}');cid=raw.get('class_id');gid=raw.get('class_group_id')
                if gid is not None:add(table,key,'group',[('groups',ring+'|'+str(gid))])
                if cid is not None and not raw.get('schedule_break'):
                    ckey=ring+'|'+str(cid);add(table,key,'class',[('classes',ckey)])
                    add(table,key,'trips',[('trips',k) for k in data['trips'] if k.startswith(ckey+'|')])
            if table=='trips':
                raw=json.loads(row.get('source-json') or '{}');phone=raw.get('iphonev2',{})
                if raw.get('entry_id'):add(table,key,'entry',[('entries',parts[1]+'|'+str(raw['entry_id']))])
                for field,source,phone_name,target in [('horse','horse_id','Hor','horses'),('rider','rider_id','Rid','riders'),('trainer','trainer_id','Tra','trainers'),('owner','owner_id','Own','owners')]:
                    if raw.get(source):matches=[str(raw[source])] if str(raw[source]) in ids[target] else []
                    else:matches=[k for k,v in data[target].items() if norm(v.get(field+'-name'))==norm(phone.get(phone_name)) and norm(phone.get(phone_name))]
                    if len(matches)==1:
                        matched=matches[0];add(table,key,field,[(target,matched)])
                        add(target,matched,'w-shows',[('w-shows',writer.show)])
                        if target in ('horses','riders','trainers') and matched in ids['w-'+target]:add(target,matched,'w-'+target,[('w-'+target,matched)])
                if raw.get('entry_id'):
                    ek=parts[1]+'|'+str(raw['entry_id'])
                    if ek in data['entries']:
                        add('entries',ek,'w-shows',[('w-shows',writer.show)])
                        for field,source,target in [('horse','horse_id','horses'),('riders','rider_id','riders'),('trainers','trainer_id','trainers'),('owners','owner_id','owners')]:
                            if raw.get(source):add('entries',ek,field,[(target,str(raw[source]))])
                else:
                    candidates=[ek for ek,v in data['entries'].items() if ek.startswith(parts[1]+'|') and str(v.get('entry-number'))==str(phone.get('ENo')) and phone.get('ENo') not in (None,'')]
                    if len(candidates)==1:add(table,key,'entry',[('entries',candidates[0])])
            if table=='classes':
                groups={json.loads(s.get('source-json') or '{}').get('class_group_id') for sk,s in data['schedules'].items() if sk.rsplit('|',1)[0]==ring and json.loads(s.get('source-json') or '{}').get('class_id')==row.get('class-id')}
                groups.discard(None)
                if len(groups)==1:add(table,key,'group',[('groups',ring+'|'+str(next(iter(groups))))])
            if table=='results':
                add(table,key,'trip',[('trips',key)])
                if len(parts)>5 and parts[5]:add(table,key,'entry',[('entries',parts[1]+'|'+parts[5])])
    count=0
    for table,items in plans.items():
        keys=list(items)
        for start in range(0,len(keys),10):
            batch=keys[start:start+10];formula='OR('+','.join('RECORD_ID()='+json.dumps(ids[table][k]) for k in batch)+')'
            current={r['id']:r for r in writer.api.records(DATA_BASE,tables[table]['id'],formula=formula)}
            patches=[]
            for key in batch:
                identity=ids[table][key];row=current.get(identity)
                if not row:continue
                fields={writer.store.fields[table][f]['id']:v for f,v in items[key].items() if not row['fields'].get(writer.store.fields[table][f]['id'])}
                if fields:patches.append({'id':identity,'fields':fields})
            if not patches:continue
            writer.api.request('PATCH',DATA_BASE,tables[table]['id'],body={'records':patches})
            saved={r['id']:r for r in writer.api.records(DATA_BASE,tables[table]['id'],formula=formula)}
            for p in patches:
                if any(set(saved.get(p['id'],{}).get('fields',{}).get(f,[]))!=set(v) for f,v in p['fields'].items()):raise ValueError('Empty-link save not confirmed: '+p['id'])
                cache=writer.store.inventory(table)
                for key,row in cache.items():
                    if row['id']==p['id']:row['fields'].update(p['fields'])
            count+=len(patches)
    writer.counts['empty-links']=writer.counts.get('empty-links',0)+count
    return count

def run(root):
    from fast_workflow import Writer
    from fast_collector import synced_source
    from wef_data_airtable import Airtable
    api=Airtable();schema=json.loads((root/'schema.json').read_text());st,sf=synced_source('w-shows',root/'schema.json');dt,df=synced_source('w-days',root/'schema.json');owner=next(r['fields'] for r in api.records(DATA_BASE,st) if r['fields'].get(sf['is_default']) is True);errors=[];count=0
    writer=None
    for row in api.records(DATA_BASE,dt):
        f=row['fields']
        if f.get(df['show'])==owner[sf['show-key']] and f.get(df['day-priority']) in range(5):
            if writer is None:writer=Writer(api,schema,owner[sf['show-key']],f[df['date']][:10],errors)
            else:writer.day=f[df['date']][:10]
            count+=fill_empty(writer)
    return {'ok':not errors,'state':'Linked','counts':{'saved':count},'item-errors':errors}

if __name__=='__main__':
    try:result=run(pathlib.Path(__file__).resolve().parent)
    except Exception as e:result={'ok':False,'state':'Partial','counts':{},'item-errors':[{'key':'Linking','error':str(e)}]}
    print(json.dumps(result))
