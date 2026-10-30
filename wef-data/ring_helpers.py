"""Unique facility rings from verified retained Data, using established helper keys."""
from season_store import SeasonStore,epoch

def positive(value,label):
    if isinstance(value,bool) or not isinstance(value,int) or value<=0:raise ValueError('Invalid helper '+label)
    return value

def scope(key):
    parts=key.split('|')
    if len(parts)!=3 or not parts[2].startswith(('id:','number:')):raise ValueError('Invalid existing helper key')
    customer,facility=int(parts[0]),int(parts[1]);positive(customer,'customer');positive(facility,'facility')
    positive(int(parts[2].split(':')[1]),'key identity')
    return customer,facility

def candidates(source,config,existing):
    registry={}
    for row in [*config.get('existing-identities',[]),*existing]:
        key=row['ring-key'];customer,facility=scope(key)
        prior=registry.setdefault(key,{'ring-key':key,'customer-id':customer,'facility-id':facility})
        kind,identity=key.split('|')[2].split(':')
        identity_field='ring-id' if kind=='id' else 'ring-number'
        if row.get(identity_field) not in (None,int(identity)):raise ValueError('Existing helper key/identity mismatch')
        prior[identity_field]=int(identity)
        for name in ('ring-id','ring-number'):
            value=row.get(name)
            if value is None:continue
            positive(value,name)
            if prior.get(name) not in (None,value):raise ValueError('Conflicting existing helper identity')
            prior[name]=value
    result={};clocks={}
    for facts,reference in source:
        for ring in facts.get('rings',{}).values():
            links=ring.get('show',{}).get('$links',[])
            if links!=[['shows',reference['show-key']]]:raise ValueError('Helper ring/show provenance mismatch')
            show=facts['shows'][reference['show-key']]
            customer=positive(show.get('customer-id'),'customer');facility=positive(show.get('facility-id'),'facility')
            number=positive(ring.get('ring-number'),'ring-number');rid=ring.get('ring-id')
            if rid is not None:positive(rid,'ring-id')
            matches=[row for row in registry.values() if (row['customer-id'],row['facility-id'])==(customer,facility) and ((rid is not None and row.get('ring-id')==rid) or row.get('ring-number')==number)]
            if len(matches)>1:raise ValueError('Ambiguous existing helper identity')
            if matches:
                resolved=matches[0];key=resolved['ring-key']
                if resolved.get('ring-number') not in (None,number) or (rid is not None and resolved.get('ring-id') not in (None,rid)):raise ValueError('Conflicting helper source identity')
            else:
                # Existing show-rings.js convention; never replace a resolved key.
                key=f'{customer}|{facility}|'+(f'id:{rid}' if rid is not None else f'number:{number}')
                resolved=registry.setdefault(key,{'ring-key':key,'customer-id':customer,'facility-id':facility})
            resolved['ring-number']=number
            if rid is not None:resolved['ring-id']=rid
            row=result.setdefault(key,{'ring-key':key,'ring-number':number})
            if rid is not None:row['ring-id']=rid
            name=ring.get('ring-name');at=ring.get('last-success');clock=epoch(at) if at else float('-inf')
            if name and (key not in clocks or clock>clocks[key]):row['name']=name;clocks[key]=clock
            elif name and clock==clocks.get(key) and row.get('name')!=name:raise ValueError('Ambiguous helper names at same source time')
    return result

def sync(client,config,source,runtime):
    store=SeasonStore(client,config,runtime/'ring-helpers')
    prior=store.inventory('rings-helper');fields=store.fields['rings-helper']
    existing=[{name:row['fields'][definition['id']] for name,definition in fields.items() if definition['id'] in row['fields']} for row in prior.values()]
    graph=candidates(source,config,existing)
    # Source timestamps remain in Data. Nicknames are never present in a write.
    return store.apply({'rings-helper':graph})
