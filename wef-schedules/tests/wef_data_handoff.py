"""Read-only verification and exact membership projection of a Data archive.

Does not write Season or Data, manufacture keys, or infer live freshness.
"""
import gzip
import hashlib
import io
import json
import pathlib
import tarfile

TABLE_KEYS=dict(zip(['runs','shows','days','rings','groups','classes','schedules','horses','riders','trainers','owners','entries','trips','lanes','work-items','slots','collections','requests','results'],['run-key','show-key','day-key','ring-key','group-key','class-key','schedule-key','horse-key','rider-key','trainer-key','owner-key','entry-key','trip-key','lane-key','work-key','slot-key','collection-key','request-key','result-key']))


def verify_snapshot(folder):
    folder = pathlib.Path(folder)
    manifest = json.loads((folder/'manifest.json').read_bytes())
    if manifest.get('format') != 'wef-data-retained-show-v1':
        raise ValueError('Unsupported archive format')
    packed = json.dumps(manifest, sort_keys=True, ensure_ascii=False,
                        separators=(',', ':')).encode()
    if hashlib.sha256(packed).hexdigest() != folder.name:
        raise ValueError('Manifest revision mismatch')
    bodies = {}
    for name, proof in manifest['files'].items():
        path = folder/name
        if not path.resolve().is_relative_to(folder.resolve()):
            raise ValueError('Invalid payload path')
        body = path.read_bytes()
        if len(body) != proof['bytes'] or hashlib.sha256(body).hexdigest() != proof['sha256']:
            raise ValueError('Payload hash/length mismatch: '+name)
        bodies[name] = body
    with tarfile.open(fileobj=io.BytesIO(bodies['source-and-evidence.tar.gz'])) as archive:
        # Read exact members; never extract untrusted archive paths.
        mapping = json.load(archive.extractfile('record-map-final.json'))
        readback = json.load(archive.extractfile('final-readback.json'))
    if set(manifest['counts'])!=set(TABLE_KEYS) or set(mapping)!=set(TABLE_KEYS):raise ValueError('Incomplete nineteen-table contract')
    tables = {}
    for table, expected in manifest['counts'].items():
        rows = json.loads(gzip.decompress(bodies['tables/'+table+'.json.gz']))
        fields = [r['fields'] for r in rows]
        keys = [TABLE_KEYS[table] for r in fields]
        bykey = {str(r[k]): r for r,k in zip(fields, keys)}
        if len(rows) != expected or len(bykey) != expected or set(bykey) != set(mapping[table]):
            raise ValueError('Table key/count mismatch: '+table)
        if len({r['id'] for r in rows}) != expected:
            raise ValueError('Duplicate Airtable record ID')
        for record,k in zip(rows,keys):
            if mapping[table][str(record['fields'][k])] != record['id']:
                raise ValueError('Record map mismatch')
        proof = readback['tables'][table]
        if proof['status'] != 'PASS' or proof['errors'] or proof['expected'] != expected or proof['read'] != expected:
            raise ValueError('Missing completed readback')
        tables[table] = bykey
    if sum(map(len,tables.values())) != manifest['record_count']:
        raise ValueError('Archive total mismatch')
    for records in tables.values():
        for row in records.values():
            for value in row.values():
                if isinstance(value,dict) and '$links' in value:
                    for table,key in value['$links']:
                        if key not in tables.get(table,{}):
                            raise ValueError('Unresolved link')
    if set(tables['shows']) != {manifest['show_key']}:
        raise ValueError('Show scope mismatch')
    return manifest, tables


def show_memberships(tables):
    """Directory-only entities have show membership; no day attendance is inferred."""
    output={t:{} for t in ('rings','horses','riders','trainers')}
    for table,field in [('horses','horse'),('riders','riders'),('trainers','trainers')]:
        for entry in tables['entries'].values():
            show=entry['show']['$links']
            if len(show)!=1 or show[0][0]!='shows' or show[0][1] not in tables['shows']:raise ValueError('Entry show context missing')
            for target,key in entry.get(field,{'$links':[]})['$links']:
                if target!=table or key not in tables[table]:raise ValueError('Entry entity link missing')
                output[table].setdefault(key,set()).add(show[0][1])
    for key,ring in tables['rings'].items():
        show=ring['show']['$links']
        if len(show)!=1 or show[0][0]!='shows' or show[0][1] not in tables['shows']:raise ValueError('Ring show context missing')
        output['rings'][key]={show[0][1]}
    return {t:{k:sorted(v) for k,v in rows.items()} for t,rows in output.items()}


def completed_revision(run,collections):
    """Return exact frozen units for a completed run; never use later mutable rows."""
    if run.get('status')!='Verified completed revision':raise ValueError('Run is not completed')
    state=json.loads(run['state-json'])
    if state.get('phase')!='Complete' or state.get('pending'):raise ValueError('Run journal incomplete')
    # The contract helper is deliberately inert and independent of collector IO.
    import sys
    sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'wef-data'))
    from data_contract import thaw,digest,validate_reference
    units=[];parts=[]
    for reference in state['observations']:
        row=collections[reference['key']]
        validate_reference(reference,row,completed=True)
        part=json.loads(row['retained-json'])
        if part['part']==0 and parts:units.append(thaw(parts));parts=[]
        parts.append(row)
    if parts:units.append(thaw(parts))
    if any(unit['show-key']!=state['show-key'] for unit in units):raise ValueError('Mixed show retained units')
    for unit in units:
        proof=unit.get('verification')
        if not proof or not isinstance(proof.get('record-map'),dict):raise ValueError('Missing retained ID/readback receipt')
        if not proof['record-map'] and any(unit['graph'].values()):raise ValueError('Missing nonempty retained ID/readback receipt')
        for table,rows in unit['graph'].items():
            for row in rows:
                if str(row[TABLE_KEYS[table]]) not in proof['record-map'].get(table,{}):raise ValueError('Unmapped retained fact')
    return state,units


def revision_chain(head,runs,collections):
    """Resolve pinned predecessors back to the immutable retained bootstrap."""
    output=[];seen=set();key=head;expected=None;show=None
    while key:
        if key in seen:raise ValueError('Revision chain cycle')
        seen.add(key);run=runs[key]
        if expected and hashlib.sha256(run['state-json'].encode('utf-8')).hexdigest()!=expected:raise ValueError('Predecessor journal changed')
        state,units=completed_revision(run,collections)
        if show is not None and state['show-key']!=show:raise ValueError('Cross-show predecessor')
        show=state['show-key'];output.append((state,units))
        previous=state.get('predecessor');key=previous['run-key'] if previous else None;expected=previous['state-sha256'] if previous else None
    output.reverse()
    if not output or not any(u['scope']=='retained-bootstrap' for s,units in output for u in units):raise ValueError('Immutable bootstrap is missing')
    return output


def assignment_evidence(tables):
    """Pure assessment of retained trip identities; parent lists are not assignments."""
    from collections import Counter
    def one(row,field,table,optional=False):
        refs=row.get(field,{}).get('$links',[])
        if optional and not refs:return None
        if len(refs)!=1 or refs[0][0]!=table or refs[0][1] not in tables.get(table,{}):
            raise ValueError('Invalid assignment context: '+field)
        key=refs[0][1]
        if str(tables[table][key].get(TABLE_KEYS[table]))!=key:raise ValueError('Assignment entity key mismatch')
        return key
    def identity(value):
        if isinstance(value,bool) or not (type(value) is int or isinstance(value,str) and value.isdecimal()) or int(value)<=0:
            raise ValueError('Invalid assignment source identity')
        return int(value)
    def scope(row):
        show=one(row,'show','shows');day=one(row,'day','days')
        if one(tables['days'][day],'show','shows')!=show:raise ValueError('Day/show conflict')
        import datetime
        date=str(tables['days'][day].get('date',''))[:10]
        try:datetime.date.fromisoformat(date)
        except ValueError as error:raise ValueError('Invalid assignment day') from error
        context=tables['shows'][show]
        if context.get('start-date') and not context['start-date'][:10]<=date<=context['end-date'][:10]:raise ValueError('Assignment day outside show')
        return show,day
    trips={};active={};incomplete=set()
    for key,trip in tables.get('trips',{}).items():
        if trip.get('trip-key')!=key:raise ValueError('Trip key mismatch')
        ck=one(trip,'class','classes');ek=one(trip,'entry','entries');cls=tables['classes'][ck];entry=tables['entries'][ek]
        show,day=scope(cls);ring=one(cls,'ring','rings')
        if scope(tables['rings'][ring])!=(show,day):raise ValueError('Class/ring context conflict')
        if one(entry,'show','shows')!=show:raise ValueError('Entry/class show conflict')
        try:raw=json.loads(trip['source-json'])
        except (KeyError,TypeError,ValueError) as error:raise ValueError('Missing trip source evidence') from error
        if not isinstance(raw,dict) or identity(raw.get('entry_id'))!=identity(entry.get('entry-id')):raise ValueError('Trip/entry source identity conflict')
        if raw.get('class_id') is not None and identity(raw['class_id'])!=identity(cls.get('class-id')):raise ValueError('Trip/class source identity conflict')
        for field,source in [('trip-id','trip_id'),('source-uuid','entryxclasses_uuid')]:
            if trip.get(field) not in (None,'') and str(trip[field])!=str(raw.get(source)):raise ValueError('Trip source occurrence conflict')
        uuid=raw.get('entryxclasses_uuid')
        if uuid:
            if not isinstance(uuid,str) or not uuid.strip() or uuid!=uuid.strip():raise ValueError('Invalid trip source UUID')
        else:identity(raw.get('trip_id'))
        roles={}
        for table,field in [('horses','horse'),('riders','rider'),('trainers','trainer')]:
            linked=one(trip,field,table,optional=table=='trainers');source=raw.get(field+'_id')
            if linked is None:
                if source not in (None,'',0,'0'):raise ValueError('Missing source-backed trainer link')
            else:
                if identity(source)!=identity(tables[table][linked].get(field+'-id')):raise ValueError('Trip '+field+' source identity conflict')
            roles[field]=linked
        if one(entry,'horse','horses')!=roles['horse']:raise ValueError('Trip/entry horse conflict')
        complete=roles['trainer'] is not None;historical=trip.get('record-status')=='dropped'
        trips[key]={'entry':ek,'class':ck,'show':show,'day':day,**roles,'complete':complete,'historical':historical,'source-as-of':trip.get('last-success')}
        if not historical:
            if complete:active.setdefault(ek,set()).add(tuple(roles[f] for f in ('horse','rider','trainer')))
            else:incomplete.add(ek)
    withheld={}
    for key,entry in tables.get('entries',{}).items():
        tuples=active.get(key,set());reasons=[]
        parent={f:{k for t,k in entry.get(f,{}).get('$links',[])} for f in ('riders','trainers')}
        if len(tuples)>1:reasons.append('multiple-trip-assignments')
        if any(len(values)>1 for values in parent.values()):reasons.append('multiple-parent-associations')
        if not tuples:reasons.append('no-complete-trip-assignment')
        if key in incomplete:reasons.append('incomplete-trip-assignment')
        if len(tuples)==1:
            value=next(iter(tuples))
            if any(parent[f] and parent[f]!={value[index]} for f,index in [('riders',1),('trainers',2)]):reasons.append('unreconciled-parent-associations')
        if reasons:withheld[key]=reasons
    reasons=Counter(reason for values in withheld.values() for reason in values)
    return {'trips':trips,'withheld-entries':withheld,'summary':{
        'complete-trips':sum(t['complete'] for t in trips.values()),'incomplete-trips':sum(not t['complete'] for t in trips.values()),
        'historical-trips':sum(t['historical'] for t in trips.values()),'withheld-entries':len(withheld),'withholding-reasons':dict(sorted(reasons.items()))}}


def memberships(tables,assessment=None):
    """Dated source history, independent of ambiguous registration-role arrays."""
    assessment=assignment_evidence(tables) if assessment is None else assessment
    output={t:{} for t in ('rings','horses','riders','trainers')}
    def add(table,key,show,day):output[table].setdefault(key,set()).add((show,day))
    for key,ring in tables.get('rings',{}).items():
        def one(field,table):
            refs=ring[field]['$links']
            if len(refs)!=1 or refs[0][0]!=table or refs[0][1] not in tables.get(table,{}):raise ValueError('Invalid ring '+field)
            return refs[0][1]
        show=one('show','shows');day=one('day','days')
        if tables['days'][day]['show']['$links']!=[['shows',show]]:raise ValueError('Ring day/show conflict')
        add('rings',key,show,day)
    for trip in assessment['trips'].values():
        for table,field in [('horses','horse'),('riders','rider'),('trainers','trainer')]:
            if trip[field] is not None:add(table,trip[field],trip['show'],trip['day'])
    return {t:{k:sorted(v) for k,v in rows.items()} for t,rows in output.items()}
