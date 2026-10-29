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
    from data_contract import thaw,digest
    units=[];parts=[]
    for reference in state['observations']:
        row=collections[reference['key']]
        if reference.get('record-id')!=row.get('_record-id'):raise ValueError('Retained collection record ID changed')
        if row.get('status')!='Verified retained observation' or row['retained-sha256']!=reference['sha256'] or digest(row['retained-json'])!=reference['sha256']:
            raise ValueError('Completed collection mismatch')
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


def memberships(tables):
    output = {t:{} for t in ('rings','horses','riders','trainers')}
    def one(row,field,table):
        links = row[field]['$links']
        if len(links)!=1 or links[0][0]!=table:
            raise ValueError('Ambiguous context: '+field)
        return links[0][1]
    def add(table,key,show,day):
        if one(tables['days'][day],'show','shows') != show:
            raise ValueError('Day/show conflict')
        output[table].setdefault(key,set()).add((show,day))
    for key,ring in tables['rings'].items():
        add('rings',key,one(ring,'show','shows'),one(ring,'day','days'))
    for trip in tables['trips'].values():
        cls=tables['classes'][one(trip,'class','classes')]
        entry=tables['entries'][one(trip,'entry','entries')]
        show=one(cls,'show','shows');day=one(cls,'day','days')
        if one(entry,'show','shows') != show:
            raise ValueError('Entry/class show conflict')
        ring=tables['rings'][one(cls,'ring','rings')]
        if one(ring,'show','shows')!=show or one(ring,'day','days')!=day:
            raise ValueError('Class/ring context conflict')
        for table,field,entryfield in [('horses','horse','horse'),('riders','rider','riders'),('trainers','trainer','trainers')]:
            refs=trip[field]['$links']
            if not refs and table=='trainers':continue
            key=one(trip,field,table)
            if [table,key] not in entry[entryfield]['$links']:
                raise ValueError('Trip/entry entity conflict')
            add(table,key,show,day)
    return {t:{k:sorted(v) for k,v in rows.items()} for t,rows in output.items()}
