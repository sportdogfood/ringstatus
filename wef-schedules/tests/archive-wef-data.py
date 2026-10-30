"""Archive an independently read-back Data snapshot without running a collector.

The destination is the existing ringstatus-data Git checkout. No Airtable rows
are changed or retired. A published archive is verified at the pushed commit.
"""
import argparse
import gzip
import hashlib
import io
import json
import pathlib
import subprocess
import tarfile

TABLE_KEYS = dict(zip(
    ['runs','shows','days','rings','groups','classes','schedules','horses','riders',
     'trainers','owners','entries','trips','lanes','work-items','slots',
     'collections','requests','results'],
    ['run-key','show-key','day-key','ring-key','group-key','class-key','schedule-key',
     'horse-key','rider-key','trainer-key','owner-key','entry-key','trip-key',
     'lane-key','work-key','slot-key','collection-key','request-key','result-key']))


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':')).encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True,timeout=60).strip()


def build(evidence):
    graph = json.loads((evidence/'latest-graph.json').read_text(encoding='utf-8'))
    mapping = json.loads((evidence/'record-map-final.json').read_text(encoding='utf-8'))
    proof = json.loads((evidence/'final-readback.json').read_text(encoding='utf-8'))
    if proof.get('status') != 'PASS' or proof.get('base') != 'appdl16HJ5ETm8N69':
        raise ValueError('Missing successful Data readback')
    if set(graph) != set(TABLE_KEYS) or set(mapping) != set(graph):
        raise ValueError('Incomplete Data table inventory')
    if len(graph['shows']) != 1:
        raise ValueError('An archive must contain exactly one scoped show')
    show_key = graph['shows'][0]['show-key']
    customer, show = show_key.split('|')
    if not customer.isdecimal() or not show.isdecimal():
        raise ValueError('Invalid canonical show key')
    files = {}
    counts = {}
    for table, key in TABLE_KEYS.items():
        rows = graph[table]
        keys = [str(row[key]) for row in rows]
        receipt = proof['tables'][table]
        if len(set(keys)) != len(keys) or set(keys) != set(mapping[table]):
            raise ValueError('Duplicate or unmapped keys: '+table)
        if not (receipt['status'] == 'PASS' and not receipt['errors'] and
                receipt['expected'] == receipt['read'] == len(rows)):
            raise ValueError('Readback count mismatch: '+table)
        for row in rows:
            for value in row.values():
                if isinstance(value, dict) and '$links' in value:
                    for target, target_key in value['$links']:
                        if target_key not in mapping.get(target, {}):
                            raise ValueError('Unresolved native link')
        files['tables/'+table+'.json.gz'] = gzip.compress(encoded([
            {'id': mapping[table][str(row[key])], 'fields': row}
            for row in rows]), mtime=0)
        counts[table] = len(rows)
    if sum(counts.values()) != proof['records']:
        raise ValueError('Total readback mismatch')
    # Preserve exact original captures and receipts, not just mutable projections.
    names = ['schema-final.json','record-map-final.json','final-readback.json',
             'control-readback.json','baseline-readback.json','README.md',
             'baseline/results.json','adaptive/trial.json',
             'adaptive/trial-initial-segment.json']
    names += [p.relative_to(evidence).as_posix()
              for p in sorted((evidence/'baseline/source').rglob('*.json'))]
    names += [p.relative_to(evidence).as_posix()
              for p in sorted((evidence/'adaptive').glob('*.json'))]
    generic=(evidence/'evidence-manifest.json').exists()
    context=json.loads((evidence/'archive-context.json').read_text(encoding='utf-8')) if generic else {}
    if generic:names=json.loads((evidence/'evidence-manifest.json').read_text(encoding='utf-8'))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w') as archive:
        for name in sorted(set(names)):
            if not (evidence/name).resolve().is_relative_to(evidence.resolve()):
                raise ValueError('Evidence path escapes scope')
            body = (evidence/name).read_bytes()
            info = tarfile.TarInfo(name)
            info.size = len(body)
            info.mtime = 0
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(body))
    files['source-and-evidence.tar.gz'] = gzip.compress(raw.getvalue(), mtime=0)
    operational={}
    if (evidence/'operational-tables.json').exists():
        operational=json.loads((evidence/'operational-tables.json').read_text(encoding='utf-8'))
        if set(operational)!={'heartbeat','run-logs','err-logs','meta'}:raise ValueError('Incomplete operational archive inventory')
        for table,rows in operational.items():
            if len({r['id'] for r in rows})!=len(rows) or any(not isinstance(r.get('fields'),dict) for r in rows):raise ValueError('Invalid operational record inventory')
            files['tables/'+table+'.json.gz']=gzip.compress(encoded(rows),mtime=0)
    manifest = {
        'format': 'wef-data-retained-show-v1', 'show_key': show_key,
        'base_id': proof['base'], 'verified_at': proof['checkedAt'],
        'scope': context.get('scope','Saved October 24 trial snapshot; not a fresh source collection'),
        'coverage': context.get('coverage','Scheduled class graph, not proof of all-entry directory coverage'),
        'record_count': proof['records'], 'counts': counts,
        'links': 'Exact Airtable IDs retained; symbolic links resolve through record-map-final.json',
        'limitations': (['Retained projection at verified cutoff, not a fresh complete source collection',
                         'Interrupted units are evidence, not completed Data revisions; no records deleted'] if generic else
                        ['Current rows are a final trial projection, not immutable per-observation revisions',
                         'No natural drop, sustained runner, or automatic rollover acceptance implied']),
        'files': {name: {'sha256': sha(body), 'bytes': len(body)}
                  for name, body in sorted(files.items())}}
    if operational:
        manifest.update({'operational_counts':{t:len(rows) for t,rows in operational.items()},'all_table_count':len(counts)+len(operational),
                         'operational_record_count':sum(map(len,operational.values())),'accounting':context.get('accounting',{})})
    revision = sha(encoded(manifest))
    files['manifest.json'] = encoded(manifest)
    return pathlib.Path('wef-data/shows')/customer/show/revision, files


def verify(repo, prefix, files, ref=None):
    for name, expected in files.items():
        actual = (subprocess.check_output(['git','-C',str(repo),'show',
                  ref+':'+(prefix/name).as_posix()]) if ref else
                  (repo/prefix/name).read_bytes())
        if actual != expected:
            raise ValueError('Archive readback mismatch: '+name)


def archive(evidence, repo, publish=False, pending_bundles=None):
    if publish:
        # Data owns this helper. Reuse the proven guarded/history-preserving
        # publisher; do not introduce another merge/retry implementation.
        import sys
        data_root=str(pathlib.Path(__file__).resolve().parents[2]/'wef-data')
        if data_root not in sys.path:sys.path.insert(0,data_root)
        from archive_evidence import publish_bundle,ArchiveDeferred
        prefix,files=build(evidence)
        try:
            return publish_bundle(repo,prefix.as_posix(),{(prefix/name).as_posix():body for name,body in files.items()},**({'pending_bundles':pending_bundles} if pending_bundles else {}))
        except ArchiveDeferred as error:
            cause=error.__cause__
            if isinstance(cause,subprocess.CalledProcessError) and cause.returncode==1 and 'merge-tree' in cause.cmd:
                raise ValueError('Conflicting outgoing archive history; immutable evidence retained') from error
            raise
    remote = git(repo, 'remote', 'get-url', 'origin').removesuffix('.git')
    if remote not in ('https://github.com/sportdogfood/ringstatus-data',
                      'git@github.com:sportdogfood/ringstatus-data'):
        raise ValueError('Wrong archive repository')
    if git(repo, 'status', '--porcelain'):
        raise ValueError('Archive checkout has unrelated changes')
    prefix, files = build(evidence)
    for name, body in files.items():
        dest = repo/prefix/name
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and dest.read_bytes() != body:
            raise ValueError('Immutable archive collision')
        dest.write_bytes(body)
    verify(repo, prefix, files)
    return {'path': prefix.as_posix(), 'files': len(files), 'published': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=pathlib.Path, required=True)
    parser.add_argument('--repository', type=pathlib.Path, required=True)
    parser.add_argument('--publish', action='store_true')
    args = parser.parse_args()
    print(json.dumps(archive(args.evidence.resolve(), args.repository.resolve(), args.publish)))
