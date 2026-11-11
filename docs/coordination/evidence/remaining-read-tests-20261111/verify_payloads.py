import base64, hashlib, json, pathlib, re, sys

root = pathlib.Path(__file__).resolve().parent
prepared = json.loads((root / 'airtable-prepared.json').read_text(encoding='utf-8-sig'))
provenance = {r['id']: r for r in json.loads((root / 'canonical-provenance.json').read_text(encoding='utf-8-sig'))}
part_fields = ['fld96MSi0YJ7zi6P5', 'fldnZivw1DAymfmOC', 'fldkeG2smOkpc2EVy', 'fldtQY4wY1U0wNt5e']
observed = None
if len(sys.argv) > 1:
    observed = {r['id']: r['cellValuesByFieldId'] for r in json.loads(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8-sig'))}

def expected_body(p):
    path = root / p['sourceFile']
    if p['format'] == 'text':
        return {'format': 'text/html; charset=utf-8' if p['label'] == 'remain-class-detail-html' else 'text/html', 'body': path.read_bytes().decode('utf-8')}
    data = json.loads(path.read_text(encoding='utf-8-sig'))
    if p['format'] == 'excel-cells':
        binary = (root / 'remain-export.xls').read_bytes()
        assert hashlib.sha256(binary).hexdigest() == p['rawSHA256']
        assert binary[:8].hex() == 'd0cf11e0a1b11ae1'
        return {'format': 'BIFF .xls', 'representation': data['representation'], 'source_sha256': p['rawSHA256'], 'sheets': data['sheets']}
    return data

results = []
for record in prepared:
    p = provenance[record['id']]
    fields = record['fields'] if observed is None else observed[record['id']]
    if observed is not None:
        for field, value in record['fields'].items():
            assert fields.get(field) == value or (value is None and not fields.get(field)), (record['route'], field, 'saved field differs')
    attempt = json.loads(fields['fldiB2BXJsb22LASE'])
    assert attempt['url'] == p['url'], (record['route'], 'request mismatch')
    pattern = ''.join(r'[^/?]+' if s.startswith('{') else re.escape(s) for s in re.split(r'(\{[^}]+\})', record['route']))
    assert re.fullmatch(pattern, p['url'].split('?')[0]), (record['route'], 'route does not match request')
    parts = [json.loads(fields[f]) for f in part_fields[:record['partCount']]]
    merged = parts[0]
    if record['splitKey']:
        for extra in parts[1:]:
            assert set(extra) == {record['splitKey']}
            merged[record['splitKey']].extend(extra[record['splitKey']])
    assert merged == expected_body(p), (record['route'], 'body differs from captured source')
    if record['route'].endswith('/classes/{id}'):
        assert str(merged['class']['class_id']) == p['url'].split('/classes/')[1].split('?')[0]
        assert all(t.get('trip_id') is not None for t in merged['trips'])
        assert merged['class']['results_verified'] == 1
    if record['route'].endswith('/getLiveClassData'):
        cid = re.search(r'[?&]cid=(\d+)', p['url']).group(1)
        assert all(str(t['class_id']) == cid for t in merged['rows'])
    if record['route'].endswith('/shows'):
        assert len(merged['shows']) == merged['total_shows'] == 802
    if p['supplements']:
        supplemental = json.loads(fields['fldtQY4wY1U0wNt5e'])['fullSuccessfulSamples']
        assert len(supplemental) == len(p['supplements'])
        for actual, source in zip(supplemental, p['supplements']):
            assert actual['attempt']['url'] == source['url']
            if source['format'] == 'binary':
                payload = actual['payload']
                raw = base64.b64decode(payload['body'])
                assert raw == (root / source['sourceFile']).read_bytes()
                assert hashlib.sha256(raw).hexdigest() == source['rawSHA256']
            else:
                assert actual['payload'] == json.loads((root / source['sourceFile']).read_text(encoding='utf-8-sig')), (record['route'], source['label'], 'supplement differs')
    results.append({'id': record['id'], 'route': record['route'], 'parts': record['partCount'], 'source_match': True, 'saved_match': observed is not None})

assert len({r['id'] for r in prepared}) == len(prepared)
report = {'result': 'PASS', 'records': len(results), 'supplements': sum(len(p['supplements']) for p in provenance.values()), 'saved_readback_verified': observed is not None, 'details': results}
destination = root / ('saved-payload-verification.json' if observed is not None else 'source-payload-verification.json')
destination.write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps({k: v for k, v in report.items() if k != 'details'}))
