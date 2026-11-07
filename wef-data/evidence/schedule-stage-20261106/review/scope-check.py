import ast,json,pathlib
root=pathlib.Path(__file__).resolve().parents[1]
baseline=root/'baseline'; candidate=root/'candidate'
def definition(path,name):
    module=ast.parse(path.read_text(encoding='utf-8'))
    return next(node for node in module.body if isinstance(node,(ast.FunctionDef,ast.ClassDef)) and node.name==name)
for name in ('run','Source','schedule_payload'):
    assert ast.dump(definition(baseline/'fast_collector.py',name))==ast.dump(definition(candidate/'fast_collector.py',name)),name+' changed'
old=json.loads((baseline/'schema.json').read_text(encoding='utf-8'))
new=json.loads((candidate/'schema.json').read_text(encoding='utf-8'))
added=new['tables']['schedules']['fields'][len(old['tables']['schedules']['fields']):]
assert {f['name'] for f in added}=={'total-trips','rs-lever','rs-flag','rs-priority'}
new['tables']['schedules']['fields']=new['tables']['schedules']['fields'][:len(old['tables']['schedules']['fields'])]
assert new==old,'unrelated schema change'
old_text=(baseline/'fast_collector.py').read_text(encoding='utf-8')
new_text=(candidate/'fast_collector.py').read_text(encoding='utf-8')
start='    from record_mappings import RESULT_FIELDS,trip_records'
end="    timed('fetch-and-prepare-entries-trips')"
assert old_text[old_text.index(start):old_text.index(end)]==new_text[new_text.index(start):new_text.index(end)]
print('PASS identical cadence/run, Source timeout, retry, class-detail request/preparation code; only four schedule schema mappings added')
