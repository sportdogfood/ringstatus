"""Inert stdin replay bridge. No credentials, network, collectors or record writes."""
import json, pathlib, sys
root=pathlib.Path(sys.argv[1])
sys.path.insert(0,str(root/'wef-data'))
sys.path.insert(0,str(root/'wef-schedules'/'tests'))
from day_counts import accepted_summaries
import data_contract

def replay(value):
    # Both chain validation and count validation thaw the same immutable parts.
    # Cache only exact bytes within this invocation; every reference, chain and
    # count validator still executes. No accepted result or freshness is cached.
    original=data_contract.thaw
    decoded={}
    def thaw_once(records):
        key=tuple((row['retained-json'],row['retained-sha256']) for row in records)
        if key not in decoded:
            decoded[key]=original(records)
        return decoded[key]
    data_contract.thaw=thaw_once
    try:
        return accepted_summaries(value['head'],value['runs'],value['collections'],value['context'])
    finally:
        data_contract.thaw=original

try:
    value=json.load(sys.stdin)
    result=replay(value)
    json.dump(result,sys.stdout,ensure_ascii=False,separators=(',',':'))
except Exception as error:
    print(str(error),file=sys.stderr)
    sys.exit(1)
