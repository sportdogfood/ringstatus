"""Inert stdin replay bridge. No credentials, network, collectors or record writes."""
import json, pathlib, sys
root=pathlib.Path(sys.argv[1])
sys.path.insert(0,str(root/'wef-data'))
sys.path.insert(0,str(root/'wef-schedules'/'tests'))
from day_counts import accepted_summaries
try:
    value=json.load(sys.stdin)
    result=accepted_summaries(value['head'],value['runs'],value['collections'],value['context'])
    json.dump(result,sys.stdout,ensure_ascii=False,separators=(',',':'))
except Exception as error:
    print(str(error),file=sys.stderr)
    sys.exit(1)
