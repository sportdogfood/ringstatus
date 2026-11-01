"""Entry point used by the existing scheduled task."""
import json,pathlib
from linking import run
from retirement import worker_lock
ROOT=pathlib.Path(__file__).resolve().parent

def main():
    try:
        with worker_lock(ROOT/'runtime'/'linking'):
            result=run(ROOT/'schema.json')
    except Exception as error:
        result={'ok':False,'state':'Partial','counts':{'saved':0},'item-errors':[{'key':'Linking','error':str(error)}]}
    print(json.dumps(result));return 0

if __name__=='__main__':raise SystemExit(main())
