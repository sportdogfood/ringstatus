"""Only the registered Windows Data task invokes this business entry point."""
import json,pathlib
from data_runner import run
from retirement import worker_lock,WorkerBusy
ROOT=pathlib.Path(__file__).resolve().parent

def main():
    try:
        with worker_lock(ROOT/'runtime'):
            result=run(ROOT/'runtime',ROOT/'schema.json',ROOT/'authored-fields.json',ROOT/'runtime/archive-repository')
        print(json.dumps(result));return 0 if result['ok'] or result.get('state')=='Deferred' else 1
    except WorkerBusy as error:
        print(json.dumps({'ok':False,'state':'Deferred','source-requests':0,'error':str(error),'write-outcome':'NotSent'}));return 0
    except Exception as error:
        print(json.dumps({'ok':False,'state':'Stopped','error':type(error).__name__+': '+str(error)}));return 1

if __name__=='__main__':raise SystemExit(main())
