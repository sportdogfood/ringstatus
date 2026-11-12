"""Scheduled one-day entry; the same implementation for every day."""
import argparse,json,pathlib
from fast_workflow import collect
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--day',type=int,required=True);args=parser.parse_args()
    try:result=collect(pathlib.Path(__file__).resolve().parent,args.day)
    except Exception as e:result={'ok':False,'counts':{},'item-errors':[{'key':'Collection','error':str(e)}]}
    print(json.dumps(result))
