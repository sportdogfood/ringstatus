"""Scheduled one-day entry; the same implementation for every day."""
import argparse,json,pathlib
from fast_workflow import collect,save_captures
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--day',type=int);parser.add_argument('--saving',action='store_true');args=parser.parse_args()
    try:result=save_captures(pathlib.Path(__file__).resolve().parent) if args.saving else collect(pathlib.Path(__file__).resolve().parent,args.day)
    except Exception as e:result={'ok':False,'counts':{},'item-errors':[{'key':'Collection','error':str(e)}]}
    print(json.dumps(result))
