"""Read-only Airtable acceptance inspection. Never invokes a collector/source/task."""
import argparse,json,pathlib
from data_runner import Repository,Airtable,KEYS,packed,digest,stamp
from wef_data_handoff import revision_chain
ROOT=pathlib.Path(__file__).resolve().parent

def snapshot(repo):
    return {'checked-at':stamp(),'record-map':{t:{k:r['id'] for k,r in rows.items()} for t,rows in repo.inventory.items()},
        'immutable':{t:{k:digest(packed(row)) for k,row in repo.graph[t].items() if 'Verified' in row.get('status','')} for t in ('runs','collections')},
        'show75':{k:v for k,v in repo.graph['shows']['15|200000075'].items() if k!='status'}}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--baseline',type=pathlib.Path,required=True);parser.add_argument('--capture',action='store_true');args=parser.parse_args()
    repo=Repository(Airtable(),json.loads((ROOT/'schema.json').read_text(encoding='utf-8')),json.loads((ROOT/'authored-fields.json').read_text(encoding='utf-8')))
    current=snapshot(repo)
    if args.capture:
        if args.baseline.exists():raise ValueError('Refusing to replace protected baseline')
        args.baseline.write_text(packed(current),encoding='utf-8')
        print(packed({'ok':True,'mode':'Read-only host-access/baseline diagnostic; not scheduled collection proof','counts':{t:len(rows) for t,rows in current['record-map'].items()}}));return
    before=json.loads(args.baseline.read_text(encoding='utf-8'))
    for table,rows in before['record-map'].items():
        for key,identity in rows.items():
            if current['record-map'][table].get(key)!=identity:raise ValueError('Protected record missing or ID changed: '+table)
    for table,rows in before['immutable'].items():
        for key,sha in rows.items():
            if current['immutable'][table].get(key)!=sha:raise ValueError('Original verified receipt changed')
    if current['show75']!=before['show75']:raise ValueError('Protected75 source fields changed')
    if repo.graph['shows']['15|200000075'].get('status')!='Archived retained snapshot':raise ValueError('75 closure pending')
    authority=repo.client.canonical_context();show=next(r['show-key'] for r in authority['records'] if r['default'])
    candidates=[r for r in repo.graph['runs'].values() if r.get('status')=='Verified completed revision' and json.loads(r.get('state-json') or '{}').get('show-key')==show]
    if not candidates:raise ValueError('No completed canonical-show revision')
    head=max(candidates,key=lambda r:json.loads(r['state-json'])['sequence'])
    collections={k:{**row,'_record-id':repo.inventory['collections'][k]['id']} for k,row in repo.graph['collections'].items()}
    chain=revision_chain(head['run-key'],repo.graph['runs'],collections)
    state=chain[-1][0]
    print(packed({'ok':True,'show-key':show,'head':head['run-key'],'protected-records':sum(map(len,before['record-map'].values())),
        'original-receipts-unchanged':True,'show75':'Archived retained snapshot','verified-revisions':len(chain),
        'directory-complete':state.get('directory',{}).get('complete',False),'capacity':state.get('capacity',{})}))

if __name__=='__main__':main()
