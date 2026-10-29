import argparse,json,pathlib

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--evidence-root',required=True,type=pathlib.Path);parser.add_argument('graph');parser.add_argument('label');args=parser.parse_args()
    root=args.evidence_root.resolve();out=(root/args.label).resolve();source=(root/args.graph).resolve()
    if not out.is_relative_to(root) or not source.is_relative_to(root):raise ValueError('Preparation path escapes evidence root')
    if out.exists() or (root/(args.label+'-manifest.json')).exists():raise ValueError('Refusing to overwrite retained batches')
    graph=json.loads(source.read_text(encoding='utf-8'));out.mkdir();manifest=[]
    for table,rows in graph.items():
     batches=[];batch=[];size=0
     for row in rows:
      length=len(json.dumps(row))
      if batch and (len(batch)>=50 or size+length>80000):batches.append(batch);batch=[];size=0
      batch.append(row);size+=length
     if batch:batches.append(batch)
     files=[]
     for i,b in enumerate(batches):
      target=out/f'{table}-{i:03}.json';target.write_text(json.dumps(b,separators=(',',':')));files.append(str(target.resolve()))
     manifest.append({'table':table,'count':len(rows),'files':files})
    (root/(args.label+'-manifest.json')).write_text(json.dumps(manifest));print(args.label,sum(len(x['files']) for x in manifest),'batches,',sum(x['count'] for x in manifest),'records')

if __name__=='__main__':
    main()
