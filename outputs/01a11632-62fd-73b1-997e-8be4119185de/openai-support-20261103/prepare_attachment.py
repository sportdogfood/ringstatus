import json,re
from pathlib import Path
from collections import Counter
root=Path.cwd(); source=root/'.git/ringstatus-control/complaint-review-20261103'
out=root/'outputs/01a11632-62fd-73b1-997e-8be4119185de/openai-support-20261103'
read=lambda name:json.loads((source/name).read_text(encoding='utf-8'))
rows=read('classification.json'); records={r['id']:r for r in read('readback.json')['records']}
latest=read('owner-example-C030-readback.json')['after']['records'][0];records[latest['id']]=latest
research=read('research-methods.json')
def clean(value,limit=550):
 s=re.sub(r'\s+',' ',str(value or '')).strip()
 s=re.sub(r'[A-Za-z]:[\\/][^\s;]+','[local evidence path]',s)
 s=re.sub(r'\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b','[email redacted]',s,flags=re.I)
 s=re.sub(r'\b(?:sk-|pat)[A-Za-z0-9_-]{12,}\b','[credential redacted]',s)
 s=re.sub(r'https?://\S+','[linked evidence]',s)
 s=re.sub(r'(?<!\w)(?:\+?1[-. ]?)?\(?\d{3}\)?[-. ]\d{3}[-. ]\d{4}(?!\w)','[phone redacted]',s)
 if len(s)>limit:s=s[:limit].rsplit(' ',1)[0]+' [excerpt]'
 return s
entries=[]
for c in rows:
 r=records[c['id']];v=r['cellValuesByFieldId']; g=research['groups'][c['group']]
 name=str(v.get('fld26uoOiURXkOPy3',''));complaint=v.get('fldTfgHVQZfCSywak','')
 excerpt=clean((name+': ' if name and not re.fullmatch(r'C\d+',name) else '')+complaint,600)
 method=research['new_case_checks'].get(str(c['index']),g[1])
 if c['id']=='recNiiTVWAqeIJTvn':method='Owner supplied a fresh example of retrospective promises. Require evidence of the exact approved delta before edits, actual diff and required independent review before deployment, and the existing authorized workflow result before stopping. These checks must occur during the work.'
 entries.append({'id':c['id'],'ref':name if re.fullmatch(r'C\d+',name) else c['id'],'created':r['createdTime'],'new':c['new_since_20261102'],'group':g[0],'excerpt':excerpt,'remediation':v.get('fldU2n8LYGo3yT6KZ',{}).get('name','Unknown'),'method':clean(method,480),'url':c['url']})
assert len(entries)==209 and len({x['id'] for x in entries})==209
assert sum(x['new'] for x in entries)==25
groups=[{'key':k,'label':v[0],'method':clean(v[1],400),'count':sum(x['group']==v[0] for x in entries),'new':sum(x['group']==v[0] and x['new'] for x in entries)} for k,v in research['groups'].items()]
groups.sort(key=lambda x:(-x['count'],x['label']))
for g in groups:g['prior']=g['count']-g['new'];assert g['prior']>0
data={'asOf':'2026-11-03','baseline':'2026-11-02','records':entries,'groups':groups,'recordCount':209,'baselineCount':184,'newCount':25,'method':'Snapshot reviewed 3 November 2026. Added means absent from the 2 November 184-record baseline, not necessarily that the underlying incident first occurred in that interval. Each distinct record has one primary family. Shared families show recurring patterns, not duplicate incidents or a proven common root cause. Complaint excerpts are shortened and contact/local-path details omitted. Correction states are recorded states, not fresh operational certification.'}
(out/'attachment-data.json').write_text(json.dumps(data,indent=2,ensure_ascii=False),encoding='utf-8')
print(json.dumps({'records':len(entries),'new':sum(x['new'] for x in entries),'groups':len(groups),'newInExistingFamilies':25,'counts':[{k:g[k] for k in ['label','count','prior','new']} for g in groups]}))
