import json,sys,time,cProfile,pstats,pathlib
root=pathlib.Path.cwd();sys.path[:0]=[str(root/'wef-data'),str(root/'wef-schedules/tests')]
from day_counts import accepted_summaries
base=root/'wef-shows/evidence/count-replay-fix-20261030'
v=json.loads((base/'profile-input.json').read_bytes());p=cProfile.Profile();start=time.monotonic();p.enable()
r=accepted_summaries(v['head'],v['runs'],v['collections'],v['context']);p.disable()
(base/'baseline-result.json').write_text(json.dumps(r),encoding='utf-8')
with (base/'profile.txt').open('w') as f:pstats.Stats(p,stream=f).sort_stats('cumulative').print_stats(22)
print(json.dumps({'seconds':time.monotonic()-start,'days':len(r)}))
