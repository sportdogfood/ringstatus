"""Read-only Data source projection. No credentials, writes, collectors or source requests."""
import datetime,hashlib,json,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parent.parent
sys.path[:0]=[str(ROOT/'wef-data'),str(ROOT/'wef-schedules/tests')]
from retained_feed import load
from wef_data_handoff import completed_revision
from day_counts import retained_units,instant

def read(path):return json.loads(path.read_text(encoding='utf-8'))
def positive(value):return type(value) is int and value>0
def project(root,pointer,context):
    fence=[context[k] for k in ('show-key','start-date','end-date','selected-date')]
    if pointer.get('authority')!=fence or pointer.get('show-key')!=context['show-key']:raise ValueError('Data canonical authority mismatch')
    # Reuse the producer's full immutable-chain/readback validator.
    _,proof=load(root,pointer)
    key,sha=pointer['head'],pointer['run-sha256'];inventories={};visited=set()
    while key:
        if key in visited:raise ValueError('Data chain cycle')
        visited.add(key);run=read(root/'objects'/('run-'+sha+'.json'))
        if run['run-key']!=key or hashlib.sha256(run['state-json'].encode('utf-8')).hexdigest()!=sha:raise ValueError('Data journal mismatch')
        state=json.loads(run['state-json']);parts={}
        if key==pointer['head'] and (state.get('authority')!=fence or state.get('authority-before')!=fence):raise ValueError('Data completed-head authority mismatch')
        for ref in state['observations']:
            part=read(root/'objects'/('part-'+ref['record-id']+'-'+ref['sha256']+'.json'));parts[ref['key']]=part
        completed_revision(run,parts)
        for unit,unit_refs in retained_units(state,parts):
            if unit['scope']!='dated-inventory-page':continue
            source,metric=unit['source'],unit['metric'];day=source['show_date'];at=metric['completed']
            if source.get('show',{}).get('show_id')!=context['show-id'] or not context['start-date']<=day<=context['end-date']:raise ValueError('Data inventory scope mismatch')
            if day in inventories and instant(inventories[day]['observed_at'])>=instant(at):continue
            groups={};seen=set()
            for ring in source['rings']:
                rn=ring['ring_number'];rid=ring.get('ring_id')
                if not positive(rn) or not positive(rid):raise ValueError('Data ring identity missing')
                for pos,row in enumerate(ring['classes']):
                    gid=row.get('class_group_id');cid=row.get('class_id');occ=row.get('class_groupxclasses_id')
                    if not positive(gid):raise ValueError('Data group identity missing')
                    source_key=f"{context['show-key']}|{day}|{rn}|"+(str(occ) if occ is not None else f'position-{pos}')
                    if source_key in seen:raise ValueError('Data occurrence identity repeated')
                    seen.add(source_key);gkey=f"{context['show-id']}|{day}|{rn}|{gid}"
                    group=groups.setdefault(gkey,{'identity':{'customer':context['customer-id'],'show':context['show-id'],'date':day,'ring':rn,'group':gid},'rows':[],'class_keys':[]})
                    if positive(cid) and not row.get('schedule_break'):group['class_keys'].append(f"{context['show-id']}|{day}|{cid}")
                    group['rows'].append({'stage-key':source_key,'class-id':cid if positive(cid) else None,
                        'start-time':row.get('estimated_start_time') or row.get('start_time_default'),'end-time':row.get('estimated_end_time'),
                        'total-trips':row.get('total_trips'),'type':'break' if row.get('schedule_break') else row.get('class_type') or 'class'})
            refs=[{'base':'appdl16HJ5ETm8N69','table':'collections','key':ref['key'],'record':ref['record-id'],'sha256':ref['sha256']} for ref in unit_refs]
            for group in groups.values():group['class_keys']=sorted(set(group['class_keys']))
            inventories[day]={'observed_at':at,'groups':list(groups.values()),'refs':refs,'inventory_complete':True}
        prior=state.get('predecessor');key=prior['run-key'] if prior else None;sha=prior['state-sha256'] if prior else None
    return {'proof':proof,'inventories':inventories,'context':context,'source_lane':'data_live'}

if __name__=='__main__':
    try:
        context=json.load(sys.stdin);root=ROOT/'wef-data/runtime/handoff';pointer_file=root/('head-'+context['show-key'].replace('|','-')+'.json')
        if not pointer_file.exists():raise ValueError('Current-show Data handoff unavailable')
        pointer=read(pointer_file);result=project(root,pointer,context)
        if read(pointer_file)!=pointer:raise ValueError('Data pointer changed during read')
        json.dump(result,sys.stdout,ensure_ascii=False,separators=(',',':'))
    except Exception as error:
        print(str(error),file=sys.stderr);sys.exit(1)
