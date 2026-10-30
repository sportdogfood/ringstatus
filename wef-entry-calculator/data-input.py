"""Read-only extension of the existing verified Data handoff; no acquisition."""
import importlib.util,json,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('timing_data',ROOT/'wef-timing/data-input.py')
baseline=importlib.util.module_from_spec(spec);spec.loader.exec_module(baseline)

def project(root,pointer,context):
    result=baseline.project(root,pointer,context)
    root=pathlib.Path(root);key=pointer['head'];sha=pointer['run-sha256'];snapshots={}
    while key:
        run=baseline.read(root/'objects'/('run-'+sha+'.json'))
        state=json.loads(run['state-json'])
        parts={r['key']:baseline.read_part(root,r) for r in state['observations']}
        for unit,refs in baseline.retained_units(state,parts):
            if unit['scope']!='class':continue
            payloads=unit['source']['classes'];metrics=unit['metric']['requests']
            if len(payloads)!=len(metrics):raise ValueError('Entry class receipt coverage mismatch')
            for payload,metric in zip(payloads,metrics):
                related=payload['class_related_data'];day=str(related['date'])[:10]
                if day!=context['selected-date']:continue
                if str(payload['show_id'])!=str(context['show-id']):raise ValueError('Entry source show mismatch')
                identity=[context['customer-id'],context['show-id'],day,related['ring'],payload['class']['class_id']]
                scope=json.dumps(identity,separators=(',',':'));at=metric['completed']
                value={'identity':identity,'observed_at':at,'payload':payload,'refs':[baseline.source_reference(r) for r in refs]}
                previous=snapshots.get(scope)
                if previous and baseline.instant(previous['observed_at'])==baseline.instant(at) and previous['payload']!=payload:
                    raise ValueError('Ambiguous equal-time entry snapshots')
                if previous is None or baseline.instant(previous['observed_at'])<baseline.instant(at):snapshots[scope]=value
        previous=state.get('predecessor');key=previous['run-key'] if previous else None;sha=previous['state-sha256'] if previous else None
    result['entry_classes']=list(snapshots.values())
    return result

if __name__=='__main__':
    try:
        context=json.load(sys.stdin);root=ROOT/'wef-data/runtime/handoff'
        file=root/('head-'+context['show-key'].replace('|','-')+'.json');pointer=baseline.read(file)
        result=project(root,pointer,context)
        if baseline.read(file)!=pointer:raise ValueError('Data pointer changed during read')
        json.dump(result,sys.stdout,ensure_ascii=False,separators=(',',':'))
    except Exception as error:
        print(str(error),file=sys.stderr);sys.exit(1)
