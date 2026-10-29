"""Pure Season mapping. Source keys are copied; dated attendance is never inferred."""
from wef_data_handoff import memberships,show_memberships

def project(facts,entity):
    if entity not in ('horses','riders','trainers','rings'):raise ValueError('Unsupported Season entity')
    shown=show_memberships(facts)[entity];dated=memberships(facts)[entity]
    stem=entity[:-1];keyfield=stem+'-key'
    show_fields=['show-key','show-id','customer-id','facility-id','show-name','start-date','end-date','selected-date','last-success']
    output={'shows':{},'show-days':{},entity:{}}
    for key,row in facts.get('shows',{}).items():output['shows'][key]={f:row[f] for f in show_fields if f in row}
    for key,row in facts.get('days',{}).items():
        refs=row.get('show',{}).get('$links',[])
        if len(refs)!=1 or refs[0][0]!='shows' or refs[0][1] not in output['shows']:raise ValueError('Season day/show provenance missing')
        show=output['shows'][refs[0][1]];day=row['date'][:10]
        if not show['start-date'][:10]<=day<=show['end-date'][:10]:raise ValueError('Season day out of range')
        output['show-days'][key]={'day-key':row['day-key'],'date':day,'show':[refs[0][1]],**({'last-success':row['last-success']} if row.get('last-success') else {})}
    for key,shows in shown.items():
        source=facts[entity][key]
        if str(source[keyfield])!=key:raise ValueError('Season source identity mismatch')
        fields=[keyfield,stem+'-id',stem+'-name','last-success']+(['ring-number'] if entity=='rings' else [])
        row={f:source[f] for f in fields if f in source and source[f] is not None}
        row['shows']=shows;row['show-days']=sorted({day for show,day in dated.get(key,[])})
        if entity=='rings' and (len(shows)!=1 or len(row['show-days'])!=1):raise ValueError('Dated ring scope ambiguous')
        output[entity][key]=row
    return output
