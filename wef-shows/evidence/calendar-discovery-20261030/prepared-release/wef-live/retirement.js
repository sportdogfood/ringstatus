"use strict";
const {canRetire}=require('./rollover');
const {localDay}=require('./model');
const order=['live-entries','live-classes','live-groups','live-rings','collections'];
async function retireOld({api,scope,archive,report,now}) {
  // An upcoming selected date must not make today's records eligible.
  const selectedDate=scope.selected_date||scope.date;
  const today=localDay(now), cutoff={...scope,date:selectedDate < today ? selectedDate : today};
  const plans=[];
  for(const table of order){
    const rows=(await api.list(table)).filter(r=>canRetire(r.fields,cutoff) && (selectedDate<=today || r.fields.date<today) &&
      (table!=='collections'||['rings','groups','class'].includes(r.fields['source-kind'])));
    const groups=new Map();
    for(const r of rows){
      const show=Number(r.fields['show-id']);
      if(!Number.isSafeInteger(show)||show<=0)throw Error('Invalid archived show identity');
      const key=show+'|'+r.fields.date;
      if(!groups.has(key))groups.set(key,[]);groups.get(key).push(r);
    }
    for(const rows of groups.values()){
      const f=rows[0].fields;
      const receipt=await archive('retired-'+table,rows,{shows:[Number(f['show-id'])],dates:[f.date]});
      report.archives.push(receipt);
      const catalog={'collection-id':['archive',f['show-id'],f.date,table,receipt.sha256].join('|'),
        'show-id':Number(f['show-id']),date:f.date,'source-kind':'archive-catalog','source-id':table,
        started:now,completed:now,status:'Verified','row-count':rows.length,fingerprint:receipt.sha256,
        'archive-file':receipt.file,message:JSON.stringify({version:1,run:report.run_id,table,recordIds:rows.map(r=>r.id),cleanup:'pending'}),errors:''};
      plans.push({table,rows,receipt,catalog});
    }
  }
  // Verify every archive catalog before the first delete, including parent links.
  for(const p of plans)await api.upsert('collections','collection-id',[p.catalog]);
  const saved=await api.list('collections');
  for(const p of plans){
    const matches=saved.filter(r=>r.fields['collection-id']===p.catalog['collection-id']);
    if(matches.length!==1||Object.entries(p.catalog).some(([k,v])=>v!=='' && matches[0].fields[k]!==v))throw Error('Archive catalog readback mismatch');
  }
  for(const p of plans){
    try{
      await api.retire(p.table,p.rows,cutoff,p.receipt);
    }catch(error){
      p.receipt.cleanup='Uncertain';
      try{await api.upsert('collections','collection-id',[{...p.catalog,status:'Uncertain',errors:error.message}]);}catch{}
      throw error; // No alternate delete or immediate retry.
    }
    p.receipt.cleanup='Retired';
    await api.upsert('collections','collection-id',[{...p.catalog,status:'Retired',message:JSON.stringify({version:1,run:report.run_id,table:p.table,recordIds:p.rows.map(r=>r.id),cleanup:'complete'})}]);
  }
  report.retired=plans.map(p=>({table:p.table,show:p.catalog['show-id'],date:p.catalog.date,records:p.rows.length,catalog:p.catalog['collection-id']}));
}
module.exports={retireOld};
