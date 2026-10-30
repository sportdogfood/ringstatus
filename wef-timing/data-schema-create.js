"use strict";
// Explicit schema setup only; never part of a recurring runner.
const fs=require('node:fs'),path=require('node:path'),{spawnSync}=require('node:child_process');
const t=name=>({name,type:'singleLineText'}),j=name=>({name,type:'multilineText'}),n=(name,precision=0)=>({name,type:'number',options:{precision}});
const definitions=[{name:'timing-answers',fields:[t('answer-key'),t('scope-key'),t('answer-kind'),t('source-lane'),n('show-id'),t('date'),n('ring-number'),n('group-id'),t('phase'),t('availability'),t('pace-method'),n('pace-minutes-per-trip',4),t('evaluated-at'),t('observed-at'),j('reason-codes-json'),j('input-refs-json'),j('payload-json'),t('content-hash')]},
{name:'engine-checkpoints',fields:[t('checkpoint-key'),t('run-id'),t('started-at'),t('completed-at'),t('last-outcome'),n('duration-ms'),j('counts-json'),j('state-json'),j('error-summary'),t('policy-version')]}];
if(require.main===module){
  if(process.argv[2]!=='--apply')throw Error('Explicit --apply required');
  const cli=path.join(process.env.APPDATA,'npm/node_modules/@airtable/mcp-cli/dist/bin.js');
  for(const d of definitions){
    const result=spawnSync(process.execPath,[cli,'create-table','--input','-','--output','raw'],{input:JSON.stringify({baseId:'appYOykKIy1kAquhP',...d}),encoding:'utf8',windowsHide:true,env:{...process.env,AIRTABLE_TOKEN:process.env.WEF_TIMING_AIRTABLE_TOKEN},timeout:30000});
    fs.writeFileSync(path.join(__dirname,'runtime','schema-'+d.name+'.json'),result.stdout||result.stderr||'Unknown result');
    if(result.status!==0)throw Error('Airtable CLI schema setup failed; inspect outcome before retry');
    console.log(d.name+': submitted; schema readback required');
  }
}
module.exports={definitions};
