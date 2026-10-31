"use strict";
const text=name=>({name,type:'singleLineText'}),long=name=>({name,type:'multilineText'}),num=name=>({name,type:'number',options:{precision:0}}),date=name=>({name,type:'date',options:{dateFormat:{name:'iso'}}}),flag=name=>({name,type:'checkbox',options:{icon:'check',color:'greenBright'}});
const tables={
 'run-logs':[...['run-id','started','completed','result'].map(text),num('show-id'),...['class-count','ready-count','changed-count'].map(num),long('summary'),long('errors')],
 heartbeat:[...['heartbeat-key','run-id','started','completed','checked-at','last-success','result'].map(text),num('show-id'),long('summary'),long('errors')],
 'errs-log':[...['error-key','run-id','occurred-at'].map(text),num('show-id'),text('date'),num('err-class'),long('message')],
 'code-reviews':[text('routine'),date('last-review'),text('last-result'),date('next-review'),text('cadence'),long('notes')],
 snapshot:[text('snapshot-key'),...['customer-id','show-id'].map(num),text('date'),num('ring-number'),num('class-id'),...['class-number','class-name','class-status','check-status'].map(text),long('check-reason'),...['checked-at','checked-run-id','source-observed-at','accepted-at','finality','content-hash','previous-hash'].map(text),...['revision-number','result-count'].map(num),flag('results-verified'),flag('show-results'),...['results-json','phase-json','source-evidence-json','check-evidence-json'].map(long)]
};
module.exports={tables};
