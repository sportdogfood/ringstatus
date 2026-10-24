const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const loader=fs.readFileSync(path.join(__dirname,'loader.js'),'utf8');
const dashboard=fs.readFileSync(path.join(__dirname,'dashboard.js'),'utf8');
const beforeHome=fs.readFileSync(path.join(__dirname,'baseline-before-home.js'),'utf8');
function fn(source,name){
  const re=new RegExp('(?:async )?function '+name+'\\(');const start=source.search(re);
  assert.ok(start>=0,'Missing function '+name);
  const tail=source.slice(start);const end=tail.slice(1).search(/\n(?:async )?function /);
  return end<0?tail:tail.slice(0,end+1);
}
function context(values){return vm.createContext({Promise,Date,Number,JSON,Object,Array,Map,Set,console,...values});}
const oldData={sourceFingerprint:'old',classes:[{id:'old'}],references:{}};
const freshData={sourceFingerprint:'fresh',classes:[{id:'old'},{id:'new'}],references:{}};
test('loader checks network despite an unexpired old cache; repeated load shares the result',async()=>{
  let calls=0;const c=context({state:'idle',data:null,pending:null,api:{},readCache:()=>({data:oldData}),writeSessionFingerprint:()=>{},hydrate:d=>{c.data=d;c.state='ready';return c.api},fetchSource:async()=>{calls++;c.data=freshData;c.state='ready';return c.api}});
  vm.runInContext(fn(loader,'load'),c);
  await c.load();await c.load();assert.equal(calls,1);assert.equal(c.data.sourceFingerprint,'fresh');
});
test('failed network refresh rejects instead of claiming cached data is fresh',async()=>{
  const c=context({state:'idle',data:null,pending:null,api:{},readCache:()=>({data:oldData}),hydrate:()=>({}),writeSessionFingerprint:()=>{},fetchSource:async()=>{throw Error('offline')},window:{dispatchEvent(){}},CustomEvent:class{}});
  vm.runInContext(fn(loader,'load'),c);await assert.rejects(c.load(),/offline/);assert.equal(c.state,'error');
});
test('writing fresh source invalidates filtered rows from another source',()=>{
  const values=new Map([['lp:classes-filtered',JSON.stringify({sourceFingerprint:'old',rows:[{id:'old'}]})]]);
  const c=context({CACHE_KEY:'lp-usef-classes-v1',SCHEMA_VERSION:'schema',VERSION:'version',localStorage:{getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)},window:{dispatchEvent(){}},CustomEvent:class{}});
  vm.runInContext(fn(loader,'writeCache'),c);c.writeCache(freshData);
  assert.equal(JSON.parse(values.get('lp-usef-classes-v1')).sourceFingerprint,'fresh');assert.equal(values.has('lp:classes-filtered'),false);
});
test('dashboard waits for fresh engine data rather than reading old cache first',async()=>{
  let waited=false;const engine={get ready(){return Promise.resolve().then(()=>{waited=true;engine.data=freshData})},data:oldData};
  const c=context({readCache:()=>oldData,waitForEngine:async()=>engine,window:{LPEngine:engine}});
  vm.runInContext(fn(dashboard,'readData'),c);const result=await c.readData();assert.equal(waited,true);assert.equal(result.sourceFingerprint,'fresh');
});
test('filtered payload identifies its source and retains current filter subset',()=>{
  let saved;const rows=[freshData.classes[1]];const c=context({filteredRows:()=>rows,sourceFingerprint:'fresh',sourceClassCount:2,context:'global',entityKey:'',filterState:{rider:[],horse:['selected'],year:[],height:[],discipline:[],resultGroup:[],skill:[]},FILTERED_CACHE_KEY:'lp:classes-filtered',localStorage:{setItem:(k,v)=>saved=JSON.parse(v)},window:{},document:{dispatchEvent(){}},CustomEvent:class{},root:{}});
  vm.runInContext(fn(dashboard,'publishFiltered'),c);c.publishFiltered();assert.equal(saved.sourceFingerprint,'fresh');assert.equal(saved.sourceCount,2);assert.equal(saved.rows[0].id,'new');assert.deepEqual(saved.filters.horse,['selected']);
});
test('engine refresh replaces dashboard source before rebuilding filtered rows',()=>{
  let published=0;const c=context({latestEngineData:null,dashboardReady:true,setSourceData:d=>{c.source=d},publishFiltered:()=>{published++;assert.equal(c.source.sourceFingerprint,'fresh')}});
  vm.runInContext(fn(dashboard,'handleEngineReady'),c);c.handleEngineReady({detail:{engine:{data:freshData}}});assert.equal(published,1);
});
test('Latest retains the 42 day rule and includes newly loaded newest dates',()=>{
  const c=context({});vm.runInContext(fn(dashboard,'latestRows'),c);
  const rows=['2026-07-01','2026-08-19','2026-09-22'].map(date=>({dateMs:Date.parse(date)}));assert.equal(c.latestRows(rows).length,2);
});
test('294 to 321 classes, then another refresh, keeps both stored datasets and Latest aligned',async()=>{
  const oldRows=Array.from({length:294},(_,i)=>({id:'old-'+i,dateMs:Date.parse(i<8?'2026-08-19':'2026-07-01')}));
  let networkData={sourceFingerprint:'321-classes',classes:oldRows.concat(Array.from({length:27},(_,i)=>({id:'new-'+i,dateMs:Date.parse('2026-09-22')}))),references:{}};
  const values=new Map([['lp-usef-classes-v1',JSON.stringify({data:{classes:oldRows}})],['lp:classes-filtered',JSON.stringify({sourceFingerprint:'294-classes',rows:oldRows})]]);
  const events={};const c=context({state:'idle',data:null,index:null,pending:null,api:{},CACHE_KEY:'lp-usef-classes-v1',SCHEMA_VERSION:'schema',VERSION:'version',SOURCE_PATH:'/engine-data',dashboardReady:true,latestEngineData:null,sourceRows:oldRows,sourceFingerprint:'294-classes',sourceClassCount:294,references:{},context:'global',entityKey:'',filterState:{rider:[],horse:[],year:[],height:[],discipline:[],resultGroup:[],skill:[]},root:{},FILTERED_CACHE_KEY:'lp:classes-filtered',clean:v=>String(v||'').trim(),rowsForContext:r=>r.slice(),buildIndex:()=>({}),writeSessionFingerprint:()=>{},parseDocument:()=>networkData,DOMParser:class{parseFromString(){return{}}},fetch:async(url,options)=>{assert.equal(options.cache,'no-store');return{ok:true,text:async()=>'<fixture>'}},localStorage:{getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)},window:{dispatchEvent:e=>events[e.type]?.(e)},document:{dispatchEvent(){}},CustomEvent:class{constructor(type,options){this.type=type;this.detail=options.detail}}});
  c.filteredRows=()=>c.sourceRows;c.api={get data(){return c.data}};
  for(const name of ['writeCache','hydrate','fetchSource','load','refresh'])vm.runInContext(fn(loader,name),c);
  for(const name of ['setSourceData','publishFiltered','handleEngineReady','latestRows'])vm.runInContext(fn(dashboard,name),c);
  events['lp:engine-ready']=c.handleEngineReady;
  await c.load();
  let full=JSON.parse(values.get('lp-usef-classes-v1')),filtered=JSON.parse(values.get('lp:classes-filtered'));
  assert.equal(full.data.classes.length,321);assert.equal(filtered.rows.length,321);assert.equal(filtered.sourceFingerprint,full.sourceFingerprint);assert.equal(c.latestRows(filtered.rows).length,35);
  networkData={...networkData,sourceFingerprint:'322-classes',classes:networkData.classes.concat({id:'newest',dateMs:Date.parse('2026-09-23')})};
  await c.refresh();full=JSON.parse(values.get('lp-usef-classes-v1'));filtered=JSON.parse(values.get('lp:classes-filtered'));
  assert.equal(filtered.rows.length,322);assert.equal(filtered.sourceFingerprint,full.sourceFingerprint);assert.equal(c.latestRows(filtered.rows).length,36);
});
test('Home renders only its Latest list without requiring hidden dashboard sections',()=>{
  const c=context({isHomeRecords:true,root:{},renderFilters(){throw Error('hidden filters')},renderDashboardTitle(){throw Error('hidden header')},renderStats(){throw Error('hidden stats')},renderCharts(){throw Error('hidden charts')},renderHorseGrid(){throw Error('hidden horses')},renderClassGrid(){c.rendered=true},dom:{},setStatus(){}});
  vm.runInContext(fn(dashboard,'renderFiltered'),c);c.renderFiltered({detail:{source:c.root,rows:[]}});assert.equal(c.rendered,true);
});
test('ordinary dashboard Horses still respects its selected filters',()=>{
  const rows=[{id:1}];let horseInput;const c=context({isHomeRecords:false,sourceRows:[...rows,{id:2}],root:{},renderFilters(){},renderDashboardTitle(){},renderStats(){},renderCharts(){},renderHorseGrid:r=>horseInput=r,renderClassGrid(){},dom:{},setStatus(){}});
  vm.runInContext(fn(dashboard,'renderFiltered'),c);c.renderFiltered({detail:{source:c.root,rows}});assert.equal(horseInput.length,1);
});
test('horse aggregate calculations retain the proven implementation',()=>{assert.equal(fn(dashboard,'horseGridRows'),fn(beforeHome,'horseGridRows'))});
test('Home removes Rider from both grids without changing the other dashboards',()=>{
  for(const home of [true,false]){
    const configs=[];const c=context({isHomeRecords:home,currentDataTab:'latest',enableClassDateFilter:false,latestRows:r=>r,classGridRows:r=>r,dom:{},classGridApi:null,horseGridApi:null,classPageSize:10,window:{agGrid:{createGrid:(mount,config)=>{configs.push(config);return{}}}},cellRenderer:()=>()=>{},solidDataPillRenderer:()=>()=>{},horseResultBadgeRenderer(){},windowBadgeRenderer(){},windowRangeRenderer(){},horseGridRows:()=>[],recordWindowPercent:()=>0});
    vm.runInContext(fn(dashboard,'renderClassGrid'),c);vm.runInContext(fn(dashboard,'renderHorseGrid'),c);c.renderClassGrid([]);c.renderHorseGrid([]);
    for(const config of configs)assert.equal(!!config.columnDefs.find(x=>x.field==='rider').hide,home);
  }
});
test('Home Latest takes the newest 20 before rendering, with pagination disabled',()=>{
  const rows=Array.from({length:35},(_,id)=>({id,dateMs:id+1}));let config;
  const c=context({isHomeRecords:true,currentDataTab:'latest',enableClassDateFilter:false,latestRows:r=>r,classGridRows:r=>r,dom:{},classGridApi:null,classPageSize:10,window:{agGrid:{createGrid:(mount,options)=>{config=options;return{}}}},cellRenderer:()=>()=>{},solidDataPillRenderer:()=>()=>{}});
  vm.runInContext(fn(dashboard,'renderClassGrid'),c);c.renderClassGrid(rows);assert.equal(config.rowData.length,20);assert.equal(config.rowData[0].id,34);assert.equal(config.rowData[19].id,15);assert.equal(config.pagination,false);assert.equal(config.domLayout,'autoHeight');
});
test('other dashboards retain their complete Latest window and pagination',()=>{
  const rows=Array.from({length:35},(_,id)=>({id,dateMs:id+1}));let config;
  const c=context({isHomeRecords:false,currentDataTab:'latest',enableClassDateFilter:false,latestRows:r=>r,classGridRows:r=>r,dom:{},classGridApi:null,classPageSize:10,window:{agGrid:{createGrid:(mount,options)=>{config=options;return{}}}},cellRenderer:()=>()=>{},solidDataPillRenderer:()=>()=>{}});
  vm.runInContext(fn(dashboard,'renderClassGrid'),c);c.renderClassGrid(rows);assert.equal(config.rowData.length,35);assert.equal(config.pagination,true);
});
