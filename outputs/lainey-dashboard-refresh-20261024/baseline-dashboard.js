
(function(){
"use strict";
if(window.__LP_SHARED_DASHBOARD_V3__)return;
window.__LP_SHARED_DASHBOARD_V3__=true;
var ENGINE_VERSION="1.2.9";
var SCHEMA_VERSION="lp-usef-classes-v1.6";
var CACHE_KEY="lp-usef-classes-v1";
var FILTERED_CACHE_KEY="lp:classes-filtered";
var root=document.querySelector(".lp-dashboard[data-lp-dashboard-context]");
if(!root)return;
var pageMeta=document.querySelector(".page-meta-container");
var context=clean(pageMeta&&pageMeta.getAttribute("data-lp-dashboard-context")||root.getAttribute("data-lp-dashboard-context")||"global");
var entityKey=clean(pageMeta&&pageMeta.getAttribute("data-lp-dashboard-entity-key")||root.getAttribute("data-lp-dashboard-entity-key"));
if(!entityKey&&(context==="horse"||context==="rider")){
try{entityKey=decodeURIComponent(location.pathname.split("/").filter(Boolean).pop()||"")}catch(error){}
}
var dom={
titleBase:root.querySelector("[data-lp-dashboard-title-base]"),
titleSelection:root.querySelector("[data-lp-dashboard-title-selection]"),
status:root.querySelector("[data-lp-dashboard-status]"),
statusText:root.querySelector("[data-lp-dashboard-status-text]"),
stats:root.querySelector("[data-lp-dashboard-stats]"),
filterStatus:root.querySelector("[data-lp-dashboard-filter-status]"),
rowCount:root.querySelector("[data-lp-dashboard-row-count]"),
classGrid:root.querySelector("[data-lp-dashboard-grid]"),
horseGrid:root.querySelector("[data-lp-dashboard-horse-grid]"),
horseCount:root.querySelector("[data-lp-dashboard-horse-count]"),
toolbarHorseCount:root.querySelector("[data-lp-dashboard-toolbar-horse-count]"),
filterToggle:root.querySelector("[data-lp-dashboard-filter-toggle]"),
filterSummary:root.querySelector("[data-lp-filter-summary]"),
dataTabs:root.querySelector("[data-lp-dashboard-data-tabs]"),
filterRoot:root.querySelector("[data-lp-dashboard-filters]"),
visualGrid:root.querySelector("[data-lp-dashboard-visual-grid]"),
visualPrev:root.querySelector("[data-lp-dashboard-visual-prev]"),
visualNext:root.querySelector("[data-lp-dashboard-visual-next]"),
filters:{
rider:root.querySelector("[data-lp-filter-riders]"),
horse:root.querySelector("[data-lp-filter-horses]"),
year:root.querySelector("[data-lp-filter-years]"),
result:root.querySelector("[data-lp-filter-results]"),
height:root.querySelector("[data-lp-filter-heights]"),
discipline:root.querySelector("[data-lp-filter-disciplines]"),
resultGroup:root.querySelector("[data-lp-filter-results]"),
skill:root.querySelector("[data-lp-filter-skills]")
},
charts:{
results:root.querySelector("[data-lp-dashboard-results]"),
years:root.querySelector("[data-lp-dashboard-years]"),
heights:root.querySelector("[data-lp-dashboard-heights]"),
disciplines:root.querySelector("[data-lp-dashboard-disciplines]")
}
};
var references={};
var sourceRows=[];
var classGridApi=null;
var horseGridApi=null;
var charts=[];
var currentDataTab="horses";
var filterState={rider:[],horse:[],year:[],height:[],discipline:[],resultGroup:[],skill:[]};
var defaultFilterState={rider:[],horse:[],year:[],height:[],discipline:[],resultGroup:[],skill:[]};
var classPageSize=10;
var enableClassDateFilter=false;
var defaultDataTab="horses";
function copyFilterState(source){
return{
rider:(source.rider||[]).slice(),
horse:(source.horse||[]).slice(),
year:(source.year||[]).slice(),
height:(source.height||[]).slice(),
discipline:(source.discipline||[]).slice(),
resultGroup:(source.resultGroup||[]).slice(),
skill:(source.skill||[]).slice()
};
}
function clean(value){return String(value==null?"":value).replace(/\s+/g," ").trim()}
function html(value){return clean(value).replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;")}
function unique(values){return Array.from(new Set(values.map(clean).filter(Boolean)))}
function injectRuntimeStyles(){
if(document.getElementById("lp-shared-dashboard-runtime-style-v3"))return;
var style=document.createElement("style");
style.id="lp-shared-dashboard-runtime-style-v3";
style.textContent=".lp-dashboard-data-pill{display:inline-flex;align-items:center;justify-content:center;min-height:28px;padding:5px 10px;border:1px solid var(--lp-data-pill-border,var(--lp-data-pill-color));border-radius:999px;background:var(--lp-data-pill-color);color:var(--lp-data-pill-text,#fff);font-size:12px;font-weight:700;line-height:1;white-space:nowrap}.lp-dashboard .lp-dashboard-usef-link,.lp-dashboard .lp-dashboard-dash-link,.lp-dashboard button.lp-dashboard-video-link{box-sizing:border-box!important;display:inline-flex!important;flex:0 0 auto!important;align-self:center!important;align-items:center!important;justify-content:center!important;width:auto!important;height:21px!important;min-height:21px!important;min-width:31px!important;margin:0!important;padding:3px 7px!important;border:1px solid!important;border-radius:6px!important;font-family:inherit!important;font-size:10px!important;font-weight:700!important;line-height:1!important;letter-spacing:0!important;text-decoration:none!important;white-space:nowrap!important;appearance:none!important}.lp-dashboard .lp-dashboard-usef-link{border-color:rgba(143,184,255,.24)!important;background-color:rgba(73,118,255,.16)!important;color:rgb(143,184,255)!important}.lp-dashboard button.lp-dashboard-video-link{border-color:rgba(255,141,154,.25)!important;background-color:rgba(255,141,154,.18)!important;color:rgb(255,141,154)!important;cursor:pointer!important}.lp-dashboard .lp-dashboard-dash-link.is-horse{border-color:rgba(73,209,125,.24)!important;background-color:rgba(73,209,125,.16)!important;color:rgb(73,209,125)!important}.lp-dashboard .lp-dashboard-dash-link.is-rider{border-color:rgba(195,134,255,.22)!important;background-color:rgba(156,89,255,.18)!important;color:rgb(195,134,255)!important}";
document.head.appendChild(style);
}
function setStatus(text,isError,isLoading){
if(dom.statusText)dom.statusText.textContent=text;
if(dom.status)dom.status.classList.toggle("is-error",!!isError);
root.classList.toggle("is-loading",!!isLoading);
}
function loadLibrary(urls,test){
return new Promise(function(resolve,reject){
if(test())return resolve();
var index=0;
function attempt(){
if(index>=urls.length)return reject(new Error("Dashboard library could not load"));
var node=document.createElement("script");
node.src=urls[index++];
node.onload=function(){if(test())resolve();else{node.remove();attempt()}};
node.onerror=function(){node.remove();attempt()};
document.head.appendChild(node);
}
attempt();
});
}
function readCache(){
try{
var payload=JSON.parse(localStorage.getItem(CACHE_KEY)||"null");
if(!payload||payload.schemaVersion!==SCHEMA_VERSION||payload.engineVersion!==ENGINE_VERSION)return null;
if(!Number.isFinite(payload.savedAt)||Date.now()-payload.savedAt>12*60*60*1000)return null;
if(!clean(payload.sourceFingerprint)||!payload.data||payload.data.sourceFingerprint!==payload.sourceFingerprint)return null;
if(!Array.isArray(payload.data.classes)||!payload.data.classes.length)return null;
return payload.data;
}catch(error){return null}
}
function waitForEngine(attempt){
attempt=attempt||0;
if(window.LPEngine)return Promise.resolve(window.LPEngine);
if(attempt>=80)return Promise.resolve(null);
return new Promise(function(resolve){setTimeout(function(){resolve(waitForEngine(attempt+1))},100)});
}
async function readData(){
var cached=readCache();
if(cached)return cached;
var engine=await waitForEngine();
if(engine&&engine.ready)await engine.ready;
if(window.LPEngine&&window.LPEngine.data)return window.LPEngine.data;
return readCache();
}
function refList(type){return Array.isArray(references[type])?references[type]:[]}
function refMap(type){
var map=Object.create(null);
refList(type).forEach(function(item){var key=clean(item.key);if(key)map[key]=item});
return map;
}
function refDisplay(type,item){
if(!item)return"";
if(type==="horses"||type==="riders"||type==="competitions")return clean(item.aliasesText);
return clean(item.display);
}
function refColor(item){
var value=clean(item&&item.color);
return /^#[0-9a-f]{3,8}$/i.test(value)?value:"";
}
function refUsef(item){
var value=clean(item&&(item.usefLink||item.usefLinkText));
return /^(https?:\/\/|\/)/i.test(value)?value:"";
}
function refDash(type,item){
var key=clean(item&&item.key);
if(!key)return"";
if(type==="horses")return"/horses/"+encodeURIComponent(key);
if(type==="riders")return"/riders/"+encodeURIComponent(key);
return"";
}
function helperKeys(type){return new Set(refList(type).map(function(item){return clean(item.key)}).filter(Boolean))}
function onlyKnown(type,values){
var allowed=helperKeys(type);
return unique(Array.isArray(values)?values:values?[values]:[]).filter(function(value){return allowed.has(value)});
}
function applyPageConfiguration(){
pageMeta=document.querySelector(".page-meta-container");
defaultDataTab=clean(pageMeta&&pageMeta.getAttribute("data-lp-default-tab")||"horses").toLowerCase()==="classes"?"classes":"horses";
currentDataTab=defaultDataTab;
classPageSize=Math.max(10,Math.min(100,Number(pageMeta&&pageMeta.getAttribute("data-lp-page-size"))||10));
enableClassDateFilter=clean(pageMeta&&pageMeta.getAttribute("data-lp-class-date-filter")).toLowerCase()==="true";
root.classList.toggle("is-print-friendly",clean(pageMeta&&pageMeta.getAttribute("data-lp-print-friendly")).toLowerCase()==="true");
var visible=clean(pageMeta&&pageMeta.getAttribute("data-lp-visible-sections"));
if(visible){
var allowed=new Set(visible.split(",").map(clean).filter(Boolean));
var recordsTabs=root.querySelector("[data-lp-dashboard-data-tabs]");
var visualsParent=recordsTabs&&recordsTabs.closest('[data-lp-dashboard-section="visuals"]');
if(recordsTabs&&visualsParent){visualsParent.insertAdjacentElement("afterend",recordsTabs);}
root.querySelectorAll("[data-lp-dashboard-section]").forEach(function(section){
var visible=allowed.has(clean(section.getAttribute("data-lp-dashboard-section")));
section.hidden=!visible;
if(visible)section.style.removeProperty("display");
else section.style.setProperty("display","none","important");
});
var recordsTabs=root.querySelector("[data-lp-dashboard-data-tabs]");
if(recordsTabs){
var recordsVisible=allowed.has("horses")||allowed.has("classes");
recordsTabs.hidden=!recordsVisible;
if(recordsVisible)recordsTabs.style.removeProperty("display");
else recordsTabs.style.setProperty("display","none","important");
}
}
var config={};
try{config=JSON.parse(pageMeta&&pageMeta.getAttribute("data-lp-filtered-params")||"{}")}
catch(error){throw new Error("Invalid dashboard component parameters")}
filterState.rider=onlyKnown("riders",config.rider);
filterState.horse=onlyKnown("horses",config.horse);
filterState.year=onlyKnown("years",config.year);
filterState.height=onlyKnown("heights",config.height);
filterState.discipline=onlyKnown("disciplines",config.discipline);
filterState.skill=onlyKnown("skills",config.skill||config.skills);
filterState.resultGroup=unique(Array.isArray(config.resultGroup)?config.resultGroup:Array.isArray(config.result)?config.result:[]).filter(function(key){return["wins","top3","top8"].indexOf(clean(key))>=0}).map(clean);
if(!filterState.rider.length&&refList("riders").length===1)filterState.rider=[clean(refList("riders")[0].key)];
defaultFilterState=copyFilterState(filterState);
applyDefaultDataTab();
}
function ensureLatestTab(){
if(!dom.dataTabs)return;
var tablist=dom.dataTabs.querySelector('[role="tablist"]');
if(!tablist||tablist.querySelector('[data-lp-dashboard-data-tab="latest"]'))return;
var button=document.createElement("button");
button.className="lp-dashboard__data-tab";
button.type="button";
button.setAttribute("role","tab");
button.setAttribute("aria-selected","false");
button.setAttribute("data-lp-dashboard-data-tab","latest");
button.textContent="Latest";
tablist.appendChild(button);
}
function applyDefaultDataTab(){
if(!dom.dataTabs)return;
ensureLatestTab();
dom.dataTabs.querySelectorAll("[data-lp-dashboard-data-tab]").forEach(function(tab){
var active=clean(tab.getAttribute("data-lp-dashboard-data-tab"))===defaultDataTab;
tab.classList.toggle("is-active",active);
tab.setAttribute("aria-selected",String(active));
});
dom.dataTabs.querySelectorAll("[data-lp-dashboard-data-panel]").forEach(function(panel){
var active=clean(panel.getAttribute("data-lp-dashboard-data-panel"))===defaultDataTab;
panel.classList.toggle("is-active",active);
panel.hidden=!active;
});
}
function rowsForContext(rows){
if(context==="horse"&&entityKey)return rows.filter(function(row){return clean(row.horseKey)===entityKey});
if(context==="rider"&&entityKey){
var sole=refList("riders").length===1?clean(refList("riders")[0].key):"";
return rows.filter(function(row){return clean(row.riderKey||sole)===entityKey});
}
return rows.slice();
}
function rowValues(row,type){
var soleRider=refList("riders").length===1?clean(refList("riders")[0].key):"";
if(type==="rider")return unique([row.riderKey||soleRider]);
if(type==="horse")return unique([row.horseKey]);
if(type==="year")return unique([row.yearKey]);
if(type==="result")return unique([row.resultKey]);
if(type==="height")return unique([row.heightKey]);
if(type==="discipline")return unique([row.disciplineKey].concat(row.disciplineRefKeys||[]));
if(type==="skill")return unique([row.skillKey].concat(row.skillRefKeys||[])).filter(function(key){return helperKeys("skills").has(key)});
if(type==="resultGroup"){
var result=refMap("results")[clean(row.resultKey)],place=Number(result&&result.sort),values=[];
if(place===1)values.push("wins");
if(place>=1&&place<=3)values.push("top3");
if(place>=1&&place<=8)values.push("top8");
return values;
}
return[];
}
function matches(row,type){
var selected=filterState[type];
if(!selected.length)return true;
var values=rowValues(row,type);
return selected.some(function(key){return values.indexOf(key)>=0});
}
function filteredRows(ignoreType){
var types=["rider","horse","year","height","discipline","resultGroup","skill"];
return sourceRows.filter(function(row){
return types.every(function(type){return type===ignoreType||matches(row,type)});
});
}
function countKeys(rows,getKeys){
var counts=Object.create(null);
rows.forEach(function(row){
var keys=getKeys(row);
if(!Array.isArray(keys))keys=[keys];
unique(keys).forEach(function(key){counts[key]=(counts[key]||0)+1});
});
return counts;
}
function facet(type,referenceType){
var counts=countKeys(filteredRows(type),function(row){return rowValues(row,type)});
return refList(referenceType).map(function(item){
var key=clean(item.key);
return{key:key,label:refDisplay(referenceType,item),count:counts[key]||0,sort:Number(item.sort),color:refColor(item)};
}).filter(function(item){
if(!item.key||!item.label||item.count<=0)return false;
if(type==="height")return["3'3\"","3'6\"","3'9\""].indexOf(item.label)>=0;
if(type==="discipline")return["hunter","jumper","equitation"].indexOf(item.label.toLowerCase())>=0;
return true;
})
.sort(function(a,b){
var aSort=Number.isFinite(a.sort)?a.sort:9999,bSort=Number.isFinite(b.sort)?b.sort:9999;
return aSort-bSort||a.label.localeCompare(b.label);
});
}
function pill(item,type,active){
var style=item.color?' style="--lp-pill-color:'+html(item.color)+'"':"";
return'<button type="button" class="lp-filter-pill'+(active?' is-active':'')+'" data-lp-filter-type="'+type+'" data-lp-filter-value="'+html(item.key)+'" aria-pressed="'+(active?'true':'false')+'"'+style+'>'+html(item.label)+'</button>';
}
function isSelected(type,key){return filterState[type].indexOf(clean(key))>=0}
function resultGroupFacets(){
var rows=filteredRows("resultGroup"),resultItems=refList("results");
function colorForPlace(place){
var item=resultItems.find(function(result){return Number(result.sort)===place});
return refColor(item);
}
return[
{key:"wins",label:"Wins",count:rows.filter(function(row){return rowValues(row,"resultGroup").indexOf("wins")>=0}).length,sort:1,color:colorForPlace(1)},
{key:"top3",label:"Top 3",count:rows.filter(function(row){return rowValues(row,"resultGroup").indexOf("top3")>=0}).length,sort:2,color:colorForPlace(3)},
{key:"top8",label:"Top 8",count:rows.filter(function(row){return rowValues(row,"resultGroup").indexOf("top8")>=0}).length,sort:3,color:colorForPlace(8)}
].filter(function(item){return item.count>0});
}
function renderFilters(){
var sets=[
["year","years",true],
["height","heights",false],
["discipline","disciplines",false],
["skill","skills",false]
];
sets.forEach(function(config){
var type=config[0],items=facet(type,config[1]),markup="";
if(config[2])markup+=pill({key:"",label:"All",color:""},type,!filterState[type].length);
markup+=items.map(function(item){return pill(item,type,isSelected(type,item.key))}).join("");
if(dom.filters[type])dom.filters[type].innerHTML=markup;
});
if(dom.filters.resultGroup){
dom.filters.resultGroup.innerHTML=resultGroupFacets().map(function(item){return pill(item,"resultGroup",isSelected("resultGroup",item.key))}).join("");
}
}
function aggregateValue(rows,metric){
var places=rows.map(function(row){return Number(row.place)});
if(metric==="years-count")return new Set(rows.map(function(row){return clean(row.yearKey)}).filter(Boolean)).size;
if(metric==="competitions-count")return new Set(rows.map(function(row){return clean(row.competitionKey)}).filter(Boolean)).size;
if(metric==="classes-count")return rows.length;
if(metric==="horses-count")return new Set(rows.map(function(row){return clean(row.horseKey)}).filter(Boolean)).size;
if(metric==="wins-count")return places.filter(function(value){return value===1}).length;
if(metric==="second-count")return places.filter(function(value){return value===2}).length;
if(metric==="third-count")return places.filter(function(value){return value===3}).length;
if(metric==="fourth-count")return places.filter(function(value){return value===4}).length;
if(metric==="fifth-count")return places.filter(function(value){return value===5}).length;
if(metric==="sixth-count")return places.filter(function(value){return value===6}).length;
if(metric==="seventh-count")return places.filter(function(value){return value===7}).length;
if(metric==="eighth-count")return places.filter(function(value){return value===8}).length;
if(metric==="top3-count")return places.filter(function(value){return value>=1&&value<=3}).length;
if(metric==="top8-count")return places.filter(function(value){return value>=1&&value<=8}).length;
return null;
}
function aggregateLabel(metric,value,label){
label=clean(label);
if(value!==1)return label;
var singular={
"years-count":"Year",
"competitions-count":/show/i.test(label)?"Show":"Competition",
"classes-count":"Class",
"horses-count":"Horse",
"wins-count":"Win",
"second-count":/second/i.test(label)?"Second":label,
"third-count":/third/i.test(label)?"Third":label,
"fourth-count":/fourth/i.test(label)?"Fourth":label,
"fifth-count":/fifth/i.test(label)?"Fifth":label,
"sixth-count":/sixth/i.test(label)?"Sixth":label,
"seventh-count":/seventh/i.test(label)?"Seventh":label,
"eighth-count":/eighth/i.test(label)?"Eighth":label,
"top3-count":label.replace(/[’']?s$/i,""),
"top8-count":label.replace(/[’']?s$/i,"")
};
return singular[metric]||label;
}
function populateAggregateBindings(rows){
root.querySelectorAll("[data-lp-agg-values]").forEach(function(node){
var key=clean(node.getAttribute("data-lp-agg-values")),value=aggregateValue(rows,key);
if(value==null)return;
node.textContent=String(value);
var card=node.closest("[data-lp-agg-key]");
var labelNode=card&&card.querySelector("[data-lp-agg-label]");
if(labelNode)labelNode.textContent=aggregateLabel(key,value,labelNode.getAttribute("data-lp-agg-label-default")||labelNode.textContent);
});
}
function renderStats(rows){
var order=context==="global"
?["years-count","competitions-count","classes-count","horses-count","wins-count","second-count","third-count","top8-count"]
:["years-count","competitions-count","classes-count","wins-count","second-count","third-count","top3-count","top8-count"];
var aggs=refMap("aggs");
dom.stats.innerHTML=order.map(function(key){
var item=aggs[key],value=aggregateValue(rows,key);
if(!item||value==null||!clean(item.display))return"";
var color=refColor(item),style=color?' style="--lp-agg-color:'+html(color)+';background:var(--lp-agg-color)"':"";
var label=aggregateLabel(key,value,item.display);
return'<div class="lp-dashboard-stat" data-lp-agg-key="'+html(key)+'"'+style+'><strong class="lp-dashboard-stat__value" data-lp-agg-values="'+html(key)+'">'+value+'</strong><span class="lp-dashboard-stat__label" data-lp-agg-label data-lp-agg-label-default="'+html(item.display)+'">'+html(label)+'</span></div>';
}).join("");
populateAggregateBindings(rows);
}
function seriesData(rows,type){
var counts=countKeys(rows,function(row){
if(type==="years")return rowValues(row,"year");
if(type==="heights")return rowValues(row,"height");
if(type==="disciplines")return rowValues(row,"discipline");
return rowValues(row,"result");
});
return refList(type).map(function(item){
var key=clean(item.key);
return{key:key,label:clean(item.display),count:counts[key]||0,sort:Number(item.sort),color:refColor(item)};
}).filter(function(item){
if(!item.key||!item.label||!item.color||item.count<=0)return false;
if(type==="results")return Number.isFinite(item.sort)&&item.sort>=1&&item.sort<=8;
return true;
}).sort(function(a,b){
var aSort=Number.isFinite(a.sort)?a.sort:9999,bSort=Number.isFinite(b.sort)?b.sort:9999;
return aSort-bSort||a.label.localeCompare(b.label);
});
}
function emptyChart(mount){mount.innerHTML='<div class="lp-dashboard-chart-empty">No matching data</div>'}
function createBar(mount,data,horizontal){
if(!data.length){emptyChart(mount);return null}
return window.agCharts.AgCharts.create({
container:mount,
data:data,
background:{fill:"#fff"},
padding:{top:24,right:24,bottom:26,left:24},
legend:{enabled:false},
series:[{
type:"bar",
direction:horizontal?"horizontal":"vertical",
xKey:"label",
yKey:"count",
strokeWidth:1,
itemStyler:function(params){
var color=clean(params.datum.color).toLowerCase();
var white=color==="#fff"||color==="#ffffff"||color==="white";
return{fill:params.datum.color,stroke:white?"#111":params.datum.color,strokeWidth:white?1:0};
},
label:{enabled:true,placement:"outside-end"}
}]
});
}
function renderCharts(rows){
charts.forEach(function(chart){if(chart&&typeof chart.destroy==="function")chart.destroy()});
charts=[];
Object.keys(dom.charts).forEach(function(key){if(dom.charts[key])dom.charts[key].innerHTML=""});
charts=[
createBar(dom.charts.results,seriesData(rows,"results"),true),
createBar(dom.charts.years,seriesData(rows,"years"),false),
createBar(dom.charts.heights,seriesData(rows,"heights"),false),
createBar(dom.charts.disciplines,seriesData(rows,"disciplines"),false)
].filter(Boolean);
requestAnimationFrame(updateVisualNav);
}
function entityCell(value,href,secondary,dashHref,videoId,videoTitle,videoMeta,entityType){
var detail=secondary?'<span class="lp-dashboard-entity-secondary">'+html(secondary)+'</span>':"";
var link=href?' <a class="lp-dashboard-usef-link" href="'+html(href)+'" target="_blank" rel="noopener">USEF</a>':"";
var dash=dashHref?' <a class="lp-dashboard-dash-link is-'+html(entityType||"")+'" href="'+html(dashHref)+'">DASH</a>':"";
var video=videoId?' <button type="button" class="lp-dashboard-video-link" data-lp-class-video-id="'+html(videoId)+'" data-lp-class-video-title="'+html(videoTitle||value)+'" data-lp-class-video-meta="'+html(videoMeta||"")+'">VIDEO</button>':"";
return'<span class="lp-dashboard-entity-name">'+html(value)+detail+link+dash+video+'</span>';
}
function cellRenderer(valueField,hrefField,secondaryField,dashField,videoField,videoTitleField,videoMetaField){
return function(params){return entityCell(params.data&&params.data[valueField],params.data&&params.data[hrefField],secondaryField&&params.data&&params.data[secondaryField],dashField&&params.data&&params.data[dashField],videoField&&params.data&&params.data[videoField],videoTitleField&&params.data&&params.data[videoTitleField],videoMetaField&&params.data&&params.data[videoMetaField],valueField)};
}
function solidDataPillRenderer(valueField,colorField){
return function(params){
var row=params.data||{},label=clean(row[valueField]),color=clean(row[colorField]);
if(!label||!color)return label?html(label):'<span class="lp-dashboard-entity-secondary">—</span>';
var lower=color.toLowerCase(),light=lower==="#fff"||lower==="#ffffff"||lower==="#ffff00"||lower==="#ffcc00"||lower==="#ffd000";
return '<span class="lp-dashboard-data-pill" style="--lp-data-pill-color:'+html(color)+';--lp-data-pill-border:'+(light?'#111':html(color))+';--lp-data-pill-text:'+(light?'#111':'#fff')+'">'+html(label)+'</span>';
};
}
function horseResultBadgeRenderer(params){
var row=params.data||{},label=clean(row.highestResult)||"—",color=clean(row.highestResultColor);
if(!color)return '<span class="lp-dashboard-entity-secondary">—</span>';
var lower=color.toLowerCase(),light=lower==="#fff"||lower==="#ffffff"||lower==="#ffff00"||lower==="#ffcc00"||lower==="#ffd000";
return '<span class="lp-horse-result-badge" style="--lp-result-color:'+html(color)+';--lp-result-border:'+(light?"#111":html(color))+';--lp-result-text:'+(light?"#111":"#fff")+'">'+html(label)+'</span>';
}
function windowPurple(value){
var percent=Math.max(0,Math.min(100,Number(value)||0));
if(percent<=10)return{fill:"#E9DDF7",text:"#35164F",stroke:"#C8A9E6"};
if(percent<=20)return{fill:"#D5BCEB",text:"#35164F",stroke:"#B58AD8"};
if(percent<=40)return{fill:"#B38ADB",text:"#28103E",stroke:"#9561C5"};
if(percent<=80)return{fill:"#7A42AE",text:"#FFFFFF",stroke:"#63318F"};
return{fill:"#4A176E",text:"#FFFFFF",stroke:"#35104F"};
}
function windowBadgeRenderer(params){
var row=params.data||{},tone=windowPurple(row.windowPercent);
return '<span class="lp-horse-window-badge" style="--lp-window-color:'+tone.fill+';--lp-window-text:'+tone.text+';--lp-window-stroke:'+tone.stroke+'">'+html(params.value||"—")+'</span>';
}
function windowRangeLine(label,value,isReference){
var percent=Math.max(0,Math.min(100,Number(value)||0)),tone=windowPurple(percent);
var visual=isReference
?'<span class="lp-horse-window-range__reference" style="--lp-window-line:'+tone.stroke+';--lp-window-marker:'+tone.fill+';--lp-marker-position:'+percent+'%" aria-hidden="true"><span class="lp-horse-window-range__marker"></span></span>'
:'<span class="lp-horse-window-range__track" aria-hidden="true"><span class="lp-horse-window-range__fill" style="width:'+percent+'%;background:'+tone.fill+';border-color:'+tone.stroke+'"></span></span>';
return '<span class="lp-horse-window-range__line"><span class="lp-horse-window-range__label">'+html(label)+'</span>'+visual+'</span>';
}
function windowRangeRenderer(params){
var row=params.data||{};
return '<div class="lp-horse-window-range">'+windowRangeLine(row.horse||"Horse",row.windowPercent,false)+windowRangeLine("Lainey avg",row.averagePercent,true)+windowRangeLine("12 months",80,true)+'</div>';
}
function classGridRows(rows){
var maps={
horses:refMap("horses"),riders:refMap("riders"),results:refMap("results"),heights:refMap("heights"),
disciplines:refMap("disciplines"),competitions:refMap("competitions"),years:refMap("years"),skills:refMap("skills")
};
var sole=refList("riders").length===1?refList("riders")[0]:null;
return rows.map(function(row,index){
var horse=maps.horses[clean(row.horseKey)];
var rider=maps.riders[clean(row.riderKey||(sole&&sole.key))];
var result=maps.results[clean(row.resultKey)];
var height=maps.heights[clean(row.heightKey)];
var competition=maps.competitions[clean(row.competitionKey)];
var year=maps.years[clean(row.yearKey)];
var disciplineKey=rowValues(row,"discipline").find(function(key){return!!maps.disciplines[key]})||"";
var discipline=maps.disciplines[disciplineKey];
var skillKeys=unique([row.skillKey].concat(row.skillRefKeys||[])).filter(function(key){return!!maps.skills[key]});
return{
id:clean(row.id)||"class-"+clean(row.sourcePosition||index),
date:clean(row.date||row.dateRaw),
dateMs:Number(row.dateMs)||0,
horse:refDisplay("horses",horse),
horseShowName:clean(horse&&horse.showName),
horseUsef:refUsef(horse),
horseDash:refDash("horses",horse),
horseKey:horse?clean(row.horseKey):"",
rider:refDisplay("riders",rider),
riderUsef:refUsef(rider),
riderDash:refDash("riders",rider),
riderKey:rider?clean(rider.key):"",
className:clean(row.title),
classUsef:refUsef(row),
youtubeId:clean(row.youtubeId),
videoMeta:[clean(row.date||row.dateRaw),refDisplay("competitions",competition)].filter(Boolean).join(" · "),
result:clean(result&&result.display),
resultKey:result?clean(result.key):"",
resultColor:result?refColor(result):"",
height:clean(height&&height.display),
heightKey:height?clean(height.key):"",
heightColor:height?refColor(height):"",
discipline:clean(discipline&&discipline.display),
disciplineKey:discipline?clean(discipline.key):"",
disciplineColor:discipline?refColor(discipline):"",
competition:refDisplay("competitions",competition),
competitionUsef:refUsef(competition),
competitionKey:competition?clean(competition.key):"",
year:clean(year&&year.display),
yearKey:year?clean(year.key):"",
skills:skillKeys.map(function(key){return clean(maps.skills[key].display)}),
skillKeys:skillKeys
};
});
}
function latestRows(rows){
var dated=rows.filter(function(row){return Number(row.dateMs)>0});
if(!dated.length)return[];
var anchor=Math.max.apply(null,dated.map(function(row){return Number(row.dateMs)}));
var cutoff=anchor-(42*86400000);
return dated.filter(function(row){return Number(row.dateMs)>=cutoff&&Number(row.dateMs)<=anchor});
}
function renderClassGrid(rows){
var selectedRows=currentDataTab==="latest"?latestRows(rows):rows;
var data=classGridRows(selectedRows);
if(dom.rowCount)dom.rowCount.textContent=data.length+" records";
if(classGridApi){classGridApi.setGridOption("rowData",data);return}
classGridApi=window.agGrid.createGrid(dom.classGrid,{
theme:"legacy",
rowData:data,
getRowId:function(params){return params.data.id},
columnDefs:[
{field:"date",headerName:"Date",minWidth:125,flex:.72,sort:"desc",filter:enableClassDateFilter?"agDateColumnFilter":false,filterParams:enableClassDateFilter?{comparator:function(filterDate,cellValue){var value=new Date(clean(cellValue)+"T00:00:00");if(!Number.isFinite(value.getTime()))return-1;var day=new Date(value.getFullYear(),value.getMonth(),value.getDate());return day<filterDate?-1:day>filterDate?1:0}}:undefined},
{field:"className",headerName:"Class",minWidth:240,flex:1.7,cellRenderer:cellRenderer("className","classUsef","","","youtubeId","className","videoMeta")},
{field:"horse",headerName:"Horse",minWidth:210,flex:1.2,cellRenderer:cellRenderer("horse","horseUsef","horseShowName","horseDash")},
{field:"rider",headerName:"Rider",minWidth:175,flex:.95,cellRenderer:cellRenderer("rider","riderUsef","", "riderDash")},
{field:"result",headerName:"Result",minWidth:125,flex:.7,cellRenderer:solidDataPillRenderer("result","resultColor")},
{field:"height",headerName:"Height",minWidth:110,flex:.6,cellRenderer:solidDataPillRenderer("height","heightColor")},
{field:"discipline",headerName:"Discipline",minWidth:145,flex:.82,cellRenderer:solidDataPillRenderer("discipline","disciplineColor")},
{field:"competition",headerName:"Competition",minWidth:200,flex:1.15,cellRenderer:cellRenderer("competition","competitionUsef")},
{field:"year",headerName:"Year",hide:true},
{field:"skills",headerName:"Skills",hide:true}
],
defaultColDef:{sortable:true,filter:false,resizable:true,cellDataType:false},
pagination:true,
paginationPageSize:classPageSize,
paginationPageSizeSelector:false,
animateRows:false,
ensureDomOrder:true
});
}
function recordWindowLabel(days){
if(!Number.isFinite(days)||days<1)return"—";
if(days<=31)return"UNDER 1 MO";
if(days<=92)return"UNDER 3 MOS";
if(days<=183)return"UNDER 6 MOS";
if(days<=366)return"UNDER 12 MOS";
return"OVER 12 MOS";
}
function recordWindowPercent(days){
if(!Number.isFinite(days)||days<1)return 0;
if(days<=31)return 10;
if(days<=92)return 20;
if(days<=183)return 40;
if(days<=366)return 80;
return 100;
}
function horseGridRows(rows){
var horses=refMap("horses"),results=refMap("results"),riders=refMap("riders"),soleRider=refList("riders").length===1?refList("riders")[0]:null,groups=Object.create(null);
rows.forEach(function(row){
var key=clean(row.horseKey),horse=horses[key];
if(!horse)return;
if(!groups[key])groups[key]={key:key,horse:horse,riderKeys:[],classes:0,wins:0,top3:0,top8:0,dates:[],bestPlace:Infinity,bestResultKey:""};
var group=groups[key],place=Number(row.place),riderKey=clean(row.riderKey||(soleRider&&soleRider.key));
if(riders[riderKey]&&group.riderKeys.indexOf(riderKey)<0)group.riderKeys.push(riderKey);
group.classes+=1;
var dateMs=Number(row.dateMs);
if(Number.isFinite(dateMs)&&dateMs>0)group.dates.push(dateMs);
if(place===1)group.wins+=1;
if(place>=1&&place<=3)group.top3+=1;
if(place>=1&&place<=8)group.top8+=1;
if(place>=1&&place<=8&&place<group.bestPlace&&results[clean(row.resultKey)]){
group.bestPlace=place;
group.bestResultKey=clean(row.resultKey);
}
});
return Object.keys(groups).map(function(key){
var group=groups[key];
var dates=group.dates.slice().sort(function(a,b){return a-b});
var start=dates.length?dates[0]:0,end=dates.length?dates[dates.length-1]:0;
var windowDays=dates.length?Math.floor((end-start)/86400000)+1:null;
var bestResult=results[group.bestResultKey],rider=riders[group.riderKeys[0]];
return{
horseKey:key,
horse:refDisplay("horses",group.horse),
horseShowName:clean(group.horse.showName),
horseUsef:refUsef(group.horse),
horseDash:refDash("horses",group.horse),
rider:refDisplay("riders",rider),
riderUsef:refUsef(rider),
riderDash:refDash("riders",rider),
classes:group.classes,
wins:group.wins,
top3:group.top3,
top8:group.top8,
highestResult:bestResult?clean(bestResult.display):"—",
highestResultColor:bestResult?refColor(bestResult):"",
window:recordWindowLabel(windowDays),
windowDays:windowDays
};
}).sort(function(a,b){return a.horse.localeCompare(b.horse)});
}
function renderHorseGrid(rows){
var data=horseGridRows(rows);
var validWindows=data.map(function(item){return Number(item.windowDays)}).filter(function(days){return Number.isFinite(days)&&days>0});
var averageDays=validWindows.length?validWindows.reduce(function(sum,days){return sum+days},0)/validWindows.length:0;
var averagePercent=recordWindowPercent(averageDays);
data.forEach(function(item){
item.windowPercent=recordWindowPercent(Number(item.windowDays));
item.averagePercent=averagePercent;
});
if(dom.horseCount)dom.horseCount.textContent=data.length+" horses";
if(dom.toolbarHorseCount)dom.toolbarHorseCount.textContent=data.length+" horses";
if(dom.filterSummary)dom.filterSummary.textContent="Showing "+data.length+" horses";
if(horseGridApi){horseGridApi.setGridOption("rowData",data);return}
horseGridApi=window.agGrid.createGrid(dom.horseGrid,{
theme:"legacy",
rowData:data,
getRowId:function(params){return params.data.horseKey},
columnDefs:[
{field:"horse",headerName:"Horse",minWidth:235,flex:1.6,cellRenderer:cellRenderer("horse","horseUsef","horseShowName","horseDash")},
{field:"rider",headerName:"Rider",minWidth:175,flex:.9,cellRenderer:cellRenderer("rider","riderUsef","", "riderDash")},
{field:"classes",headerName:"Classes",minWidth:90,flex:.55},
{field:"wins",headerName:"Wins",minWidth:78,flex:.48},
{field:"top3",headerName:"Top 3",minWidth:82,flex:.5},
{field:"top8",headerName:"Top 8",minWidth:82,flex:.5},
{field:"highestResult",headerName:"Highest result",minWidth:145,flex:.8,cellRenderer:horseResultBadgeRenderer},
{field:"window",headerName:"Window",minWidth:128,flex:.75,cellRenderer:windowBadgeRenderer},
{field:"windowPercent",headerName:"Window range",minWidth:280,flex:1.5,cellRenderer:windowRangeRenderer}
],
defaultColDef:{sortable:true,filter:false,resizable:true,cellDataType:false},
pagination:true,
paginationPageSize:classPageSize,
paginationPageSizeSelector:false,
animateRows:false,
ensureDomOrder:true,
onRowClicked:function(event){
if(!event.column||event.column.getColId()!=="horse")return;
var key=clean(event.data&&event.data.horseKey);
filterState.horse=filterState.horse.length===1&&filterState.horse[0]===key?[]:[key];
publishFiltered();
}
});
}
function publishFiltered(){
var rows=filteredRows("");
var persisted={
schemaVersion:"lp:classes-filtered-v1",
savedAt:Date.now(),
context:context,
entityKey:entityKey,
filters:{
rider:filterState.rider.slice(),
horse:filterState.horse.slice(),
year:filterState.year.slice(),
height:filterState.height.slice(),
discipline:filterState.discipline.slice(),
resultGroup:filterState.resultGroup.slice(),
skill:filterState.skill.slice()
},
rows:rows.slice()
};
window.LPClassesFiltered=rows.slice();
try{localStorage.setItem(FILTERED_CACHE_KEY,JSON.stringify(persisted))}
catch(error){console.warn("LP filtered classes could not be persisted",error)}
document.dispatchEvent(new CustomEvent("lp:classes-filtered",{detail:{rows:rows.slice(),count:rows.length,source:root,persisted:persisted}}));
}
function renderDashboardTitle(){
var horses=refMap("horses"),riders=refMap("riders");
if(context==="horse"&&entityKey&&horses[entityKey]){
if(dom.titleBase)dom.titleBase.textContent=refDisplay("horses",horses[entityKey]);
if(dom.titleSelection)dom.titleSelection.textContent="";
return;
}
if(context==="rider"&&entityKey&&riders[entityKey]){
if(dom.titleBase)dom.titleBase.textContent=refDisplay("riders",riders[entityKey]);
if(dom.titleSelection)dom.titleSelection.textContent="";
return;
}
if(!dom.titleSelection)return;
var selected=filterState.horse.map(function(key){
var horse=horses[clean(key)];
return horse?refDisplay("horses",horse):"";
}).filter(Boolean);
dom.titleSelection.textContent=selected.length?" + "+selected.join(" + "):"";
}
function renderFiltered(event){
if(!event.detail||event.detail.source!==root)return;
var rows=event.detail.rows||[];
renderFilters();
renderDashboardTitle();
renderStats(rows);
renderCharts(rows);
renderHorseGrid(rows);
renderClassGrid(rows);
if(dom.filterStatus)dom.filterStatus.textContent=rows.length+" filtered classes";
setStatus(rows.length+" filtered classes",false,false);
}
function classVideoController(){
if(window.LPClassVideoDrawer)return window.LPClassVideoDrawer;
var drawer=document.querySelector(".ytp-drawer");
if(!drawer)return null;
var player=drawer.querySelector("[data-ytp-drawer-player]");
function stop(){
drawer.querySelectorAll("iframe[data-lp-class-video-frame]").forEach(function(frame){frame.remove()});
if(player)Array.prototype.forEach.call(player.children,function(child){child.hidden=false});
}
function close(){
stop();
drawer.classList.remove("ytp-drawer-open");
drawer.setAttribute("aria-hidden","true");
if(window.LPScrollLock&&typeof window.LPScrollLock.unlock==="function")window.LPScrollLock.unlock("class-video");
}
function open(config){
var id=clean(config&&config.id);
if(!id)return;
var title=drawer.querySelector(".ytp-section-title");
var meta=drawer.querySelector(".ytp-card-meta");
var poster=drawer.querySelector(".ytp-drawer-poster");
if(title)title.textContent=clean(config.title);
if(meta)meta.textContent=clean(config.meta);
if(player){
player.setAttribute("data-video-id",id);
player.setAttribute("aria-label","Play "+(clean(config.title)||"video"));
}
if(poster){
poster.src="https://i.ytimg.com/vi/"+encodeURIComponent(id)+"/maxresdefault.jpg";
poster.removeAttribute("srcset");
poster.removeAttribute("sizes");
poster.alt=clean(config.title);
}
drawer.classList.add("ytp-drawer-open");
drawer.setAttribute("aria-hidden","false");
if(window.LPScrollLock&&typeof window.LPScrollLock.lock==="function")window.LPScrollLock.lock("class-video");
}
drawer.addEventListener("click",function(event){
var closer=event.target.closest("[data-ytp-close-drawer],.ytp-pill-button");
if(closer){event.preventDefault();close();return}
var target=event.target.closest("[data-ytp-drawer-player]");
if(!target)return;
event.preventDefault();
var id=clean(target.getAttribute("data-video-id"));
if(!id||target.querySelector("[data-lp-class-video-frame]"))return;
Array.prototype.forEach.call(target.children,function(child){child.hidden=true});
var frame=document.createElement("iframe");
frame.setAttribute("data-lp-class-video-frame","");
frame.src="https://www.youtube-nocookie.com/embed/"+encodeURIComponent(id)+"?autoplay=1&rel=0";
frame.title=clean(target.getAttribute("aria-label"))||"YouTube video";
frame.allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share";
frame.allowFullscreen=true;
frame.style.width="100%";
frame.style.height="100%";
frame.style.border="0";
target.appendChild(frame);
});
window.LPClassVideoDrawer={open:open,close:close};
return window.LPClassVideoDrawer;
}
function setFiltersOpen(open){
if(!dom.filterRoot||!dom.filterToggle)return;
dom.filterRoot.classList.toggle("is-open",open);
dom.filterRoot.style.setProperty("display",open?"grid":"none","important");
dom.filterRoot.setAttribute("aria-hidden",String(!open));
dom.filterToggle.classList.toggle("is-active",open);
dom.filterToggle.setAttribute("aria-expanded",String(open));
dom.filterToggle.setAttribute("aria-label",open?"Close filters":"Open filters");
}
dom.filterToggle.addEventListener("click",function(event){event.preventDefault();event.stopPropagation();setFiltersOpen(!dom.filterRoot.classList.contains("is-open"))});
function updateVisualNav(){
if(!dom.visualGrid||!dom.visualPrev||!dom.visualNext)return;
var maxScroll=Math.max(0,dom.visualGrid.scrollWidth-dom.visualGrid.clientWidth);
var hasOverflow=dom.visualGrid.clientWidth>0&&maxScroll>2;
var atStart=!hasOverflow||dom.visualGrid.scrollLeft<=2;
var atEnd=!hasOverflow||dom.visualGrid.scrollLeft>=maxScroll-2;
dom.visualPrev.hidden=atStart;
dom.visualNext.hidden=atEnd;
dom.visualPrev.disabled=atStart;
dom.visualNext.disabled=atEnd;
}
function moveVisual(direction){
if(!dom.visualGrid)return;
var tile=dom.visualGrid.querySelector(".lp-dashboard-widget");
if(!tile)return;
var styles=getComputedStyle(dom.visualGrid),gap=parseFloat(styles.columnGap||styles.gap)||0,step=tile.getBoundingClientRect().width+gap;
dom.visualGrid.scrollBy({left:direction*step,behavior:"smooth"});
}
if(dom.visualGrid)dom.visualGrid.addEventListener("scroll",updateVisualNav,{passive:true});
window.addEventListener("resize",updateVisualNav);
if(dom.visualPrev)dom.visualPrev.addEventListener("click",function(){moveVisual(-1)});
if(dom.visualNext)dom.visualNext.addEventListener("click",function(){moveVisual(1)});
dom.filterRoot.querySelectorAll("[data-lp-filter-close]").forEach(function(button){button.addEventListener("click",function(event){event.preventDefault();event.stopPropagation();setFiltersOpen(false)})});
document.addEventListener("lp:classes-filtered",renderFiltered);
root.addEventListener("click",function(event){
var videoAction=event.target.closest("[data-lp-class-video-id]");
if(videoAction){
event.preventDefault();
var controller=classVideoController();
if(controller)controller.open({
id:videoAction.getAttribute("data-lp-class-video-id"),
title:videoAction.getAttribute("data-lp-class-video-title"),
meta:videoAction.getAttribute("data-lp-class-video-meta")
});
return;
}
var clearFilters=event.target.closest("[data-lp-filter-clear]");
if(clearFilters){
filterState=copyFilterState(defaultFilterState);
publishFiltered();
return;
}
var dataTab=event.target.closest("[data-lp-dashboard-data-tab]");
if(dataTab&&dom.dataTabs){
var target=clean(dataTab.getAttribute("data-lp-dashboard-data-tab"));
currentDataTab=target;
dom.dataTabs.querySelectorAll("[data-lp-dashboard-data-tab]").forEach(function(tab){
var active=clean(tab.getAttribute("data-lp-dashboard-data-tab"))===target;
tab.classList.toggle("is-active",active);
tab.setAttribute("aria-selected",String(active));
});
dom.dataTabs.querySelectorAll("[data-lp-dashboard-data-panel]").forEach(function(panel){
var panelName=clean(panel.getAttribute("data-lp-dashboard-data-panel"));
var active=panelName===(target==="latest"?"classes":target);
panel.classList.toggle("is-active",active);
panel.hidden=!active;
});
if(target==="classes"||target==="latest")renderClassGrid(filteredRows(""));
return;
}
var control=event.target.closest("[data-lp-filter-type]");
if(!control)return;
var type=clean(control.getAttribute("data-lp-filter-type"));
if(type==="rider")return;
var value=clean(control.getAttribute("data-lp-filter-value"));
if(!value)filterState[type]=[];
else if(type==="horse"){
filterState.horse=isSelected("horse",value)
?filterState.horse.filter(function(key){return key!==value})
:unique(filterState.horse.concat([value]));
}else{
var index=filterState[type].indexOf(value);
if(index>=0)filterState[type].splice(index,1);
else filterState[type].push(value);
}
publishFiltered();
});
async function init(){
try{
injectRuntimeStyles();
setStatus("Loading dashboard…",false,true);
var data=await readData();
if(!data||!Array.isArray(data.classes)||!data.classes.length)throw new Error("No class records are available");
references=data.references||{};
sourceRows=rowsForContext(data.classes);
applyPageConfiguration();
await Promise.all([
loadLibrary(
["https://cdn.jsdelivr.net/npm/ag-grid-community@36.1.0/dist/ag-grid-community.min.js","https://unpkg.com/ag-grid-community@36.1.0/dist/ag-grid-community.min.js"],
function(){return!!(window.agGrid&&window.agGrid.createGrid)}
),
loadLibrary(
["https://cdn.jsdelivr.net/npm/ag-charts-community@14.1.0/dist/umd/ag-charts-community.min.js","https://unpkg.com/ag-charts-community@14.1.0/dist/umd/ag-charts-community.min.js"],
function(){return!!(window.agCharts&&window.agCharts.AgCharts)}
)
]);
publishFiltered();
}catch(error){
console.error("LP shared dashboard",error);
setStatus(error.message||String(error),true,false);
}
}
document.readyState==="loading"?document.addEventListener("DOMContentLoaded",init,{once:true}):init();
})();

