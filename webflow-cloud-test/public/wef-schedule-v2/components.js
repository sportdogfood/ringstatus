import {commentsFor,commentCount} from './comments.js?v=tokens-policy-1';
import {ringDetails} from './schedule-data.js';
const iconPaths={comment:'<path d="M21 11a8 8 0 0 1-8 8H8l-5 3 1.5-5A8 8 0 1 1 21 11Z"/>',ring:'<rect x="2" y="5" width="20" height="14" rx="7"/><rect x="5" y="8" width="14" height="8" rx="4"/>',group:'<path d="M4 3v14a2 2 0 0 0 2 2h4M4 9h6M14 9h7M14 19h7M4 3h6"/>',helmet:'<path d="M4 14v-3a8 8 0 0 1 16 0v3H4Zm0 0H2m18 0h2M6 14v3l5 4 4-7M12 3v6"/>',rider:'<path d="M8 7V6a4 4 0 0 1 8 0v1M7 7h11M8 9a4 4 0 0 0 8 0M5 22v-3a7 7 0 0 1 14 0v3"/>',trainer:'<circle cx="12" cy="7" r="3"/><path d="M5 21v-3a7 7 0 0 1 14 0v3M9 14l3 3 3-3M12 17v4"/>' ,pin:'<path d="M20 10c0 6-8 12-8 12S4 16 4 10a8 8 0 1 1 16 0Z"/><circle cx="12" cy="10" r="2.5"/>',horse:'<path d="M19.5 22c.2-3.6.9-6.4.1-10.2-.5-2.7-2-4.9-4.4-6.4L14.8 2l-2.4 3.2-2.3-2.4.2 4.1C9 8.1 8.7 10 7.2 11.7l-3.5 3.8c-.6.7-.5 1.6.2 2.2l1.8 1.4c.6.5 1.4.5 2 .1l4.1-3.1c1.5 1.7.9 3.9-.8 5.9"/><path d="M15.3 6.5c3 3.4 3.4 6.8 3 10.5M6 15.5l.5.2"/><circle cx="11.6" cy="10.2" r=".65" fill="currentColor" stroke="none"/>',list:'<path d="M9 6h12M9 12h12M9 18h12M3 5h1v3M3 11h2l-2 3h2M3 17h2v3H3"/>',clock:'<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>'};
export const icon=name=>`<svg class="rings-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${iconPaths[name]||''}</svg>`;
const escape=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const paths={document:'<path d="M6 3h8l4 4v14H6Z"/><path d="M14 3v5h4M9 12h6M9 16h6"/>',stopwatch:'<circle cx="12" cy="14" r="8"/><path d="M12 6V3M9 2h6M18 7l2-2M12 10v4l2 2"/>',person:'<circle cx="12" cy="7" r="3"/><path d="M5 21v-3a7 7 0 0 1 14 0v3"/>',walk:'<circle cx="14" cy="4" r="2"/><path d="m7 12 4-5 4 3 4 1M11 8l-1 7-4 6m4-6 5 2 2 5"/>',trophy:'<path d="M8 3h8v7a4 4 0 0 1-8 0ZM8 5H4v3a4 4 0 0 0 4 4m8-7h4v3a4 4 0 0 1-4 4m-4 2v7m-4 0h8"/>'};
const svg=n=>paths[n]?'<svg class="rings-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'+paths[n]+'</svg>':icon(n);
const typeLabel=t=>({HNT:'HUNT',JMP:'JUMP',EQ:'EQTA'}[t]??t);
const statusLabel=s=>({done:'DONE',next:'NEXT',now:'LIVE',upc:'UPCM',scratch:'SCRT'}[s]??s?.toUpperCase()??'');
const minutesText=v=>'IN '+(v==null?'—':v);
const status=(s,n)=>!s?'':'<span class="v2-status"><span class="rings-badge" data-status="'+escape(s)+'">'+escape(statusLabel(s))+(n!=null?' '+n:'')+'</span></span>';
const countdown=r=>r.status==='now'?r.ends:['next','upc'].includes(r.status)?r.till:null;
const classHead=()=>'<div class="v2-class v2-thead" aria-hidden="true"><span>START</span><span>NO</span><span>CLASS</span><span>SEQ</span><span>TYPE</span><span>STATUS</span><span>MINS</span></div>';
const metric=(label,v)=>'<span class="v2-metric"><span class="rings-token-label">'+label+'</span><span>'+escape(v)+'</span></span>';
const identity=(i,name)=>'<span class="v2-identity">'+svg(i)+'<span title="'+escape(name)+'">'+escape(name)+'</span></span>';
export function entryPill(row,e){return '<button class="rings-entry-rollup" type="button" data-row-key="'+escape(row.key)+'" data-entry-key="'+escape(e.key)+'">'+icon('horse')+'<span class="rings-entry-name">'+escape(e.name.toUpperCase())+'</span>'+(e.order!=null?'<span class="rings-order">('+escape(e.order)+')</span>':'')+'<span aria-hidden="true">›</span></button>';}
const commentToken=(r,e=null)=>{const count=commentCount(r.key,e?.key??null);return '<span class="v2-comment-count" aria-label="'+count+' comments">'+icon('comment')+(count>0?'<span>'+count+'</span>':'')+'</span>';};
const oog10Text=value=>{const match=/^(\d+)→10$/.exec(value??'');return match?'10 OUT in '+match[1]:value;};
const detailToggle=kind=>'<button type="button" class="v2-detail-toggle" data-detail-toggle="'+kind+'" aria-expanded="true" aria-label="Hide '+(kind==='entry'?'horse, rider and groom':'group')+'">−</button>';
const metadataToggle=r=>'<button type="button" class="v2-meta-toggle" aria-expanded="false" aria-label="Show metadata for class '+escape(r.id)+'">+</button>';
const classIdentity=r=>'<span class="v2-identity">'+icon('list')+metadataToggle(r)+'<span title="'+escape(r.name)+'">'+escape(r.name.toUpperCase())+'</span></span>';
const classOpen=r=>'<button class="v2-class-open" type="button" data-row-key="'+escape(r.key)+'" aria-haspopup="dialog" aria-label="View class '+escape(r.id)+' '+escape(r.name)+'"></button>';
export function classLine(r,interactive=true){if(r.kind!=='class')return '<div class="v2-class v2-schedule-block"><span class="v2-start detail-value">'+escape(r.time)+'</span><span></span><span class="v2-name detail-value" title="'+escape(r.name)+'">'+escape(r.name)+'</span></div>';const tag='div';return '<div class="v2-class">'+(interactive?classOpen(r):'')+
'<span class="v2-start detail-value" title="Class start">'+escape(r.time)+'</span><span class="v2-no detail-value" data-seq="'+escape(r.sequence)+'">'+escape(r.id)+'</span><span class="v2-name detail-value" title="'+escape(r.name)+'">'+classIdentity(r)+'<span class="v2-desktop-comments">'+commentToken(r)+'</span></span><span class="v2-seq" data-seq="'+escape(r.sequence)+'">'+escape(r.sequence)+'</span><span class="v2-type">'+escape(typeLabel(r.type))+'</span><span class="v2-class-state">'+status(r.status)+'</span><span class="v2-till" aria-label="'+'Minutes: '+(countdown(r)??'unavailable')+'">'+metric('IN',countdown(r)??'—')+'</span><span class="v2-mobile-meta"><span>'+escape(r.sequence)+'</span><span>'+escape(typeLabel(r.type))+'</span>'+('<span class="v2-mobile-countdown">IN <strong>'+escape(countdown(r)??'—')+'</strong></span>')+commentToken(r)+'</span>'+(!interactive&&r.group?'<span class="v2-group-subline">'+identity('group',r.group)+'</span>':'')+'</'+tag+'>';}
function ringHead(ring){
 const d=ringDetails[ring],hasOffset=Number.isFinite(d.offset),hasWalk=Number.isFinite(d.walk)&&d.walk>=0;
 const offsetKind=hasOffset?(d.offset>0?'late':d.offset<0?'early':'on-time'):'unknown';
 const offsetText=hasOffset?(d.offset===0?'ON TIME':(d.offset>0?'LATE ':'EARLY ')+Math.abs(d.offset)):'—';
 const offsetLabel=hasOffset?(d.offset===0?'On time':Math.abs(d.offset)+' minutes '+offsetKind):'Early/late timing unavailable';
 const walkLabel=hasWalk?d.walk+' minute walk':'Walking time unavailable';
 return '<header class="rings-ring-head"><h3>'+icon('ring')+escape(d.name)+'</h3><div class="v2-ring-signals"><span class="rings-badge v2-offset" data-offset="'+offsetKind+'" aria-label="'+escape(offsetLabel)+'" title="'+escape(offsetLabel)+'">'+offsetText+'</span><span class="rings-badge v2-walk" aria-label="'+escape(walkLabel)+'" title="'+escape(walkLabel)+'">'+svg('walk')+' '+(hasWalk?d.walk:'—')+'</span></div></header>';
}
function entryLane(r){
 if(!r.entries.length)return '';
 const id='entries-'+r.scheduleId;
 return '<div class="rings-entry-line these-rollups"><div class="v2-entry-tools"><button type="button" class="v2-entry-search-toggle" aria-label="Search horses in class '+escape(r.id)+'" aria-expanded="false"><svg class="rings-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><circle cx="10" cy="10" r="6"/><path d="m15 15 6 6"/></svg></button><input type="search" class="v2-entry-search" aria-label="Search horses in class '+escape(r.id)+'" placeholder="Search horses" hidden><button type="button" class="v2-entry-expand" aria-label="Expand horses in class '+escape(r.id)+'" aria-controls="'+id+'" aria-expanded="false">+</button></div><div class="rings-entry-rollups" id="'+id+'">'+r.entries.map(e=>entryPill(r,e)).join('')+'</div><span class="v2-entry-empty" role="status" hidden>No matching horses</span></div>';
}
export function ringPanel(ring,rows){return '<section class="rings-panel">'+ringHead(ring)+(rows.length?classHead():'<p class="rings-detail-note">No schedule rows for this day.</p>')+rows.map(r=>'<div class="rings-class-related-data this-class-wrapper">'+entryLane(r)+classLine(r)+'</div>').join('')+'</section>';}
export function compactEntry(r,e,expandable=false){
 const gone=e.goneIn===1;
 const firsts=e.firstScore!=null||e.firstTime!=null;
 const mins=gone?r.ends:e.tillGo;
 const orderKind=e.runningOrder!=null?'roog':'oog';
 return '<div class="v2-entry-summary" data-expandable="'+expandable+'" aria-label="'+escape(e.name)+' entry">'+
 '<span class="v2-entry-go detail-value">'+escape(e.go||'')+'</span>'+
 '<span class="detail-value">'+escape(e.entryNo)+'</span>'+
 '<button class="v2-entry-name-link" type="button" data-row-key="'+escape(r.key)+'" data-entry-key="'+escape(e.key)+'" title="'+escape(e.name)+'">'+identity('horse',e.name.toUpperCase())+'</button>'+
 '<span class="v2-entry-state">'+status(e.status)+'</span>'+
 '<button type="button" class="v2-entry-meta-toggle" aria-expanded="false" aria-label="Show metadata for '+escape(e.name)+'">+</button><span class="v2-entry-meta">'+(e.runningOrder!=null||e.order!=null?'<span class="v2-order" data-kind="'+orderKind+'">'+orderKind.toUpperCase()+' <strong>'+escape(e.runningOrder??e.order)+'</strong></span>':'')+
 '<span class="v2-entry-countdown" data-kind="'+(gone?'ends':'go')+'">'+('IN <strong>'+escape(mins??'—')+'</strong>')+'</span>'+(gone?(firsts?'<button class="v2-firsts" type="button" data-row-key="'+escape(r.key)+'" data-entry-key="'+escape(e.key)+'" data-focus-results="true" aria-label="View first results for '+escape(e.name)+'" title="First results available">'+svg(e.firstTime!=null?'stopwatch':'document')+'</button>':''):(e.oog10Label?'<span class="v2-oog10" title="OOG10">'+escape(oog10Text(e.oog10Label))+'</span>':''))+commentToken(r,e)+'</span>'+(expandable?detailToggle('entry'):'')+'</div>';
}
const personHead=(name,labels)=>'<div class="v2-list-head"><span></span><span></span><span>'+name+'</span><span></span>'+labels.map(label=>'<span>'+label+'</span>').join('')+'</div>';
const personLine=(name,values,attrs='',symbol='person')=>'<div class="v2-person-line" '+attrs+'><span></span><span></span>'+identity(symbol,name)+'<span></span>'+values.map(value=>'<span>'+escape(value)+'</span>').join('')+'</div>';
function entryDetails(r,e){return '<section class="v2-entry" aria-label="'+escape(e.name)+' entry">'+
entryHead()+compactEntry(r,e,true)+
'<details class="v2-horse-section v2-collapse" aria-label="Horse"><summary class="v2-list-head"><span></span><span class="v2-collapse-sign" aria-hidden="true"></span><span>HORSE</span><span></span><span></span><span></span><span></span></summary>'+personLine(e.name.toUpperCase(),['','',''],'data-entity="horse"','horse')+'</details>'+
(e.rider?'<details class="v2-collapse" aria-label="Rider"><summary class="v2-list-head"><span></span><span class="v2-collapse-sign" aria-hidden="true"></span><span>RIDER</span><span></span><span>TIME</span><span>SCORE</span><span>PLACE</span></summary>'+personLine(e.rider,[e.firstTime??'',e.firstScore??'',e.place??''],'id="v2-entry-results" tabindex="-1"','rider')+'</details>':'')+
'<details class="v2-collapse" aria-label="Groom"><summary class="v2-list-head"><span></span><span class="v2-collapse-sign" aria-hidden="true"></span><span>GROOM</span><span></span><span>TASK1</span><span>TASK2</span><span>TASK3</span></summary>'+
personLine(e.groom||'—',[e.task1??'—',e.task2??'—',e.task3??'—'],e.groom?'':'title="Groom data unavailable in this snapshot"')+'</details></section>';}
const entryHead=()=>'<div class="v2-list-head v2-entry-head"><span>GO</span><span>NO</span><span>ENTRY</span><span>STATUS</span></div>';
export function commentsList(r){
 const all=commentsFor(r.key);
 const item=c=>'<article class="v2-comment"><div class="v2-comment-author">'+escape(c.author)+'</div><p>'+escape(c.text)+'</p><button type="button" data-comment-reply="'+escape(c.id)+'">Reply</button>'+all.filter(x=>x.parentId===c.id).map(item).join('')+'</article>';
 return '<details class="v2-collapse v2-comments" data-class-comments="'+escape(r.key)+'"><summary class="v2-list-head"><span></span><span class="v2-collapse-sign" aria-hidden="true"></span><span>COMMENTS</span><span></span><span></span><span></span>'+commentToken(r)+'</summary><div class="v2-comments-body"><p class="v2-comment-note">Prototype · saved in this browser</p><button type="button" data-comment-add>Add comment</button><div class="v2-comments-list">'+(all.filter(c=>!c.parentId).map(item).join('')||'<p>No comments yet.</p>')+'</div><form class="v2-comment-form" hidden><label for="class-comment-text">Add comment</label><textarea id="class-comment-text" name="comment" maxlength="1000" rows="2" required></textarea><div><button type="submit">Add comment</button><button type="button" data-comment-cancel>Cancel</button></div></form></div></details>';
}
export function detailPanel(r,e=null){return '<div class="rings-detail-panel">'+ringHead(r.ring)+classHead()+classLine(r,false)+
(e?entryDetails(r,e):r.entries.length?'<section aria-label="Entries">'+entryHead()+r.entries.map(item=>compactEntry(r,item)).join('')+'</section>':'<p class="rings-detail-note">No linked active trips in this snapshot.</p>')+
(!e?commentsList(r):'')+
(e?'<button class="v2-back" type="button" data-row-key="'+escape(r.key)+'">View class '+escape(r.id)+' · '+r.entries.length+' trips ›</button>':'')+'</div>';}
export function dayLine(r){
 if(r.kind!=='class')return '<div class="v2-day-row v2-schedule-block"><span class="v2-day-time detail-value">'+escape(r.time)+'</span><span class="v2-day-ring" title="'+escape(ringDetails[r.ring].name)+'">'+escape(ringDetails[r.ring].name)+'</span><span></span><span class="v2-day-name detail-value" title="'+escape(r.name)+'">'+escape(r.name)+'</span><span></span></div>';
 const count=new Set(r.entries.map(e=>e.entryId)).size;
 return '<div class="v2-day-row">'+classOpen(r)+
 '<span class="v2-day-time detail-value">'+escape(r.time)+'</span><span class="v2-day-ring" title="'+escape(ringDetails[r.ring].name)+'">'+escape(ringDetails[r.ring].name)+'</span>'+
 '<span class="v2-day-no detail-value" data-seq="'+escape(r.sequence)+'">'+escape(r.id)+'</span>'+
 '<span class="v2-day-name detail-value">'+classIdentity(r)+'</span>'+
 '<span class="v2-day-status">'+status(r.status)+'</span>'+
 '<span class="v2-day-entries" aria-label="'+count+' entries">'+count+(r.trips!=null?'/'+escape(r.trips):'')+'</span>'+
 '<span class="v2-day-meta"><span>'+escape(r.sequence)+'</span><span>'+escape(typeLabel(r.type))+'</span><span class="v2-day-status-text" data-status="'+escape(r.status)+'">'+escape(statusLabel(r.status))+'</span><span>'+escape(minutesText(countdown(r)))+'</span>'+commentToken(r)+'</span></div>';
}
