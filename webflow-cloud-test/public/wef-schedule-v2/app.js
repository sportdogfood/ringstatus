import {headerDropdowns} from './header-dropdown.js';
import {dayRows} from './day.js?v=source-order-1';
import {enableDragScroll} from './drag-scroll.js?v=entry-lane-1';
import {addComment} from './comments.js?v=tokens-policy-1';
import {records,days,ringDetails,metadata,replaceData,scopeReady} from './schedule-data.js';
import {createDeviceData,trainerOptions,filterTrainer} from './device-data.js';
import {pingLocation} from './device-location.js';
import {ringPanel,detailPanel,icon,commentsList,dayLine} from './components.js?v=group-subline-1';
const isDay=document.body.dataset.view==='day';
const requestedDay=new URLSearchParams(location.search).get('date');
const initialDay=days.some(d=>d.date===requestedDay)?requestedDay:days.some(d=>d.date===metadata.defaultDate)?metadata.defaultDate:days[0]?.date;
const state={trainer:new URLSearchParams(location.search).get('trainer')||'',day:initialDay,ring:null,horses:new Set(),selection:null,includeDone:false,focus:false,daySort:'time',dayDirection:'asc'};
const schedule=document.querySelector('#rings-schedule');
const surface=document.querySelector('#rings-surface');
const scroller=document.querySelector('.rings-scroll');
const ringActions=document.querySelector('.rings-actions');
const horseActions=document.querySelector('.rings-horse-actions');
document.querySelectorAll('.rings-actions:not(.v2-day-tools),.rings-horse-actions,.v2-page-nav').forEach(enableDragScroll);
const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const dateLabel=date=>new Intl.DateTimeFormat('en-US',{month:'short',day:'numeric',timeZone:'UTC'}).format(new Date(date+'T12:00:00Z'));
const daySelect=document.querySelector('.v2-schedule-day');
const trainerSelect=document.createElement('select');
trainerSelect.className='v2-schedule-day';trainerSelect.setAttribute('aria-label','Trainer');
// Both header dropdowns share the same icon control and native selection behavior.
const headerFilters=document.createElement('div');
headerFilters.className='v2-header-filters';
daySelect.before(headerFilters);
const headerFilter=headerDropdowns(headerFilters);
const dayControl=headerFilter(daySelect,'<svg class="rings-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="5" width="18" height="16" rx="2"/><path d="M16 3v4M8 3v4M3 11h18M8 15h3v3H8z"/></svg>');
const trainerControl=headerFilter(trainerSelect,icon('trainer'));
trainerControl.hidden=!document.body.hasAttribute('data-trainer-filter');
trainerSelect.addEventListener('change',()=>{state.trainer=trainerSelect.value;state.horses.clear();if(surface.open)surface.close();syncDay();renderSchedule();});
daySelect.innerHTML=days.map(d=>`<option value="${escape(d.date)}">${escape(dateLabel(d.date))}</option>`).join('');
daySelect.value=state.day;
let returnFocus=null;
let rings=[];
function syncDay(){
 if(!state.day)return;
 const options=trainerOptions(records,state.day);
 if(state.trainer&&!options.some(([id])=>id===state.trainer))state.trainer='';
 const url=new URL(location.href);url.searchParams.set('date',state.day);if(state.trainer)url.searchParams.set('trainer',state.trainer);else url.searchParams.delete('trainer');history.replaceState(null,'',url);
 document.querySelectorAll('a[href*="schedule.html"],a[href*="day.html"]').forEach(a=>{const target=new URL(a.getAttribute('href'),location.href);target.searchParams.set('date',state.day);if(state.trainer)target.searchParams.set('trainer',state.trainer);else target.searchParams.delete('trainer');a.href=target.href;});
 daySelect.innerHTML=days.map(d=>`<option value="${escape(d.date)}">${escape(dateLabel(d.date))}</option>`).join('');daySelect.value=state.day;
 trainerSelect.innerHTML='<option value="">'+(options.length?'All trainers':'No trainers for '+escape(dateLabel(state.day)))+'</option>'+options.map(([id,name])=>`<option value="${escape(id)}">${escape(name)}</option>`).join('');trainerSelect.value=state.trainer;
 const dateText=dateLabel(state.day);
 dayControl.title=daySelect.title='Schedule day: '+dateText;
 trainerControl.title=trainerSelect.title='Trainer: '+trainerSelect.selectedOptions[0].textContent;
 trainerControl.dataset.active=String(Boolean(state.trainer));
 schedule.setAttribute('aria-label','Schedule for '+dateText);
 if(isDay)document.querySelector('.v2-day-tools h1').textContent=state.day===new Intl.DateTimeFormat('en-CA',{timeZone:metadata.timezone,year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date())?'Today':dateText;
 const footer=document.querySelector('.rings-shell-footer');
 footer.textContent=(metadata.source==='wef-data'?'Airtable refreshed':'Airtable snapshot')+' · '+new Intl.DateTimeFormat('en-US',{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',timeZone:metadata.timezone,timeZoneName:'short'}).format(new Date(metadata.capturedAt))+(metadata.source==='wef-data'?'':' - not live');
 footer.title=metadata.show+' · '+metadata.capturedAt;
 const rows=filterTrainer(records.filter(r=>r.date===state.day&&(r.kind!=='class'||r.entryCount!==0)),state.trainer);
 rings=days.find(d=>d.date===state.day)?.rings||[];
 if(!isDay)ringActions.innerHTML=rings.map(r=>`<button class="rings-pill" type="button" data-ring="${r}" aria-pressed="false">${icon('ring')}${escape(ringDetails[r].name)}</button>`).join('');

 const horses=new Map(rows.flatMap(r=>r.entries.map(e=>[e.horseId,e.name])));
 horseActions.innerHTML=[...horses].map(([id,name])=>`<button class="rings-pill" type="button" data-horse="${escape(id)}" aria-pressed="false">${icon('horse')}${escape(name)}</button>`).join('');
}
daySelect.addEventListener('change',()=>{
 state.day=daySelect.value;state.ring=null;state.horses.clear();horseActions.hidden=true;
 if(surface.open)surface.close();
 syncDay();renderSchedule();scroller.scrollTo({top:0,behavior:'instant'});
});
let entryLaneCleanup=[];
function renderSchedule(){
 entryLaneCleanup.forEach(cleanup=>cleanup());entryLaneCleanup=[];
 const dayRecords=filterTrainer(records.filter(r=>r.date===state.day&&(r.kind!=='class'||r.entryCount!==0)),state.trainer);
 if(isDay){
  const rows=dayRows(dayRecords,{includeDone:state.includeDone,focus:state.focus,sort:state.daySort,direction:state.dayDirection});
  schedule.innerHTML='<section class="rings-panel v2-day-list" aria-label="Classes in day order"><div class="v2-day-head">'+daySortHead('time','TIME')+daySortHead('ring','RING')+'<span>NO</span><span>CLASS</span><span>ENTRIES</span></div>'+rows.map(dayLine).join('')+(rows.length?'':'<p class="rings-detail-note">No classes match this view.</p>')+'</section>';
  schedule.style.paddingBottom='0px';return;
 }
 const visible=state.horses.size?dayRecords.map(row=>({...row,entries:row.entries.filter(e=>state.horses.has(e.horseId))})).filter(row=>row.entries.length):dayRecords;
 schedule.innerHTML=rings.filter(r=>(!state.horses.size&&!state.trainer)||visible.some(row=>row.ring===r)).map(r=>`<div id="ring-${r}" class="rings-anchor">${ringPanel(r,visible.filter(row=>row.ring===r))}</div>`).join('')||'<p class="rings-detail-note" role="status">No schedule rows match this view.</p>';
 entryLaneCleanup=[...schedule.querySelectorAll('.rings-entry-rollups')].map(enableDragScroll);
 if(state.ring!==null&&!visible.some(r=>String(r.ring)===state.ring))state.ring=null;
 ringActions.querySelectorAll('[data-ring]').forEach(b=>{b.setAttribute('aria-pressed',String(b.dataset.ring===state.ring));b.disabled=false;});
 horseActions.querySelectorAll('[data-horse]').forEach(b=>b.setAttribute('aria-pressed',String(state.horses.has(b.dataset.horse))));
 requestAnimationFrame(sizeAnchorSpace);
}
function selectRecord(rowKey,entryKey,trigger){
 const row=filterTrainer(records,state.trainer).find(row=>row.key===rowKey);
 if(!row||row.date!==state.day||row.kind!=='class')return;
 const entry=entryKey?row.entries.find(entry=>entry.key===entryKey):null;
 if(entryKey&&!entry)return;
 state.selection={rowKey,entryKey:entry?.key??null};
 if(!surface.open)returnFocus=trigger;
 surface.dataset.mode=entry?'entry':'class';
 document.querySelector('#rings-surface-context').innerHTML=`${icon('ring')}<span>${escape(ringDetails[row.ring].name)}</span>`;
 document.querySelector('#rings-surface-title').textContent=entry?'Entry details':'Class details';
 const trainer=document.querySelector('#rings-surface-trainer');trainer.hidden=!entry?.trainer;trainer.innerHTML=entry?'<span>Trainer:</span>'+icon('trainer')+'<span>'+escape(entry.trainer??'—')+'</span>':'';
 document.querySelector('#rings-surface-content').innerHTML=detailPanel(row,entry);
 surface.querySelectorAll('[data-detail-toggle]').forEach(button=>detailTargets(button).forEach(block=>{block.hidden=true;}));
 syncDetailToggles();
 if(!surface.open)surface.showModal();
 document.querySelector('#rings-surface-title').focus({preventScroll:true});
 if(trigger?.dataset.focusResults==='true'){
  const result=document.querySelector('#v2-entry-results');
  const rider=result?.closest('details');if(rider){rider.hidden=false;rider.open=true;}
  syncDetailToggles();
  result?.scrollIntoView({block:'nearest'});
  result?.focus({preventScroll:true});
 }
}
function onRecordClick(event){
 const trigger=event.target.closest('button[data-row-key]');
 if(trigger)selectRecord(trigger.dataset.rowKey,trigger.dataset.entryKey,trigger);
}
schedule.addEventListener('click',onRecordClick);
surface.addEventListener('click',event=>{
 if(event.target===surface){const box=surface.getBoundingClientRect();if(event.clientX<box.left||event.clientX>box.right||event.clientY<box.top||event.clientY>box.bottom)surface.close();return;}
 onRecordClick(event);
});
document.querySelector('#rings-close').addEventListener('click',()=>surface.close());
surface.addEventListener('close',()=>{state.selection=null;if(returnFocus?.isConnected)returnFocus.focus({preventScroll:true});returnFocus=null;});
ringActions.addEventListener('click',event=>{
 const b=event.target.closest('[data-ring]');if(!b)return;
 const reset=state.ring===b.dataset.ring;
 state.ring=reset?null:b.dataset.ring;
 // Ring controls always navigate the full schedule; horse filters are independent.
 state.horses.clear();state.selection=null;
 if(surface.open)surface.close();
 renderSchedule();
 if(reset){
  horseActions.hidden=true;horseActions.scrollLeft=0;ringActions.scrollLeft=0;
  scroller.scrollTo({top:0,behavior:'instant'});return;
 }
 const target=document.getElementById(`ring-${state.ring}`);
 if(target){const y=scroller.scrollTop+target.getBoundingClientRect().top-scroller.getBoundingClientRect().top-ringActions.getBoundingClientRect().height;scroller.scrollTo({top:Math.max(0,y),behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'});}
});
horseActions.addEventListener('click',event=>{const b=event.target.closest('[data-horse]');if(!b)return;const name=b.dataset.horse;if(state.horses.has(name))state.horses.delete(name);else state.horses.add(name);renderSchedule();});
scroller.addEventListener('scroll',()=>{if(isDay)return;if(scroller.scrollTop>8)horseActions.hidden=false;else if(!state.horses.size)horseActions.hidden=true;},{passive:true});
syncDay();renderSchedule();
function sizeAnchorSpace(){if(isDay)return;const last=schedule.lastElementChild;schedule.style.paddingBottom=last?`${Math.max(0,scroller.clientHeight-ringActions.offsetHeight-last.offsetHeight)}px`:'0px';}
new ResizeObserver(sizeAnchorSpace).observe(scroller);
surface.addEventListener('click',event=>{
 const area=event.target.closest('.v2-comments');if(!area)return;
 const form=area.querySelector('form');
 const reply=event.target.closest('[data-comment-reply]');
 if(reply||event.target.closest('[data-comment-add]')){
  form.hidden=false;form.dataset.parentId=reply?.dataset.commentReply||'';
  form.querySelector('label').textContent=reply?'Reply':'Add comment';
  form.querySelector('[type="submit"]').textContent=reply?'Post reply':'Add comment';
  form.querySelector('textarea').focus();
 }
 if(event.target.closest('[data-comment-cancel]')){form.reset();form.hidden=true;}
});
surface.addEventListener('submit',event=>{
 if(!event.target.matches('.v2-comment-form'))return;event.preventDefault();
 const form=event.target,area=form.closest('.v2-comments');
 const row=records.find(r=>r.key===area.dataset.classComments);if(!row)return;
 if(!addComment(row.key,form.elements.comment.value,form.dataset.parentId||null))return;
 area.outerHTML=commentsList(row);
 const updated=surface.querySelector('.v2-comments');updated.open=true;
 updated.querySelector('[data-comment-add]').focus();
 // Update class header/count and schedule without replacing other open accordions.
 const header=surface.querySelector('.rings-detail-panel > .v2-class:not(.v2-thead)');
 if(header){const temp=document.createElement('div');temp.innerHTML=detailPanel(row);header.replaceWith(temp.querySelector('.v2-class:not(.v2-thead)'));}
 syncDetailToggles();
 renderSchedule();
});
function detailTargets(button){
 return button.dataset.detailToggle==='entry'
  ? [...surface.querySelectorAll('.v2-entry > details.v2-collapse')]
  : [...surface.querySelectorAll('.rings-detail-panel > .v2-group-list')];
}
function syncDetailToggles(){
 surface.querySelectorAll('[data-detail-toggle]').forEach(button=>{
  const targets=detailTargets(button), expanded=targets.length>0&&targets.every(d=>!d.hidden);
  button.textContent=expanded?'−':'+';button.setAttribute('aria-expanded',String(expanded));
  button.setAttribute('aria-label',(expanded?'Hide ':'Show ')+(button.dataset.detailToggle==='entry'?'horse, rider and groom':'group'));
 });
}
surface.addEventListener('click',event=>{
 const button=event.target.closest('[data-detail-toggle]');if(!button)return;
 const targets=detailTargets(button),expand=!targets.every(d=>!d.hidden);
 targets.forEach(d=>{d.hidden=!expand;});syncDetailToggles();
});
function daySortHead(key,label){
 const active=state.daySort===key;
 const direction=active?state.dayDirection:null;
 return '<button type="button" class="v2-sort-head" data-day-sort="'+key+'" data-active="'+active+'" aria-label="'+label+': '+(direction||'not sorted')+'. Sort '+(direction==='asc'?'descending':'ascending')+'">'+label+'<span aria-hidden="true">'+(direction==='asc'?'↑':direction==='desc'?'↓':'↕')+'</span></button>';
}
if(isDay){
 document.querySelector('[data-day-focus]').addEventListener('click',event=>{state.focus=!state.focus;event.currentTarget.setAttribute('aria-pressed',String(state.focus));renderSchedule();});
 document.querySelector('[data-day-done]').addEventListener('click',event=>{
  state.includeDone=!state.includeDone;
  event.currentTarget.setAttribute('aria-pressed',String(state.includeDone));
  event.currentTarget.title=state.includeDone?'Done included; click to hide done':'Done hidden; click to include done';
  renderSchedule();
 });
 schedule.addEventListener('click',event=>{
  const button=event.target.closest('[data-day-sort]');if(!button)return;
  const key=button.dataset.daySort;
  state.dayDirection=state.daySort===key&&state.dayDirection==='asc'?'desc':'asc';
  state.daySort=key;renderSchedule();
  schedule.querySelector('[data-day-sort="'+key+'"]').focus();
 });
}
schedule.addEventListener('click',event=>{
 const lane=event.target.closest('.rings-entry-line');if(!lane)return;
 const expand=event.target.closest('.v2-entry-expand');
 if(expand){const open=lane.classList.toggle('is-expanded');expand.textContent=open?'−':'+';expand.setAttribute('aria-expanded',String(open));expand.setAttribute('aria-label',expand.getAttribute('aria-label').replace(open?'Expand':'Collapse',open?'Collapse':'Expand'));}
 const search=event.target.closest('.v2-entry-search-toggle');
 if(search){const input=lane.querySelector('input');input.hidden=!input.hidden;search.setAttribute('aria-expanded',String(!input.hidden));if(!input.hidden)input.focus();else{input.value='';filterEntryLane(input);}}
});
function filterEntryLane(input){
 const lane=input.closest('.rings-entry-line'),query=input.value.trim().toLocaleLowerCase();let matches=0;
 lane.querySelectorAll('.rings-entry-rollup').forEach(pill=>{pill.hidden=!pill.querySelector('.rings-entry-name').textContent.toLocaleLowerCase().includes(query);if(!pill.hidden)matches++;});
 lane.querySelector('.v2-entry-empty').hidden=matches>0;
 lane.querySelector('.rings-entry-rollups').scrollLeft=0;
}
schedule.addEventListener('input',event=>{if(event.target.matches('.v2-entry-search'))filterEntryLane(event.target);});
// The icon-position control reveals only the mobile metadata line.
document.addEventListener('click',event=>{
 const toggle=event.target.closest('.v2-meta-toggle,.v2-entry-meta-toggle');if(!toggle)return;
 const row=toggle.closest('.v2-class,.v2-day-row,.v2-entry-summary');
 const expanded=row.classList.toggle('is-meta-open');
 toggle.textContent=expanded?'\u2212':'+';
 toggle.setAttribute('aria-expanded',String(expanded));
 toggle.setAttribute('aria-label',toggle.getAttribute('aria-label').replace(expanded?'Show':'Hide',expanded?'Hide':'Show'));
});
// Same-origin Astro refresh; device packages are separate from Profile storage.
const endpoint=document.body.dataset.scheduleEndpoint;
let pendingData=null;
function applyDeviceData(data){
 if(surface.open||document.activeElement?.matches('input,textarea')){pendingData=data;return;}
 pendingData=null;
 const top=scroller.scrollTop;replaceData(data);
 if(!days.some(d=>d.date===state.day))state.day=metadata.defaultDate||days[0]?.date;
 if(!state.day)return;
 syncDay();renderSchedule();scroller.scrollTop=top;
 document.querySelector('.rings-shell-footer').textContent='Updated '+new Intl.DateTimeFormat('en-US',{hour:'numeric',minute:'2-digit',timeZone:metadata.timezone}).format(new Date(metadata.refreshedAt||metadata.capturedAt));
}
if(endpoint)scopeReady.then(scope=>{
 if(!scope?.showId)return;
 trainerControl.hidden=false;
 const dataEndpoint=new URL(endpoint,location.href).href;
 const device=createDeviceData({showId:scope.showId,endpoint:dataEndpoint,seed:metadata.showId===scope.showId&&records.length?{records,days,ringDetails,metadata}:null,onData:applyDeviceData,onError:()=>{document.querySelector('.rings-shell-footer').textContent='Refresh unavailable - showing last loaded data';},onChecked:(data,lane)=>{document.querySelector('.rings-shell-footer').textContent=(lane==='day0'?'Day-0':'Full schedule')+' checked '+new Intl.DateTimeFormat('en-US',{hour:'numeric',minute:'2-digit',timeZone:data.metadata.timezone}).format(new Date());}});
 // Only the same Astro application's approximate GeoIP ping; no GPS or persistence.
 void pingLocation(new URL('geo',dataEndpoint).href);
 let stop=device.start();
 window.addEventListener('pagehide',()=>stop());
 window.addEventListener('pageshow',event=>{if(event.persisted)stop=device.start();});
 document.addEventListener('focusout',()=>{if(pendingData&&!surface.open){const next=pendingData;pendingData=null;applyDeviceData(next);}});
 surface.addEventListener('close',()=>{if(pendingData){const next=pendingData;pendingData=null;applyDeviceData(next);}});
 document.addEventListener('visibilitychange',()=>{if(!document.hidden)void device.refresh();});
});
