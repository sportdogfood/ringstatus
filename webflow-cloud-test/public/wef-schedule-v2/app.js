import './shell.js';
import {recognitionAdapter} from '/test/rs-recognition/widget.js';
import {createHorseEdits} from './horse-edits.js';
import {inlineNickname,horseForm} from './horse-forms.js';
import {openShellDialog} from './shell-dialog.js';
import {v3Button,v3Control} from '../rs-profile-v3/components.js';
import {icon as v3Icon} from '../rs-profile-v3/icons.js';
import {headerDropdowns} from './header-dropdown.js';
import {dayRows} from './day.js?v=source-order-1';
import {enableDragScroll} from './drag-scroll.js?v=entry-lane-1';
import {addComment} from './comments.js?v=tokens-policy-1';
import {records,days,ringDetails,metadata,replaceData,selectRingDetails,scopeReady} from './schedule-data.js';
import {createDeviceData,trainerOptions,filterTrainer,CWF_TEAM,teamRollups} from './device-data.js';
import {publishedSchedule} from './publication.js';
import {pingLocation} from './device-location.js';
import {ringPanel,detailPanel,icon,commentsList,dayLine} from './components.js?v=group-subline-1';
let isDay=new URLSearchParams(location.search).has('view')?new URLSearchParams(location.search).get('view')==='day':document.body.dataset.view==='day';
const requestedDay=new URLSearchParams(location.search).get('date');
const publicationOptions={showBreaks:new URLSearchParams(location.search).get('showBreaks')==='1',showOptionalClasses:new URLSearchParams(location.search).get('showOptionalClasses')==='1'};
const todayDate=()=>new Intl.DateTimeFormat('en-CA',{timeZone:metadata.timezone,year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
const initialDay=days.some(d=>d.date===requestedDay)||requestedDay===todayDate()?requestedDay:todayDate();
const newListState=()=>({trainer:'',team:'',day:todayDate(),ring:null,horses:new Set(),selection:null,dayStatus:'',daySort:'time',dayDirection:'asc'});
const listStates={schedule:newListState(),day:newListState()};
let state=listStates[isDay?'day':'schedule'];
Object.assign(state,{trainer:new URLSearchParams(location.search).get('trainer')||'',team:new URLSearchParams(location.search).get('team')===CWF_TEAM.id?CWF_TEAM.id:'',day:initialDay});
const horseEdits=createHorseEdits({endpoint:new URL('../wef-schedule/input',location.href).href,onChange:refreshHorseDisplay,onStatus:showHorseStatus});
const shownRecords=()=>horseEdits.apply(records);
let horseEditor=null;
const schedule=document.querySelector('#rings-schedule');
const surface=document.querySelector('#rings-surface');
const scroller=document.querySelector('.rings-scroll');
const ringActions=document.querySelector('.rings-actions');
const horseActions=document.querySelector('.rings-horse-actions');
document.querySelectorAll('.rings-actions,.rings-horse-actions,.v2-page-nav').forEach(enableDragScroll);
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
const teamSelect=document.createElement('select');teamSelect.setAttribute('aria-label','Team');
teamSelect.innerHTML='<option value="">Select team</option><option value="'+CWF_TEAM.id+'">'+CWF_TEAM.name+'</option>';
const teamControl=headerFilter(teamSelect,v3Icon('users'));
teamSelect.addEventListener('change',()=>{state.team=teamSelect.value;state.horses.clear();horseActions.hidden=true;if(surface.open)surface.close();syncDay();renderSchedule();});
const pageNav=document.querySelector('.v2-page-nav');
const todayLink=pageNav.querySelector('a[href="day.html"]');
const timelinesControl=document.createElement('button');timelinesControl.type='button';timelinesControl.className='rings-pill';timelinesControl.dataset.listView='day';timelinesControl.innerHTML=v3Icon('calendar')+'<span>Timelines</span>';v3Control(timelinesControl);todayLink.replaceWith(timelinesControl);
pageNav.querySelector('a').dataset.listView='schedule';
for(const [label,glyph] of [['Horses','horse'],['Results','list'],['Notifications','bell'],['Comments','comment']]){
 const button=document.createElement('button');button.type='button';button.disabled=true;button.innerHTML=v3Icon(glyph)+'<span>'+label+'</span>';button.classList.add('rings-pill');v3Control(button);pageNav.append(button);
}
// Reuse the existing Profile navigation, including the parent-owned overlay return.
const profileControl=document.createElement('button');profileControl.type='button';profileControl.className='rings-pill schedule-profile-control';profileControl.setAttribute('aria-label','Profile');profileControl.title='Profile';profileControl.innerHTML=v3Icon('user');v3Control(profileControl);
profileControl.addEventListener('click',()=>document.querySelector('.back-row a.back-link')?.click());
pageNav.prepend(profileControl);
trainerControl.hidden=true;
trainerSelect.addEventListener('change',()=>{state.trainer=trainerSelect.value;state.horses.clear();if(surface.open)surface.close();syncDay();renderSchedule();});
daySelect.innerHTML=days.map(d=>`<option value="${escape(d.date)}">${escape(dateLabel(d.date))}</option>`).join('');
daySelect.value=state.day;
let returnFocus=null;
let rings=[],ringAnchorSignature='';
const viewScroll={schedule:0,day:0};
function configureView(){
 document.body.dataset.view=isDay?'day':'schedule';
 ringActions.classList.toggle('v2-day-tools',isDay);
 ringActions.setAttribute('aria-label',isDay?'Day status':'Jump to ring');
 ringActions.innerHTML=isDay?'<h1>Timelines</h1>'+[['done','DONE'],['upc','UPCM'],['live','LIVE']].map(([value,label])=>v3Button('day-status',label,{extra:`data-day-status="${value}" aria-pressed="${state.dayStatus===value}"`}).replace('class="ui-button"','class="ui-button rings-pill" data-v3-control="action"')).join(''):'';
 ringAnchorSignature='';
 const scheduleLink=pageNav.querySelector('a');
 if(isDay){timelinesControl.setAttribute('aria-current','page');scheduleLink.removeAttribute('aria-current');}
 else{scheduleLink.setAttribute('aria-current','page');timelinesControl.removeAttribute('aria-current');}
 horseActions.hidden=isDay||!state.horses.size;
}
function switchView(day){
 viewScroll[isDay?'day':'schedule']=scroller.scrollTop;
 if(surface.open)surface.close();
 isDay=day;state=listStates[isDay?'day':'schedule'];configureView();syncDay();renderSchedule();
 scroller.scrollTo({top:viewScroll[isDay?'day':'schedule'],behavior:'instant'});
}
pageNav.addEventListener('click',event=>{
 const link=event.target.closest('[data-list-view]');
 if(!link||event.button!==0||event.ctrlKey||event.metaKey||event.shiftKey||event.altKey)return;
 event.preventDefault();const day=link.dataset.listView==='day';if(isDay!==day)switchView(day);
});

function syncDay(){
 if(!state.day)return;
 selectRingDetails(state.day);
 const options=trainerOptions(records,state.day);
 trainerControl.hidden=!document.body.hasAttribute('data-trainer-filter')||options.length===0;
 if(state.trainer&&!options.some(([id])=>id===state.trainer))state.trainer='';
 const url=new URL(location.href);url.searchParams.set('view',isDay?'day':'schedule');url.searchParams.set('date',state.day);if(state.trainer)url.searchParams.set('trainer',state.trainer);else url.searchParams.delete('trainer');
 if(state.team)url.searchParams.set('team',state.team);else url.searchParams.delete('team');history.replaceState(null,'',url);
 document.querySelectorAll('a[href*="schedule.html"],a[href*="day.html"]').forEach(a=>{const linkState=listStates[a.dataset.listView]||state;const target=new URL(a.getAttribute('href'),location.href);target.searchParams.set('date',linkState.day);if(linkState.trainer)target.searchParams.set('trainer',linkState.trainer);else target.searchParams.delete('trainer');if(linkState.team)target.searchParams.set('team',linkState.team);else target.searchParams.delete('team');a.href=target.href;});
 const dates=[...new Set([todayDate(),state.day,...days.map(d=>d.date)])].sort();
 daySelect.innerHTML=dates.map(date=>`<option value="${escape(date)}">${date===todayDate()?'Today':escape(dateLabel(date))}</option>`).join('');daySelect.value=state.day;
 trainerSelect.innerHTML='<option value="">'+(options.length?'Select trainer':'No trainers for '+escape(dateLabel(state.day)))+'</option>'+options.map(([id,name])=>`<option value="${escape(id)}">${escape(name)}</option>`).join('');trainerSelect.value=state.trainer;
 const dateText=dateLabel(state.day);
 trainerSelect.dataset.dayLabel=dateText;
 dayControl.title=daySelect.title='Schedule day: '+dateText;
 trainerControl.title=trainerSelect.title='Trainer: '+trainerSelect.selectedOptions[0].textContent;
 trainerControl.dataset.active=String(Boolean(state.trainer));
 teamSelect.value=state.team;teamSelect.dataset.dayLabel=dateText;
 teamControl.title=teamSelect.title=state.team?'Team: CWF':'Select team';teamControl.dataset.active=String(Boolean(state.team));
 schedule.setAttribute('aria-label','Schedule for '+dateText);
 if(isDay)document.querySelector('.v2-day-tools h1').textContent=state.day===todayDate()?'Timelines':dateText;
 const footer=document.querySelector('.rings-shell-footer');
 const feedSource=['wef-data','wef-schedule-app'].includes(metadata.source);
 showFeedStatus(Number.isFinite(Date.parse(metadata.capturedAt))?(feedSource?'Airtable refreshed':'Airtable snapshot')+' · '+new Intl.DateTimeFormat('en-US',{month:'short',day:'numeric',hour:'numeric',minute:'2-digit',timeZone:metadata.timezone,timeZoneName:'short'}).format(new Date(metadata.capturedAt))+(feedSource?'':' - not live'):'Loading schedule');
 footer.title=metadata.show+' · '+metadata.capturedAt;
 const rows=filterTrainer(publishedSchedule(shownRecords().filter(r=>r.date===state.day),publicationOptions),state.trainer);
 rings=days.find(d=>d.date===state.day)?.rings||[];

 const horses=new Map(teamRollups(rows,state.team).flatMap(r=>r.entries.map(e=>[e.horseId,e.name])));
 horseActions.innerHTML=[...horses].map(([id,name])=>v3Button('horse-filter',icon('horse')+escape(name),{extra:`data-horse="${escape(id)}" aria-pressed="false"`}).replace('class="ui-button"','class="ui-button rings-pill" data-v3-control="action"')).join('');
}
daySelect.addEventListener('change',()=>{
 state.day=daySelect.value;state.ring=null;state.horses.clear();horseActions.hidden=true;
 if(surface.open)surface.close();
 syncDay();renderSchedule();scroller.scrollTo({top:0,behavior:'instant'});
});
let entryLaneCleanup=[];
function renderSchedule(){
 entryLaneCleanup.forEach(cleanup=>cleanup());entryLaneCleanup=[];
 const dayRecords=filterTrainer(publishedSchedule(shownRecords().filter(r=>r.date===state.day),publicationOptions),state.trainer);
 if(isDay){
  const rows=dayRows(dayRecords,{status:state.dayStatus,sort:state.daySort,direction:state.dayDirection});
  schedule.innerHTML='<section class="rings-panel v2-day-list" aria-label="Classes in day order"><div class="v2-day-head">'+daySortHead('time','TIME')+daySortHead('ring','RING')+'<span>NO</span><span>CLASS</span><span>STATUS</span><span>ENTRIES</span></div>'+rows.map(dayLine).join('')+(rows.length?'':'<p class="rings-detail-note">No classes match this view.</p>')+'</section>';
  schedule.style.paddingBottom='0px';return;
 }
 const visible=state.horses.size?dayRecords.map(row=>({...row,entries:row.entries.filter(e=>state.horses.has(e.horseId))})).filter(row=>row.entries.length):dayRecords;
 const relevantRings=rings.filter(r=>visible.some(row=>row.ring===r));
 const signature=JSON.stringify(relevantRings.map(r=>[r,ringDetails[r].name]));
 if(signature!==ringAnchorSignature){
  const left=ringActions.scrollLeft;
  ringActions.innerHTML=relevantRings.map(r=>v3Button('ring-anchor',icon('ring')+escape(ringDetails[r].name),{extra:`data-ring="${r}" aria-pressed="false"`}).replace('class="ui-button"','class="ui-button rings-pill" data-v3-control="action"')).join('');
  ringActions.scrollLeft=left;ringAnchorSignature=signature;
 }
 schedule.innerHTML=rings.filter(r=>(!state.horses.size&&!state.trainer)||visible.some(row=>row.ring===r)).map(r=>`<div id="ring-${r}" class="rings-anchor">${ringPanel(r,teamRollups(visible.filter(row=>row.ring===r),state.team))}</div>`).join('')||'<p class="rings-detail-note" role="status">No schedule rows match this view.</p>';
 entryLaneCleanup=[...schedule.querySelectorAll('.rings-entry-rollups')].map(enableDragScroll);
 if(state.ring!==null&&!visible.some(r=>String(r.ring)===state.ring))state.ring=null;
 ringActions.querySelectorAll('[data-ring]').forEach(b=>{b.setAttribute('aria-pressed',String(b.dataset.ring===state.ring));b.disabled=false;});
 horseActions.querySelectorAll('[data-horse]').forEach(b=>b.setAttribute('aria-pressed',String(state.horses.has(b.dataset.horse))));
}
function selectRecord(rowKey,entryKey,trigger){
 const row=filterTrainer(shownRecords(),state.trainer).find(row=>row.key===rowKey);
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
 if(!surface.open)openShellDialog(surface);
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
 const edit=event.target.closest('[data-edit-horse]');if(edit){openInlineNickname(edit);return;}
 const trigger=event.target.closest('button[data-row-key]');
 if(trigger&&surface.contains(trigger)&&surface.dataset.mode==='entry'&&trigger.classList.contains('v2-entry-name-link')){openHorseEditor(trigger);return;}
 if(trigger)selectRecord(trigger.dataset.rowKey,trigger.dataset.entryKey,trigger);
}
schedule.addEventListener('click',onRecordClick);
surface.addEventListener('click',event=>{
 if(event.target===surface){const box=surface.getBoundingClientRect();if(event.clientX<box.left||event.clientX>box.right||event.clientY<box.top||event.clientY>box.bottom)surface.close();return;}
 onRecordClick(event);
});
document.querySelector('#rings-close').addEventListener('click',()=>surface.close());
surface.addEventListener('close',()=>{horseEditor=null;state.selection=null;if(returnFocus?.isConnected)returnFocus.focus({preventScroll:true});returnFocus=null;});
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
 if(target){const scale=scroller.getBoundingClientRect().height/scroller.offsetHeight||1;const anchorHeight=scroller.contains(ringActions)?ringActions.getBoundingClientRect().height:0;const y=scroller.scrollTop+(target.getBoundingClientRect().top-scroller.getBoundingClientRect().top-anchorHeight)/scale;scroller.scrollTo({top:Math.max(0,y),behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'});}
});
horseActions.addEventListener('click',event=>{const b=event.target.closest('[data-horse]');if(!b)return;const name=b.dataset.horse;if(state.horses.has(name))state.horses.delete(name);else state.horses.add(name);renderSchedule();});
scroller.addEventListener('scroll',()=>{if(isDay)return;if(scroller.scrollTop>8)horseActions.hidden=false;else if(!state.horses.size)horseActions.hidden=true;},{passive:true});
configureView();syncDay();renderSchedule();
let readinessPending=false;
function markScheduleReady(){
 if(readinessPending)return;readinessPending=true;
 const fonts=document.fonts?Promise.all([400,500,600,700].map(weight=>document.fonts.load(`${weight} 15px Outfit`))):Promise.resolve();
 fonts.catch(()=>{}).then(()=>{document.documentElement.dataset.scheduleReady='true';document.documentElement.removeAttribute('data-schedule-loading');document.dispatchEvent(new Event('rs-schedule-ready'));});
}
if(records.length)markScheduleReady();
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
ringActions.addEventListener('click',event=>{
 const button=event.target.closest('[data-day-status]');if(!isDay||!button)return;
 state.dayStatus=state.dayStatus===button.dataset.dayStatus?'':button.dataset.dayStatus;
 ringActions.querySelectorAll('[data-day-status]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.dayStatus===state.dayStatus)));
 renderSchedule();
});
schedule.addEventListener('click',event=>{
 const button=event.target.closest('[data-day-sort]');if(!isDay||!button)return;
 const key=button.dataset.daySort;
 state.dayDirection=state.daySort===key&&state.dayDirection==='asc'?'desc':'asc';
 state.daySort=key;renderSchedule();schedule.querySelector('[data-day-sort="'+key+'"]').focus();
});
schedule.addEventListener('click',event=>{
 const lane=event.target.closest('.rings-entry-line');if(!lane)return;
 const expand=event.target.closest('.v2-entry-expand');
 if(expand){const open=lane.classList.toggle('is-expanded');expand.textContent=open?'−':'+';expand.setAttribute('aria-expanded',String(open));expand.setAttribute('aria-label',expand.getAttribute('aria-label').replace(open?'Expand':'Collapse',open?'Collapse':'Expand'));}
 const search=event.target.closest('.v2-entry-search-toggle');
 if(search){const input=lane.querySelector('input');input.hidden=!input.hidden;search.setAttribute('aria-expanded',String(!input.hidden));if(!input.hidden)input.focus();else{input.value='';filterEntryLane(input);}}
});
function filterEntryLane(input){
 const lane=input.closest('.rings-entry-line'),query=input.value.trim().toLocaleLowerCase();let matches=0;
 lane.querySelectorAll('.rings-entry-rollup').forEach(pill=>{pill.hidden=!pill.querySelector('.rings-entry-name').textContent.toLocaleLowerCase().includes(query);if(pill.closest('.v2-entry-item'))pill.closest('.v2-entry-item').hidden=pill.hidden;if(!pill.hidden)matches++;});
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
// Horse inputs share Profile's identity, local-first save and quiet Astro confirmation semantics.
function paintFooter(){const footer=document.querySelector('.rings-shell-footer');footer.textContent=[footer.dataset.inputStatus,footer.dataset.feedStatus].filter(Boolean).join(' · ');}
function showFeedStatus(text){document.querySelector('.rings-shell-footer').dataset.feedStatus=text;paintFooter();}
function showHorseStatus(text){document.querySelector('.rings-shell-footer').dataset.inputStatus=text;paintFooter();}
function mayEditHorse(){
 if(horseEdits.ready)return true;const message='Sign in through Profile to edit horses.';showHorseStatus(message);
 if(surface.open){const content=document.querySelector('#rings-surface-content');let notice=content.querySelector('[data-horse-signin]');if(!notice){notice=document.createElement('p');notice.className='feedback';notice.dataset.horseSignin='';notice.setAttribute('role','status');content.prepend(notice);}notice.textContent=message;}
 return false;
}
function horseFromTrigger(trigger){const row=shownRecords().find(r=>r.key===trigger.dataset.rowKey);return {row,horse:row?.entries.find(e=>e.key===trigger.dataset.entryKey)};}
function refreshHorseDisplay(){
 if(horseEditor&&!horseEdits.ready){horseEditor=null;if(surface.open)surface.close();}
 syncDay();renderSchedule();
 if(surface.open&&surface.dataset.mode==='entry'&&state.selection&&!horseEditor)selectRecord(state.selection.rowKey,state.selection.entryKey);
}
function openInlineNickname(trigger){
 if(!mayEditHorse())return;
 const {horse}=horseFromTrigger(trigger);if(!horse)return;
 const item=trigger.closest('.v2-entry-item');if(item.querySelector('form'))return;
 item.insertAdjacentHTML('beforeend',inlineNickname(horseEdits.fields(horse).nickname));
 const form=item.querySelector('form');form.dataset.horseId=horse.horseId;form.dataset.rowKey=trigger.dataset.rowKey;form.dataset.entryKey=trigger.dataset.entryKey;
 item.querySelectorAll(':scope>button').forEach(b=>b.hidden=true);form.querySelector('input').focus();form.querySelector('input').select();
}
schedule.addEventListener('click',event=>{const cancel=event.target.closest('[data-cancel-horse]');if(!cancel)return;const item=cancel.closest('.v2-entry-item');item.querySelector('form').remove();item.querySelectorAll(':scope>button').forEach(b=>b.hidden=false);});
schedule.addEventListener('submit',event=>{
 const form=event.target;if(!form.matches('.v2-nickname-form'))return;event.preventDefault();const {horse}=horseFromTrigger(form);if(!horse)return;
 try{horseEdits.save(horse,metadata.showId,{nickname:form.elements.nickname.value});syncDay();renderSchedule();}catch(error){form.querySelector('[role=alert]').textContent=error.message;}
});
function openHorseEditor(trigger){
 const {row,horse}=horseFromTrigger(trigger);if(!row||!horse||!mayEditHorse())return;
 horseEditor={rowKey:row.key,entryKey:horse.key,horse};
 document.querySelector('#rings-surface-title').textContent='Edit horse';surface.dataset.mode='entry';
 document.querySelector('#rings-surface-content').innerHTML=horseForm(horseEdits.fields(horse));
 surface.querySelector('[name=nickname]').focus();
}
surface.addEventListener('click',event=>{
 if(!horseEditor)return;
 const option=event.target.closest('[data-horse-field]');
 if(option){const field=option.dataset.horseField;surface.querySelector('[name='+field+']').value=option.dataset.value;option.closest('.horse-choice-row').querySelectorAll('button').forEach(b=>b.setAttribute('aria-pressed',String(b===option)));}
 if(event.target.closest('[data-cancel-horse]')){const current=horseEditor;horseEditor=null;selectRecord(current.rowKey,current.entryKey);}
});
surface.addEventListener('submit',event=>{
 const form=event.target;if(!form.matches('.v2-horse-form')||!horseEditor)return;event.preventDefault();
 const current=horseEditor,fields={nickname:form.elements.nickname.value};for(const key of ['color','gender'])if(form.elements[key].value)fields[key]=form.elements[key].value;
 try{horseEdits.save(current.horse,metadata.showId,fields);horseEditor=null;selectRecord(current.rowKey,current.entryKey);}catch(error){form.querySelector('[role=alert]').textContent=error.message;}
});
let horseIdentityGeneration=0;
async function prepareHorseIdentity(){
 const check=++horseIdentityGeneration;horseEdits.setIdentity(null);
 // Reuse Profile's established returning-device association; Astro still authorizes every write.
 try{const identity=JSON.parse(localStorage.getItem('rs_recognition_identity_v1')||'null'),snapshot=identity?.personUid?JSON.parse(localStorage.getItem('rs_profile_snapshot_v1:'+identity.personUid)||'null'):null;
  if(!localStorage.getItem('rs_recognition_pending_signout_v1')&&identity?.expiresAt>Date.now()&&identity.deviceToken===localStorage.getItem('rs_recognition_device_token_v1')&&snapshot?.uid===identity.personUid&&snapshot.state?.identity?.currentId===identity.personUid&&snapshot.deviceFingerprint)horseEdits.setIdentity({uid:identity.personUid,fingerprint:snapshot.deviceFingerprint});
 }catch{}
 try{await recognitionAdapter.current();if(check!==horseIdentityGeneration)return;const identity=recognitionAdapter.profileIdentity();horseEdits.setIdentity(identity?{uid:identity.uid,fingerprint:recognitionAdapter.profileDeviceFingerprint()}:null);}catch{}
}
void prepareHorseIdentity();
addEventListener('storage',event=>{if(['rs_recognition_pending_signout_v1','rs_recognition_device_token_v1'].includes(event.key))void prepareHorseIdentity();});
addEventListener('online',()=>void horseEdits.flush());
// Same-origin Astro refresh; device packages are separate from Profile storage.
const endpoint=document.body.dataset.scheduleEndpoint;
let pendingData=null;
function applyDeviceData(data){
 if(surface.open||document.activeElement?.matches('input,textarea')){pendingData=data;return;}
 pendingData=null;
 const top=scroller.scrollTop;replaceData(data);
 if(!state.day)state.day=todayDate();
 if(!state.day)return;
 syncDay();renderSchedule();scroller.scrollTop=top;markScheduleReady();
 showFeedStatus('Updated '+new Intl.DateTimeFormat('en-US',{hour:'numeric',minute:'2-digit',timeZone:metadata.timezone}).format(new Date(metadata.refreshedAt||metadata.capturedAt)));
}
if(endpoint)scopeReady.then(scope=>{
 if(!scope?.showId){markScheduleReady();return;}
 const dataEndpoint=new URL(endpoint,location.href).href;
 const device=createDeviceData({showId:scope.showId,endpoint:dataEndpoint,seed:metadata.showId===scope.showId&&records.length?{records,days,ringDetails,metadata}:null,onData:applyDeviceData,onError:()=>{showFeedStatus('Refresh unavailable - showing last loaded data');markScheduleReady();},onChecked:(data,lane)=>{void horseEdits.flush();showFeedStatus((lane==='day0'?'Day-0':'Full schedule')+' checked '+new Intl.DateTimeFormat('en-US',{hour:'numeric',minute:'2-digit',timeZone:data.metadata.timezone}).format(new Date()));}});
 // Only the same Astro application's approximate GeoIP ping; no GPS or persistence.
 void pingLocation(new URL('geo',dataEndpoint).href);
 let stop=device.start();
 window.addEventListener('pagehide',()=>stop());
 window.addEventListener('pageshow',event=>{if(event.persisted)stop=device.start();});
 document.addEventListener('focusout',()=>{if(pendingData&&!surface.open){const next=pendingData;pendingData=null;applyDeviceData(next);}});
 surface.addEventListener('close',()=>{if(pendingData){const next=pendingData;pendingData=null;applyDeviceData(next);}});
 document.addEventListener('visibilitychange',()=>{if(!document.hidden)void device.refresh();});
});
