import {estimateWalk,withinRing} from './ring-distance.js';
import {v3Button,v3Brand,v3Back,v3Shell,v3Footer,v3Search} from './components.js';
import {smsAlerts as optinAlerts} from './optins/catalog.js';
import {key as optinKey,legacyKey as optinLegacyKey,fresh as freshOptins,decode as decodeOptins,validate as validateOptins} from './optins/model.js';
import {TAGS,queryUsers} from './model.js';
import {startProfileRefresh} from './refresh.js';
import * as service from '/test/rs-profile/service.js';
import {recognitionAdapter,mountRecognize} from '/test/rs-recognition/widget.js';
import {EMPTY,presentation,roster,membership,isOwner,canRead,profileFields,createConnection} from './connection.js';
import {icon,iconNames,iconMeta,svgSource,spriteSource} from './icons.js?v=mic-1';
const componentPage=location.pathname.includes('/component-library');
const libraryPage=location.pathname.includes('/icon-library')||componentPage;

const $=s=>document.querySelector(s),escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const launcherRoot=$('#widget'), mobileLauncher=matchMedia('(max-width:600px)');
// Keep touch/trackpad scrolling native; allow mouse dragging of destinations.
let launcherDrag=null,launcherDragged=false;
launcherRoot.addEventListener('pointerdown',event=>{
 launcherDragged=false;
 const strip=event.target.closest('.launcher-scroll');
 const vertical=strip&&getComputedStyle(strip).flexDirection.startsWith('column');
 if(event.pointerType!=='mouse'||event.button!==0||!strip||(vertical?strip.scrollHeight<=strip.clientHeight:strip.scrollWidth<=strip.clientWidth))return;
 launcherDrag={strip,id:event.pointerId,vertical,start:vertical?event.clientY:event.clientX,offset:vertical?strip.scrollTop:strip.scrollLeft};
});
launcherRoot.addEventListener('pointermove',event=>{
 const drag=launcherDrag;if(!drag||event.pointerId!==drag.id)return;
 const delta=(drag.vertical?event.clientY:event.clientX)-drag.start;if(!launcherDragged&&Math.abs(delta)<5)return;
 launcherDragged=true;launcherRoot.setPointerCapture(event.pointerId);event.preventDefault();
 const scale=drag.strip.getBoundingClientRect().width/drag.strip.offsetWidth;
 drag.strip[drag.vertical?'scrollTop':'scrollLeft']=drag.offset-delta/(scale||1);
});
function endLauncherDrag(event){if(launcherDrag?.id!==event.pointerId)return;launcherDrag=null;if(launcherRoot.hasPointerCapture(event.pointerId))launcherRoot.releasePointerCapture(event.pointerId);}
launcherRoot.addEventListener('pointerup',endLauncherDrag);
launcherRoot.addEventListener('pointercancel',endLauncherDrag);
launcherRoot.addEventListener('lostpointercapture',()=>{launcherDrag=null;});
launcherRoot.addEventListener('dragstart',event=>{if(event.target.closest('.launcher-scroll'))event.preventDefault();});
launcherRoot.addEventListener('click',event=>{if(launcherDragged){event.preventDefault();event.stopImmediatePropagation();launcherDragged=false;}},true);
let residentLauncherSlot=null;
function placeLauncher(){
 const panel=$('#app'),footer=panel.querySelector(':scope > .footer-row');
 // Reuse the rendered shared account control; Schedule keeps its Profile return action.
 const scheduleDocument=(scheduleFrame&&!scheduleFrame.hidden?scheduleFrame:hostSchedule?parent.document.getElementById('wef-schedule-v2'):null)?.contentDocument;
 const back=scheduleDocument?.querySelector('.back-row a.back-link');
 const account=state.recognized?panel.querySelector('.account-wrap [data-menu="account-menu"]'):null;
 if(back){
  const content=account?.innerHTML||'‹ Profile';
  if(back.innerHTML!==content)back.innerHTML=content;
  back.classList.toggle('ui-button',!!account);
  if(account)back.dataset.variant='quiet';else delete back.dataset.variant;
  back.setAttribute('aria-label',account?'Back to Profile: '+account.textContent.trim():'Back to Profile');
 }

 const scheduleDocked=mobileLauncher.matches&&((!!scheduleFrame&&!scheduleFrame.hidden)||(hostSchedule&&state.recognized&&panel.hidden));
 const embedded=!scheduleDocked&&!panel.hidden;
 document.body.dataset.scheduleLauncher=String(scheduleDocked);
 let slot=null;
 if(state.recognized){
  try{const frame=scheduleFrame&&!scheduleFrame.hidden?scheduleFrame:hostSchedule&&panel.hidden?parent.document.getElementById('wef-schedule-v2'):null;slot=frame?.contentDocument?.querySelector('.rings-shell > .schedule-launcher-slot');}catch{}
 }
 if(residentLauncherSlot&&residentLauncherSlot!==slot){residentLauncherSlot.hidden=true;}
 residentLauncherSlot=slot;
 // Schedule exposes only its Profile control; retain the launcher for Profile itself.
 if(slot)slot.hidden=true;
 recognizeRoot.hidden=!!slot||(hostSchedule&&panel.hidden);
 const destination=slot||(embedded?panel:$('#preview-stage'));
 if(launcherRoot.parentElement!==destination){destination.append(launcherRoot);}
 document.body.dataset.launcherEmbedded=String(embedded);
 document.body.dataset.appOpen=String(!panel.hidden||!!(scheduleFrame&&!scheduleFrame.hidden));
 launcherRoot.dataset.closed=String(panel.hidden&&!slot&&!(scheduleFrame&&!scheduleFrame.hidden));syncIdentityButton();
 if(scheduleFrame)scheduleFrame.style.height=scheduleDocked&&!slot?`${Math.max(0,innerHeight-launcherRoot.getBoundingClientRect().height)}px`:'100dvh';
}
mobileLauncher.addEventListener('change',placeLauncher);
launcherRoot.addEventListener('click',event=>{
 if(launcherRoot.ownerDocument===document)return;
 event.stopPropagation();
 const link=event.target.closest('a');
 if(link?.getAttribute('href')===scheduleHref){event.preventDefault();openSchedule(link);return;}
 void handleUIAction(event);handleDataAction(event);
});
addEventListener('message',event=>{if(hostSchedule&&event.source===parent&&event.origin===location.origin&&event.data?.type==='rs-host-schedule-ready')placeLauncher();});
const previewParams=new URLSearchParams(location.search);
const website=previewParams.get('website')==='1';document.body.dataset.website=String(website);
const hostSchedule=website&&parent!==window&&previewParams.get('host-schedule')==='1';
const scheduleHref='/test/wef-schedule-v2/schedule';
let scheduleFrame=null,scheduleReady=false,scheduleRequested=false,scheduleTrigger=null,scheduleBox=null,scheduleBoundsObserver=null;
function syncScheduleBounds(){
 placeLauncher();
 if(!scheduleFrame||scheduleFrame.hidden)return;
 const shell=scheduleFrame.contentDocument?.querySelector('.rings-shell');if(!shell)return;
 const r=shell.getBoundingClientRect();if(!r.width||!r.height)return;
 if(innerWidth<=600){scheduleBox=null;scheduleFrame.style.clipPath='none';return;}
 const drawer=shell.dataset.placement==='drawer';
 const top=drawer?0:Math.max(0,r.top-4),left=drawer?Math.max(0,innerWidth-shell.offsetWidth):Math.max(0,r.left-4),right=drawer?innerWidth:Math.min(innerWidth,r.right+4),bottom=drawer?innerHeight:Math.min(innerHeight,r.bottom+4);
 scheduleBox={top,left,right,bottom,width:right-left,height:bottom-top};
 const radius=drawer?0:parseFloat(getComputedStyle(shell).borderRadius)*(r.width/shell.offsetWidth)+4;
 scheduleFrame.style.clipPath=`inset(${top}px ${innerWidth-right}px ${innerHeight-bottom}px ${left}px round ${radius}px)`;
}
addEventListener('resize',syncScheduleBounds);
new ResizeObserver(syncScheduleBounds).observe(launcherRoot);
function scheduleBusy(busy){for(const button of [scheduleTrigger,...document.querySelectorAll('a[href="'+scheduleHref+'"]')])if(button){if(busy)button.setAttribute('aria-busy','true');else button.removeAttribute('aria-busy');}}
function scheduleFailed(){scheduleBusy(false);scheduleRequested=false;scheduleFrame?.remove();scheduleFrame=null;placeLauncher();notice('Schedule could not load. Try again.');}
function closeSchedule(){scheduleRequested=false;if(scheduleFrame)scheduleFrame.hidden=true;placeLauncher();scheduleBusy(false);if(scheduleTrigger?.isConnected)scheduleTrigger.focus({preventScroll:true});}
function openSchedule(trigger){
 if(!state.recognized)return;
 if(hostSchedule){closeDialog();$('#app').hidden=true;renderPreviewTools();return;}
 scheduleTrigger=trigger;scheduleRequested=true;
 if(scheduleFrame){if(scheduleReady){scheduleFrame.hidden=false;syncScheduleBounds();scheduleFrame.focus({preventScroll:mobileLauncher.matches});}else scheduleBusy(true);return;}
 scheduleBusy(true);
 const frame=document.createElement('iframe');scheduleFrame=frame;frame.id='profile-schedule-view';frame.title='RingStatus Schedule';frame.hidden=true;
 frame.style.cssText='position:fixed;inset:0;width:100%;height:100dvh;border:0;z-index:110;background:transparent';
 frame.addEventListener('load',()=>{
  if(frame!==scheduleFrame||!state.recognized)return;
  const doc=frame.contentDocument;if(!doc?.querySelector('.rings-shell')){scheduleFailed();return;}
  doc.addEventListener('click',event=>{if(event.target.closest('.rings-profile-nav a,.back-row a.back-link,.schedule-profile-control')){event.preventDefault();event.stopImmediatePropagation();closeSchedule();if(event.target.closest('.schedule-profile-control')){$('#app').hidden=false;renderPreviewTools();}}},true);
  const syncTheme=()=>{const theme=doc.documentElement.dataset.theme;if(['dark','light'].includes(theme)&&document.documentElement.dataset.theme!==theme)document.documentElement.dataset.theme=theme;recognizeRoot.dataset.theme=document.documentElement.dataset.theme;const toggle=$('[data-action="theme"]');if(toggle)toggle.outerHTML=themeToggle();};
  new MutationObserver(syncTheme).observe(doc.documentElement,{attributes:true,attributeFilter:['data-theme']});syncTheme();
  const reveal=()=>{
   if(frame!==scheduleFrame||!state.recognized)return;
   scheduleReady=true;scheduleBusy(false);
   if(scheduleRequested){frame.hidden=false;syncScheduleBounds();frame.focus({preventScroll:mobileLauncher.matches});}
   const shell=doc.querySelector('.rings-shell');
   if(shell){scheduleBoundsObserver?.disconnect();scheduleBoundsObserver=new ResizeObserver(syncScheduleBounds);scheduleBoundsObserver.observe(shell);}
  };
  if(doc.documentElement.dataset.scheduleReady==='true')reveal();
  else doc.addEventListener('rs-schedule-ready',reveal,{once:true});
 });
 frame.addEventListener('error',scheduleFailed);
 frame.src=scheduleHref;document.body.append(frame);
}
document.addEventListener('click',event=>{
 const link=event.target.closest('a');if(!link||link.getAttribute('href')!==scheduleHref)return;
 event.preventDefault();event.stopImmediatePropagation();openSchedule(link);
},true);
document.documentElement.dataset.theme=previewParams.get('theme')==='light'?'light':'dark';
function themeToggle(){
 const light=document.documentElement.dataset.theme==='light';
 const label=light?'Light mode; switch to Dark mode':'Dark mode; switch to Light mode';
 const shape=light?'<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M2 12h2M20 12h2M5 5l1.4 1.4M17.6 17.6 19 19M5 19l1.4-1.4M17.6 6.4 19 5"/>':'<path d="M20.9 13A9 9 0 0 1 11 3.1 9 9 0 1 0 20.9 13Z"/>';
 return `<button type="button" class="icon-button" data-action="theme" aria-label="${label}" title="${label}" aria-pressed="${light}"><svg class="icon" viewBox="0 0 24 24" aria-hidden="true">${shape}</svg></button>`;
}
document.documentElement.dataset.size=previewParams.get('size')==='compact'?'compact':'desktop';
let recognizeWidget;
const recognizeRoot=document.createElement('div');recognizeRoot.id='recognize-entry';document.body.append(recognizeRoot);
let ringSample=null,ringExpiry=null;
let canonical=null,connection,loading=true,connectionError='',accountGeneration=0,scopeErrors={};
const UI_KEY='rs_profile_v3_view:';
const defaults={profile:true,actions:true,tabs:true,search:true,secondary:true,tab:'all',sort:'last_name',layout:'list',collapsed:{},activity:[]};
let state=EMPTY(),ui={...defaults},view='kitchen',selectedId=null,query='',editor=null,draftPhoto='',activeMenu=null,returnFocus=null,noticeTimer,horseEditor=null,selectedHorseId=null,originRoster=null;

selectedId=state.currentUserId;
let optinData=freshOptins(),optinGroup=null,optinDirty=false,optinQuery='',optinSort='category',optinSearchOpen=false,optinFeedback='',optinFeedbackKind='';

const actor=()=>state.users.find(u=>u.id===state.currentUserId),selected=()=>state.users.find(u=>u.id===selectedId)||actor(),fullName=u=>`${u?.first_name||''} ${u?.last_name||''}`.trim(),owner=()=>isOwner(canonical);
const uid=()=>globalThis.crypto?.randomUUID?.()||String(Date.now())+Math.random().toString(16).slice(2);
function saveUI(){if(!state.currentUserId)return;try{const {activity,agentMessages,horses,...preferences}=ui;localStorage.setItem(UI_KEY+state.currentUserId,JSON.stringify(preferences));}catch{notice('View settings could not be saved.');}}

function log(text,detail=''){ui.activity=[{id:uid(),text,detail,at:new Date().toISOString()},...ui.activity].slice(0,50);saveUI();}
function notice(text){clearTimeout(noticeTimer);$('#notice').textContent=text;$('#notice').hidden=false;noticeTimer=setTimeout(()=>$('#notice').hidden=true,2600);}
function avatar(user,small=false,glyph='user'){return `<span class="avatar${small?' small':''}">${user?.profile_img?`<img src="${escape(user.profile_img)}" alt="" referrerpolicy="no-referrer">`:icon(glyph)}</span>`;}
function tags(user){return (user?.tags||[]).map(t=>`<span class="tag" ${t.toLowerCase()==='owner'?'data-owner':''}>${escape(t)}</span>`).join('');}
function button(action,text,options={}){return v3Button(action,text,options,icon);}
function menu(id,trigger,content,extra=''){return `<div class="menu-anchor ${extra}">${trigger}<div class="ui-menu" id="${id}" role="menu" hidden>${content}</div></div>`;}
function menuItem(action,label,whichIcon='document',extra=''){return `<button type="button" role="menuitem" data-action="${action}" ${extra}>${icon(whichIcon)}<span>${escape(label)}</span></button>`;}

function brand(){return v3Brand(`<button class="icon-button close-button" data-action="close" aria-label="Close Profile">${icon('close')}</button>`);}
function profileRow(){const u=selected();return `<div class="person-row" data-stack="profile" ${['agent','horse'].includes(view)||!state.recognized||!(libraryPage?ui.libraryProfile:ui.profile)?'hidden':''}><div class="avatar-wrap">${avatar(u)}<button class="avatar-edit" data-action="edit-profile" aria-label="Edit profile picture" ${u?.id!==state.currentUserId?'disabled':''}>${icon('edit')}</button></div><div class="person-copy"><h2>${escape(fullName(u))}</h2><div class="tag-row">${tags(u)}</div></div></div>`;}
function createControl(){return menu('create-menu',button('menu',`Create${icon('chevron')}`,{variant:'primary',extra:'data-menu="create-menu" aria-haspopup="menu" aria-expanded="false"'}),`${owner()?menuItem('add-member','Team member','users'):''}${menuItem('add-account','Account','user')}${menuItem('note','Note','comment')}`);}
function libraryLinks(){return menuItem('component-library','Component library','grid')+menuItem('icon-library','Icon library','grid');}
function actionRow(){if(libraryPage)return `<div class="action-row" data-stack="actions" ${!ui.actions?'hidden':''}>${componentPage?button('icon-library','Icon library',{icon:'grid'}):button('download-icons','Download SVGs',{variant:'primary',icon:'download'})}${button('icon-home','Profile',{icon:'back'})}${menu('library-menu',button('menu','',{variant:'icon',icon:'more',label:'Libraries',extra:'data-menu="library-menu" aria-haspopup="menu" aria-expanded="false"'}),libraryLinks())}</div>`;return `<div class="action-row" data-stack="actions" ${view==='agent'||!state.recognized||!ui.actions?'hidden':''}>${navigationActions()}</div>`;}
function navigationActions(){const active=view==='optins'?'shows':view==='group'?ui.group:'team';return [['team','Team','users'],['shows','Shows','list'],['waze','Waze','pin'],['logs','Logs','document']].map(([key,label,glyph])=>button('nav-group',label,{icon:glyph,extra:`data-value="${key}" aria-pressed="${active===key}"`})).join('');}

function slackMark(){return '<svg class="icon" viewBox="0 0 24 24" aria-hidden="true" style="stroke:none"><g fill="#36c5f0"><rect x="8" y="1" width="4" height="10" rx="2"/><rect x="1" y="7" width="6" height="4" rx="2"/></g><g fill="#2eb67d"><rect x="13" y="8" width="10" height="4" rx="2"/><rect x="13" y="1" width="4" height="6" rx="2"/></g><g fill="#ecb22e"><rect x="12" y="13" width="4" height="10" rx="2"/><rect x="17" y="13" width="6" height="4" rx="2"/></g><g fill="#e01e5a"><rect x="1" y="12" width="10" height="4" rx="2"/><rect x="7" y="17" width="4" height="6" rx="2"/></g></svg>';}
function showContact(channel){const u=selected(),label={call:'Call',text:'Text',slack:'Slack'}[channel],phone=String(u.sms||'').replace(/[^+0-9]/g,'');openDialog(label,`<p>${escape(fullName(u))}</p><p class="dialog-note">${channel==='slack'?'No Slack destination is configured.':phone?escape(u.sms):'No phone number is available.'}</p><div class="dialog-actions">${button('dialog-close','Cancel')}${channel!=='slack'&&phone?`<a class="ui-button" data-variant="primary" href="${channel==='call'?'tel':'sms'}:${escape(phone)}">${label}</a>`:''}</div>`,'button');}


function tabRow(){if(view==='optins'&&state.recognized)return groupTabRow();if(view==='group'&&state.recognized)return groupTabRow();if(componentPage)return componentTabs();if(libraryPage)return `<nav class="tab-row" role="tablist" aria-label="Icon categories" data-stack="tabs" ${!ui.tabs?'hidden':''}>${[['all','All'],['people','People'],['animals','Animals'],['interface','Interface']].map(([key,label])=>`<button class="ui-button" role="tab" id="tab-${key}" aria-selected="${(ui.iconCategory||'all')===key}" aria-controls="section-one" data-action="icon-tab" data-value="${key}">${label}</button>`).join('')}</nav>`;return `<nav class="tab-row" role="tablist" aria-label="Profile content" data-stack="tabs" ${view==='agent'||!state.recognized||!ui.tabs?'hidden':''}>${[['all','Profile'],['team','List'],['simple','Simple list'],['activity','Activity'],['outputs','Outputs']].map(([key,label])=>`<button class="ui-button" role="tab" id="tab-${key}" aria-selected="${ui.tab===key}" aria-controls="section-one" data-action="tab" data-value="${key}">${label}</button>`).join('')}</nav>`;}
function searchRow(){if(componentPage)return componentSearch();const sort=menu('sort-menu',button('menu','',{variant:'icon',icon:'sort',label:'Sort results',extra:'data-menu="sort-menu" aria-haspopup="menu" aria-expanded="false"'}),(libraryPage?[['name','Name'],['category','Category']]:[['last_name','Name'],['modified_at','Last modified'],['created_at','Last created']]).map(([key,label])=>`<button role="menuitemradio" aria-checked="${(libraryPage?(ui.iconSort||'name'):ui.sort)===key}" data-action="sort" data-value="${key}">${escape(label)}${(libraryPage?(ui.iconSort||'name'):ui.sort)===key?`<span class="menu-check">${icon('check')}</span>`:''}</button>`).join(''));return `<div class="search-row" data-stack="search" ${['agent','group','optins'].includes(view)||!state.recognized||!ui.search?'hidden':''}><button class="search-control" data-action="search" aria-label="Search ${libraryPage?'icon library':view==='horses'?'Horses':view==='riders'?'Riders':'Profile v3'}">${icon('search')}<span>Search</span></button><div class="view-tools">${sort}<span class="tool-divider" aria-hidden="true"></span>${button('layout','',{variant:'icon',icon:'grid',label:'Grid view',extra:`data-value="grid" aria-pressed="${(libraryPage?(ui.iconLayout||'grid'):ui.layout)==='grid'}"`})}${button('layout','',{variant:'icon',icon:'list',label:'List view',extra:`data-value="list" aria-pressed="${(libraryPage?(ui.iconLayout||'grid'):ui.layout)==='list'}"`})}</div></div>`;}
function section(id,label,body,count,hidden=false,action=''){const collapsed=!!ui.collapsed[id],heading=`<button class="section-head" aria-expanded="${!collapsed}" aria-controls="section-${id}-body" data-action="collapse" data-value="${id}"><span>${escape(label)}</span><span class="section-count">${count}</span>${icon('chevron')}</button>`;return `<section class="section" id="section-${id}" data-stack="${id==='one'?'section-1':'section-2'}" ${hidden?'hidden':''}>${action?`<div class="section-heading">${heading}${action}</div>`:heading}<div class="section-body" id="section-${id}-body" ${collapsed?'hidden':''}>${body}</div></section>`;}

function visiblePeople(){return roster(canonical);}
function personRows(people,glyph='user',action='person',empty='No matching people.'){return `<div class="results" data-layout="${ui.layout}">${people.map(u=>`<button class="result-row" data-action="${action}" data-id="${escape(u.id)}">${avatar(u,true,glyph)}<span class="result-copy"><strong>${escape(fullName(u))}${u.id===state.currentUserId?'<span class="pill">You</span>':''}</strong><small>${escape((u.tags||[]).map(t=>t[0].toUpperCase()+t.slice(1)).join(' · ')||u.role)}${glyph==='horse'||u.active?'':' · Inactive'}</small></span>${icon('arrow')}</button>`).join('')||`<p class="empty-result">${escape(empty)}</p>`}</div>`;}
function activityRows(){const events=ui.activity;return `<div class="results" data-layout="${ui.layout}">${events.map(a=>`<button class="result-row" data-action="activity-detail" data-id="${escape(a.id)}">${icon('clock')}<span class="result-copy"><strong>${escape(a.text)}</strong><small>${escape(new Date(a.at).toLocaleTimeString([], {hour:'numeric',minute:'2-digit'}))}</small></span></button>`).join('')||'<p class="empty-result">No recent activity.</p>'}</div>`;}
const outputs=[{id:'profile',name:'Profile summary',description:'JSON',type:'json'},{id:'team',name:'Team directory',description:'CSV',type:'csv'}];
function outputRows(){let found=outputs.filter(o=>owner()||o.id==='profile');if(ui.sort==='last_name')found=[...found].sort((a,b)=>a.name.localeCompare(b.name));return `<div class="results" data-layout="${ui.layout}">${found.map(o=>`<button class="result-row" data-action="output" data-id="${o.id}">${icon('document')}<span class="result-copy"><strong>${o.name}</strong><small>${o.description}</small></span>${icon('arrow')}</button>`).join('')||'<p class="empty-result">No matching outputs.</p>'}</div>`;}
function prototypeSections(){if(isOptinsView()&&state.recognized)return optinSections();if(['horses','riders','horse'].includes(view)&&state.recognized)return domainRosterSections();if(view==='agent')return agentSections();if(componentPage)return componentSections();if(libraryPage)return iconSections();if(!state.recognized)return section('one','Accounts',personRows(state.users),'')+section('two','Outputs','',0,true);if(view==='group')return genericGroupSections();const people=queryUsers(visiblePeople(),'',ui.sort),addMember=owner()?'<button type="button" class="text-button" data-action="add-member">Add Team member</button>':'';if(view==='person'||ui.tab==='all'){const u=selected();return section('one','Profile',`<div class="record-actions"><button type="button" class="text-button" data-action="edit-profile">Edit</button></div><div class="detail-grid">${[['first_name','First name'],['last_name','Last name'],['email','Email'],['sms','Phone'],['role','Role'],['active','Status']].map(([key,label])=>`<div class="detail-display ${key==='email'?'full':''}"><span class="detail-value">${escape(key==='active'?(u.active?'Active':'Inactive'):u[key]||'—')}</span><span class="detail-label">${label}</span></div>`).join('')}</div>`,'')+section('two','Outputs',outputRows(),owner()?2:1,!ui.secondary);}
 if(ui.tab==='simple')return section('one','Simple list',referenceListBlock(),filteredReferenceRows().length)+section('two','Outputs','',0,true);
 if(ui.tab==='team')return section('one','Team',(addMember?`<div class="record-actions">${addMember}</div>`:'')+personRows(people),people.length)+section('two','Outputs','',0,true);
 if(ui.tab==='outputs')return section('one','Outputs',outputRows(),owner()?2:1)+section('two','Activity','',0,true);
 return section('one','Recent activity',activityRows(),ui.activity.length)+section('two','Outputs',outputRows(),owner()?2:1,!ui.secondary||ui.tab==='activity');}

function render(){recognizeRoot.dataset.theme=document.documentElement.dataset.theme;const signedIn=libraryPage||state.recognized;document.body.dataset.recognized=String(signedIn);recognizeRoot.hidden=false;launcherRoot.hidden=!signedIn;$('#preview-links').hidden=!signedIn||!previewParams.has('show-footer');if(!signedIn){$('#app').hidden=true;placeLauncher();$('#app').replaceChildren();return;}renderPreviewTools();const scroll=$('.stack-scroll')?.scrollTop||0;$('#preview-stage').append(launcherRoot);$('#app').innerHTML=v3Shell(v3Back(accountHeader())+brand(),profileRow()+actionRow()+tabRow()+searchRow()+sections(),'');placeLauncher();$('.stack-scroll').scrollTop=scroll;$('#app').querySelectorAll('img').forEach(img=>img.addEventListener('error',()=>{img.parentElement.innerHTML=icon('user');},{once:true}));syncNavigationLocation();}

function closeMenus(){if(activeMenu){const m=$('#'+activeMenu);if(m)m.hidden=true;document.querySelector(`[data-menu="${activeMenu}"]`)?.setAttribute('aria-expanded','false');activeMenu=null;}}
function toggleMenu(id,trigger){const was=activeMenu===id;closeMenus();if(!was){const menu=$('#'+id);menu.hidden=false;trigger.setAttribute('aria-expanded','true');activeMenu=id;menu.style.position='fixed';menu.style.right='auto';menu.style.bottom='auto';menu.style.maxHeight=Math.max(100,innerHeight-32)+'px';const t=trigger.getBoundingClientRect(),r=menu.getBoundingClientRect();menu.style.left=Math.max(12,Math.min(t.right-r.width,innerWidth-r.width-12))+'px';const above=t.bottom+r.height+8>innerHeight-12;menu.style.top=Math.max(12,Math.min(above?t.top-r.height-8:t.bottom+6,innerHeight-r.height-12))+'px';menu.querySelector('button')?.focus();}}
function closeDialog(){if($('#dialog').open)$('#dialog').close();$('#dialog-body').replaceChildren();editor=null;horseEditor=null;draftPhoto='';returnFocus?.focus();}
function openDialog(title,content,focus='input:not([type=file]):not([type=checkbox])'){closeMenus();returnFocus=document.activeElement;$('#dialog-title').textContent=title;$('#dialog-body').innerHTML=content;if(!$('#dialog').open)$('#dialog').showModal();$('#dialog-body').querySelector(focus)?.focus();}
function showComponents(){openDialog('Components',`<div>${[['profile','Profile row'],['actions','Action row'],['tabs','Tab row'],['search','Search–sort'],['secondary','Second section']].map(([key,label])=>`<label class="visibility-row"><span>${label}</span><input type="checkbox" data-pref="${key}" ${(libraryPage&&key==='profile'?ui.libraryProfile:ui[key])?'checked':''}></label>`).join('')}</div><ol class="lock-order"><li>1. Back row or minimum space</li><li>2. Brand</li><li>3. Profile</li><li>4. Action row</li><li>5. Tab row</li><li>6. Search–sort</li><li>7. Section</li><li>8. Section</li><li>9. Footer</li></ol><div class="dialog-actions">${button('kitchen','Show all components')}${button('dialog-close','Done',{variant:'primary'})}</div>`,'input');}

function showNote(){openDialog('Add note',`<form id="note-form"><label class="field">Note<textarea name="note" rows="4" maxlength="1000" required></textarea></label><div class="dialog-actions">${button('dialog-close','Cancel')}<button class="ui-button" data-variant="primary">Save note</button></div></form>`,'textarea');}
function showLogin(){recognizeWidget?.requestCredential();}
function searchItems(){if(view==='horses')return horseSearchItems();if(view==='riders')return riderSearchItems();const text=query.toLowerCase(),people=visiblePeople().filter(u=>(fullName(u)+' '+u.email+' '+u.tags.join(' ')).toLowerCase().includes(text));const activity=ui.activity.filter(a=>(a.text+' '+a.detail).toLowerCase().includes(text)),files=outputs.filter(o=>(owner()||o.id==='profile')&&o.name.toLowerCase().includes(text));return [...people.map(u=>({kind:'person',id:u.id,label:fullName(u),description:u.tags.join(' · '),icon:'user'})),...activity.map(a=>({kind:'activity-detail',id:a.id,label:a.text,description:new Date(a.at).toLocaleString(),icon:'clock'})),...files.map(o=>({kind:'output',id:o.id,label:o.name,description:o.description,icon:'document'}))];}
function renderSearch(){if(componentPage){renderComponentSearch();return;}if(libraryPage){renderIconSearch();return;}const found=searchItems();$('#search-results').innerHTML=`<p class="count-line" role="status">${found.length} result${found.length===1?'':'s'}</p>${found.map(r=>`<button class="result-row" data-action="${r.kind}" data-id="${escape(r.id)}">${icon(r.icon)}<span class="result-copy"><strong>${escape(r.label)}</strong><small>${escape(r.description)}</small></span></button>`).join('')||'<p class="empty-result">No results.</p>'}`;}
function showSearch(){query='';openDialog('Search',`${v3Search(componentPage?'Search components':libraryPage?'Search icons':view==='horses'?'Search horses':view==='riders'?'Search riders':'Search profiles, activity and outputs','global-search',icon('search'))}<div class="search-dialog-results" id="search-results"></div>`);renderSearch();}
function csvCell(v){return '"'+String(v||'').replaceAll('"','""')+'"';}
function outputData(id){if(id==='team'){if(!owner())throw Error('Only the Team Owner can export the directory.');return {name:'team-directory.csv',type:'text/csv',text:[['First name','Last name','Email','Phone','Role','Tags'],...visiblePeople().map(u=>[u.first_name,u.last_name,u.email,u.sms,u.role,u.tags.join('; ')])].map(row=>row.map(csvCell).join(',')).join('\r\n')};}const u=selected();return{name:'profile-summary.json',type:'application/json',text:JSON.stringify({name:fullName(u),email:u.email,phone:u.sms,role:u.role,tags:u.tags,active:u.active},null,2)};}
function showOutput(id){try{const out=outputData(id);openDialog(id==='team'?'Team directory':'Profile summary',`<pre class="output-preview">${escape(out.text)}</pre><div class="dialog-actions">${button('copy-output','Copy',{extra:`data-id="${id}"`})}${button('download-output','Download',{variant:'primary',icon:'download',extra:`data-id="${id}"`})}</div>`,'button');}catch(e){notice(e.message);}}
function downloadOutput(id){try{const file=outputData(id),url=URL.createObjectURL(new Blob([file.text],{type:file.type})),a=document.createElement('a');a.href=url;a.download=file.name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);log('Downloaded '+file.name);render();notice('Downloaded');}catch(e){notice(e.message);}}

$('#dialog .icon-button').innerHTML=icon('close');
async function handleUIAction(e){const b=e.target.closest('[data-action]');if(!b){if(e.target.closest('a[href]')&&isOptinsView()&&!allowOptinsExit())e.preventDefault();if(!e.target.closest('.ui-menu'))closeMenus();return;}const action=b.dataset.action,id=b.dataset.id,value=b.dataset.value;if(isOptinsView()&&isOptinsExit(action,value)&&!allowOptinsExit())return;if(action==='menu'){toggleMenu(b.dataset.menu,b);return;}closeMenus();
 if(action==='component-library'){location.assign('/test/rs-profile-v3/component-library/');return;}if(action==='component-detail'){showComponent(id);return;}if(action==='component-tab'){ui.componentCategory=value;saveUI();render();return;}if(action==='component-sort'){ui.componentSort=value;saveUI();render();return;}if(action==='component-layout'){ui.componentLayout=value;saveUI();render();return;}if(action==='copy-component'){try{await navigator.clipboard.writeText(componentSample(id));notice('Component HTML copied');}catch{notice('Copy is unavailable.');}return;}if(action==='sample-back'){closeDialog();return;}if(action==='sample-tab'){const nav=b.closest('[role=tablist]');nav.querySelectorAll('[role=tab]').forEach(t=>t.setAttribute('aria-selected',String(t===b)));return;}if(action==='sample-collapse'){const content=b.nextElementSibling;content.hidden=!content.hidden;b.setAttribute('aria-expanded',String(!content.hidden));return;}if(action==='sample-sort'){b.closest('[role=menu]').querySelectorAll('[role=menuitemradio]').forEach(t=>t.setAttribute('aria-checked',String(t===b)));notice(value==='name'?'Sorted by name':'Sorted by date');return;}if(action==='sample-layout'){b.parentElement.querySelectorAll('[aria-pressed]').forEach(t=>t.setAttribute('aria-pressed',String(t===b)));return;}if(action==='icon-library'){location.assign('/test/rs-profile-v3/icon-library/');return;}if(action==='icon-home'){location.assign('/test/rs-profile-v3/');return;}if(action==='icon-tab'){ui.iconCategory=value;saveUI();render();return;}if(action==='icon-detail'){showIcon(id);return;}if(action==='copy-icon'){try{await navigator.clipboard.writeText(svgSource(id));notice('SVG copied');}catch{notice('Copy is unavailable. Use Download.');}return;}if(action==='download-icon'){downloadIcon(id);return;}if(action==='download-icons'){downloadIcon();return;}
 if(action==='optin-group'){optinGroup=b.dataset.group;optinQuery='';optinSort='category';optinSearchOpen=false;render();$('.stack-scroll').scrollTop=0;return;}if(action==='optin-value'){optinData.variables[b.dataset.key].value=value;markOptinsChanged();render();document.querySelector(`[data-action="optin-value"][data-key="${b.dataset.key}"][data-value="${value}"]`)?.focus();return;}if(action==='optin-search'){optinSearchOpen=!optinSearchOpen;if(!optinSearchOpen)optinQuery='';render();$('#optin-query')?.focus();return;}if(action==='optin-save'){saveOptinPreferences();return;}if(action==='reference-search'){ui.referenceSearchOpen=!ui.referenceSearchOpen;if(!ui.referenceSearchOpen)ui.referenceQuery='';saveUI();const block=b.closest('.reference-list-component');block.innerHTML=referenceListTools()+simpleList();refreshReferenceLists();block.querySelector('input')?.focus();return;}if(action==='reference-sort'){ui.referenceOrder=ui.referenceOrder==='time-desc'?'time-asc':'time-desc';saveUI();render();return;}if(action==='reference-detail'){const r=referenceListRows[Number(id)];if(r)openDialog(r[3],`<p class="dialog-note">${r[0]} · ${r[1]} · ${r[2]} · ${r[4]}</p>`+button('dialog-close','Done',{variant:'primary'}),'button');return;}if(action==='nav-group'){openGroup(value);return;}if(action==='group-tab'){view='group';ui.groupTabs={...(ui.groupTabs||{}),[ui.group]:value};saveUI();render();document.querySelector(`[data-action="group-tab"][data-value="${value}"]`)?.focus();return;}if(action==='launch'){launch(value);return;}if(action==='contact'){showContact(b.dataset.channel);return;}if(action==='not-you'){void signOutConnected(true);return;}if(action==='dialog-close'){closeDialog();return;}
 if(action==='close'){closeDialog();$('#app').hidden=true;renderPreviewTools();launcherRoot.querySelector('[data-value=profile]')?.focus();return;}
 if(action==='back'){if(isOptinsView()){if(optinGroup){optinGroup=null;optinQuery='';optinSort='category';optinSearchOpen=false;render();$('.stack-scroll').scrollTop=0;return;}view='home';ui.group='team';ui.tab='all';ui.launcher='profile';render();return;}if(view==='group'){openGroup('team');ui.tab='all';ui.launcher='profile';render();return;}if(view==='horse'||view==='person'&&originRoster==='riders'){view=view==='horse'?'horses':'riders';ui.launcher=view;selectedId=state.currentUserId;render();return;}if(['horses','riders'].includes(view)){view='home';ui.tab='all';ui.group='team';ui.launcher='profile';selectedId=state.currentUserId;render();return;}if(view==='agent'){view='home';ui.launcher='profile';ui.tab='all';render();return;}if(libraryPage){location.assign('/test/rs-profile-v3/');return;}view='home';selectedId=state.currentUserId;render();return;}
 if(action==='person'){closeDialog();if(!state.recognized){showLogin();return;}if(!canRead(canonical,id)){notice('This profile is not available.');return;}originRoster=view==='riders'?'riders':null;selectedId=id;view='person';render();return;}
 if(action==='horse-open'){closeDialog();selectedHorseId=id;view='horse';render();return;}if(action==='add-horse'){editHorse();return;}if(action==='edit-horse'){editHorse(selectedHorseId);return;}if(action==='add-rider'){if(owner())editProfile('rider');return;}if(action==='edit-profile'){editProfile();return;}if(action==='add-member'){if(owner())editProfile('member');return;}if(action==='add-account'){void signOutConnected(true);return;}
 if(action==='note'){showNote();return;}if(action==='login'){showLogin();return;}
 if(action==='switch-account'){void signOutConnected(true);return;}
 if(action==='logout'){void signOutConnected(false);return;}
 if(action==='components'){showComponents();return;}
 if(action==='kitchen'){ui={...ui,libraryProfile:libraryPage?true:ui.libraryProfile,profile:true,actions:true,tabs:true,search:true,secondary:true,tab:'all',collapsed:{}};saveUI();view='kitchen';selectedId=state.currentUserId;closeDialog();render();return;}
 if(action==='search'){showSearch();return;}
 if(action==='tab'){selectedId=state.currentUserId;ui.tab=value;ui.group='team';ui.launcher=value==='all'?'profile':'team';view='kitchen';saveUI();render();$('#tab-'+value)?.focus();return;}
 if(action==='history'){ui.tab='activity';view='kitchen';saveUI();render();return;}
 if(action==='sort'){if(libraryPage)ui.iconSort=value;else ui.sort=value;saveUI();render();document.querySelector('[data-menu="sort-menu"]')?.focus();return;}
 if(action==='layout'){if(libraryPage)ui.iconLayout=value;else ui.layout=value;saveUI();render();return;}
 if(action==='collapse'){ui.collapsed={...ui.collapsed,[value]:!ui.collapsed[value]};saveUI();render();document.querySelector(`[data-action="collapse"][data-value="${value}"]`)?.focus();return;}
 if(action==='output'){showOutput(id);return;}if(action==='export'){showOutput('profile');return;}if(action==='download-output'){downloadOutput(id);return;}
 if(action==='copy-output'){try{await navigator.clipboard.writeText(outputData(id).text);notice('Copied');}catch{notice('Copy is unavailable. Use Download.');}return;}
 if(action==='activity-detail'){const item=ui.activity.find(a=>a.id===id);if(item)openDialog('Activity',`<p class="dialog-note">${escape(new Date(item.at).toLocaleString())}</p><p>${escape(item.text)}</p>${item.detail?`<p class="dialog-note">${escape(item.detail)}</p>`:''}<div class="dialog-actions">${button('dialog-close','Done',{variant:'primary'})}</div>`,'button');return;}
 if(action==='remove-photo'){draftPhoto='';$('#profile-form [name="photo_url"]').value='';$('#photo-preview').innerHTML=avatar({});$('#photo-file').value='';return;}
}
document.addEventListener('click',handleUIAction);

document.addEventListener('change',e=>{if(e.target.dataset.optinAlert){const key=e.target.dataset.optinAlert;optinData.variables[key].enabled=e.target.checked;markOptinsChanged();render();document.querySelector(`[data-optin-alert="${key}"]`)?.focus();return;}if(e.target.dataset.optinTime){optinData.variables[e.target.dataset.optinTime].value=e.target.value;markOptinsChanged();refreshOptinFeedback();return;}if(e.target.id==='optin-sort'){optinSort=e.target.value;render();return;}if(e.target.id==='reference-order'){ui.referenceOrder=e.target.value;saveUI();refreshReferenceLists();return;}if(e.target.dataset.pref){const pref=libraryPage&&e.target.dataset.pref==='profile'?'libraryProfile':e.target.dataset.pref;ui[pref]=e.target.checked;saveUI();render();return;}if(e.target.id==='photo-file'){const file=e.target.files[0];if(!file)return;if(!['image/png','image/jpeg','image/webp'].includes(file.type)||file.size>1024*1024){$('#form-error').textContent='Choose a PNG, JPEG or WebP smaller than 1 MB.';return;}const reader=new FileReader(),forUser=editor?.userId;reader.onload=()=>{if(editor?.userId!==forUser)return;draftPhoto=String(reader.result);$('#profile-form [name="photo_url"]').value='';$('#photo-preview').innerHTML=avatar({profile_img:draftPhoto});};reader.readAsDataURL(file);}});
document.addEventListener('input',e=>{if(e.target.id==='optin-query'){optinQuery=e.target.value;$('#optin-alerts').innerHTML=renderOptinAlerts();return;}if(e.target.id==='reference-query'){ui.referenceQuery=e.target.value;saveUI();const block=e.target.closest('.reference-list-component');block.querySelector('.simple-list-frame').outerHTML=simpleList();if(block.closest('#section-one'))$('#section-one .section-count').textContent=filteredReferenceRows().length;return;}if(e.target.id==='agent-message'){const send=$('#agent-submit'),ready=!!e.target.value.trim();send.disabled=!ready;send.innerHTML=icon(ready?'send':'wave');send.setAttribute('aria-label',ready?'Send message':'Voice is not connected');return;}if(e.target.id==='global-search'){query=e.target.value;renderSearch();}});
document.addEventListener('submit',e=>{e.preventDefault();const form=e.target,fd=new FormData(form);if(form.id==='horse-form'){saveHorseForm(fd);return;}if(form.id==='profile-form'){saveConnectedProfile(fd);return;}
 if(form.id==='agent-form'){submitAgent(String(fd.get('message')||''));return;}if(form.id==='note-form'){const text=String(fd.get('note')||'').trim();if(!text)return;log('Note',text);closeDialog();render();notice('Note saved');return;}
 if(form.id==='login-form')void loginConnected(fd,form);
});

$('#dialog').addEventListener('cancel',e=>{e.preventDefault();closeDialog();});
document.addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'&&!$('#dialog').open){e.preventDefault();showSearch();return;}if(e.key==='Escape'&&!$('#dialog').open){if(isOptinsView()&&!allowOptinsExit()){e.preventDefault();return;}if(activeMenu){closeMenus();return;}$('#app').hidden=true;renderPreviewTools();launcherRoot.querySelector('[data-value=profile]')?.focus();}if(activeMenu&&['ArrowDown','ArrowUp'].includes(e.key)){e.preventDefault();const items=[...$('#'+activeMenu).querySelectorAll('button')],i=items.indexOf(document.activeElement);items[(i+(e.key==='ArrowDown'?1:-1)+items.length)%items.length]?.focus();}});


function sortedIconNames(){const category=ui.iconCategory||'all';return iconNames.filter(n=>category==='all'||iconMeta[n].category===category).sort((a,b)=>(ui.iconSort==='category'?iconMeta[a].category.localeCompare(iconMeta[b].category):0)||iconMeta[a].label.localeCompare(iconMeta[b].label));}
function iconCards(names){return `<div class="icon-grid" data-layout="${ui.iconLayout||'grid'}">${names.map(name=>`<article class="icon-card"><div class="icon-preview">${icon(name)}</div><h3>${escape(iconMeta[name].label)}</h3><p>${escape(iconMeta[name].description)}</p><div class="icon-actual">${icon(name)}<span>${escape(iconMeta[name].label)} · 18px</span></div>${button('icon-detail','View SVG',{extra:`data-id="${name}"`})}</article>`).join('')}</div>`;}
function iconSections(){const names=sortedIconNames(),all=(ui.iconCategory||'all')==='all',selected=['rider','trainer','horse'];return section('one',all?'People & horses':'Icons',iconCards(all?names.filter(n=>selected.includes(n)):names),all?3:names.length)+section('two','More icons',iconCards(all?names.filter(n=>!selected.includes(n)):[]),names.length-3,!ui.secondary||!all);}
function renderIconSearch(){const q=query.toLowerCase(),found=iconNames.filter(n=>(iconMeta[n].label+' '+iconMeta[n].description).toLowerCase().includes(q));$('#search-results').innerHTML=`<p class="count-line" role="status">${found.length} result${found.length===1?'':'s'}</p>${found.map(name=>`<button class="result-row" data-action="icon-detail" data-id="${name}">${icon(name)}<span class="result-copy"><strong>${escape(iconMeta[name].label)}</strong><small>${escape(iconMeta[name].description)}</small></span></button>`).join('')||'<p class="empty-result">No results.</p>'}`;}
function showIcon(name){if(!iconMeta[name])return;openDialog(iconMeta[name].label,`<div class="icon-preview">${icon(name)}</div><p class="dialog-note">${escape(iconMeta[name].description)} · 18px row icon</p><pre class="output-preview">${escape(svgSource(name))}</pre><div class="dialog-actions">${button('copy-icon','Copy SVG',{extra:`data-id="${name}"`})}${button('download-icon','Download',{variant:'primary',icon:'download',extra:`data-id="${name}"`})}</div>`,'button');}
function downloadIcon(name){const file=name?{name:name+'.svg',text:svgSource(name)}:{name:'ringstatus-icons.svg',text:spriteSource()},url=URL.createObjectURL(new Blob([file.text],{type:'image/svg+xml'})),a=document.createElement('a');a.href=url;a.download=file.name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);notice('Downloaded '+file.name);}


const referenceListRows=[
 ['8:00A','Ring 6','111',"USHJA HUNTER 2’ (FIRST YEAR SHOWN)",'1/1'],
 ['8:00A','Ring 6','112',"USHJA HUNTER 2’ (FIRST YEAR SHOWN)",'1/1'],
 ['8:06A','Ring 6','110',"USHJA HUNTER 2’ (FIRST YEAR SHOWN) U/S",'1/1'],
 ['8:09A','Rost','372','3’3” GREEN HUNTER','1/2'],
 ['8:09A','Rost','373','3’3” GREEN HUNTER','1/2'],
 ['8:15A','Grand','','Hand & Course Walk To Begin At 8:15am For 2 Hours',''],
 ['8:24A','Rost','371','3’3” GREEN HUNTER U/S','1/2'],
 ['8:42A','Mogav','','Fall Back # Schooling after Build/Drag until 4:30pm','']
];

// One launcher order shared by the app and its library example.
const coordinatedLauncher=[['profile','Profile','user'],['team','Team','users'],['shows','Show','list'],['waze','Waze','pin'],['logs','Logs','document'],['agent','Agent','wave'],['schedule','Schedule','calendar'],['horses','Horses','horse'],['riders','Riders','rider'],['logout','Logout','logout']];

const componentCatalog=[
 {id:'back',label:'Back row',category:'stack',description:'Back control and minimum reserved space.'},
 {id:'brand',label:'Brand row',category:'stack',description:'Locked wordmark, Pro badge and close control.'},
 {id:'profile',label:'Profile row',category:'stack',description:'Picture with edit, name and tags. Inverse Owner.'},
 {id:'actions',label:'Action row',category:'stack',description:'Team, Shows, Waze and Logs open coordinated child tabs.'},
 {id:'tabs',label:'Tab row',category:'stack',description:'Single selected pill with consistent text tabs.'},
 {id:'search',label:'Search–sort',category:'stack',description:'Pill search, sort menu and grid/list controls.'},
 {id:'section',label:'Section',category:'stack',description:'Section label, count, open/close and list content.'},
 {id:'footer',label:'Footer',category:'stack',description:'Always present. Account menu, add account and log out.'},
 {id:'create',label:'Create dropdown',category:'controls',description:'Library component only. Team member, account and note.'},
 {id:'launcher',label:'Launcher widget',category:'controls',description:'Profile, Team, Show, Waze, Logs, Agent, Schedule, Horses, Riders and Logout.'},
 {id:'utilities',label:'Utility controls',category:'controls',description:'Activity, note and more controls. Library reference only.'},
 {id:'buttons',label:'Buttons',category:'controls',description:'Primary, outlined, quiet and round icon controls.'},
 {id:'form',label:'Profile editor',category:'content',description:'Own profile form, photo, contact details, tags and save.'},
 {id:'simple-list',label:'Simple list',category:'content',description:'Five aligned columns, alternating rows and explicit detail controls.'},
 {id:'outputs',label:'Outputs',category:'content',description:'File rows with working preview, copy and download.'}
];
function catalogNames(){const category=ui.componentCategory||'all';return componentCatalog.filter(c=>category==='all'||c.category===category).sort((a,b)=>(ui.componentSort==='category'?a.category.localeCompare(b.category):0)||a.label.localeCompare(b.label));}
function componentTabs(){return `<nav class="tab-row" role="tablist" aria-label="Component categories" data-stack="tabs" ${!ui.tabs?'hidden':''}>${[['all','All'],['stack','Stack'],['controls','Controls'],['content','Content']].map(([key,label])=>`<button class="ui-button" role="tab" aria-selected="${(ui.componentCategory||'all')===key}" data-action="component-tab" data-value="${key}">${label}</button>`).join('')}</nav>`;}
function componentSearch(){const sort=menu('component-sort-menu',button('menu','',{variant:'icon',icon:'sort',label:'Sort components',extra:'data-menu="component-sort-menu" aria-haspopup="menu" aria-expanded="false"'}),[['name','Name'],['category','Category']].map(([key,label])=>`<button role="menuitemradio" aria-checked="${(ui.componentSort||'name')===key}" data-action="component-sort" data-value="${key}">${label}</button>`).join(''));return `<div class="search-row" data-stack="search" ${!ui.search?'hidden':''}><button class="search-control" data-action="search" aria-label="Search component library">${icon('search')}<span>Search</span></button><div class="view-tools">${sort}<span class="tool-divider" aria-hidden="true"></span>${[['grid','Grid view'],['list','List view']].map(([key,label])=>button('component-layout','',{variant:'icon',icon:key,label,extra:`data-value="${key}" aria-pressed="${(ui.componentLayout||'grid')===key}"`})).join('')}</div></div>`;}
function componentCards(items){return `<div class="icon-grid" data-layout="${ui.componentLayout||'grid'}">${items.map(c=>`<article class="icon-card"><h3>${c.label}</h3><p>${c.description}</p>${c.id==='create'?`<div class="component-preview">${createControl()}</div>`:`<div class="component-thumbnail" inert aria-hidden="true"><div class="component-thumbnail-content">${(c.id==='form'?button('edit-profile','Edit profile',{icon:'edit'}):componentSample(c.id)).replace(/ id="[^"]*"/g,'')}</div></div>`}${button('component-detail','Try component',{extra:`data-id="${c.id}"`})}</article>`).join('')||'<p class="empty-result">No matching components.</p>'}</div>`;}
function componentSections(){const items=catalogNames(),all=(ui.componentCategory||'all')==='all';return section('one',all?'Stack components':'Components',componentCards(all?items.filter(c=>c.category==='stack'):items),all?8:items.length)+section('two','Controls & content',componentCards(items.filter(c=>c.category!=='stack')),items.filter(c=>c.category!=='stack').length,!ui.secondary||!all);}
function renderComponentSearch(){const q=query.toLowerCase(),items=componentCatalog.filter(c=>(c.label+' '+c.description).toLowerCase().includes(q));$('#search-results').innerHTML=`<p class="count-line" role="status">${items.length} result${items.length===1?'':'s'}</p>${items.map(c=>`<button class="result-row" data-action="component-detail" data-id="${c.id}">${icon('grid')}<span class="result-copy"><strong>${c.label}</strong><small>${c.description}</small></span>${icon('arrow')}</button>`).join('')}`;}
function componentSample(id){switch(id){
 case 'back':return `<div class="back-row"><button class="back-link" data-action="sample-back">‹ Back</button></div>`;
 case 'brand':return brand();
 case 'profile':return profileRow().replace('hidden','');
 case 'actions':return `<div class="action-row">${navigationActions()}</div>`;
 case 'launcher':return `<nav class="launcher-widget" aria-label="Launcher example">${launcherMarkup()}</nav>`;
 case 'utilities':return `<div class="action-row"><div class="action-icons">${button('history','',{variant:'icon',icon:'clock',label:'Recent activity'})}${button('note','',{variant:'icon',icon:'comment',label:'Add note'})}${menu('sample-more',button('menu','',{variant:'icon',icon:'more',label:'More actions',extra:'data-menu="sample-more" aria-haspopup="menu" aria-expanded="false"'}),menuItem('export','Export profile','download')+libraryLinks())}</div></div>`;
 case 'tabs':return `<nav class="tab-row" role="tablist" aria-label="Example tabs">${['All','Team','Activity','Outputs'].map((t,i)=>`<button class="ui-button" role="tab" aria-selected="${i===0}" data-action="sample-tab">${t}</button>`).join('')}</nav>`;
 case 'search':return `<div class="search-row"><button class="search-control" data-action="search">${icon('search')}<span>Search</span></button><div class="view-tools">${menu('sample-sort',button('menu','',{variant:'icon',icon:'sort',label:'Sort sample',extra:'data-menu="sample-sort" aria-haspopup="menu" aria-expanded="false"'}),[['name','Name'],['date','Date']].map(([key,label])=>`<button role="menuitemradio" aria-checked="${key==='name'}" data-action="sample-sort" data-value="${key}">${label}</button>`).join(''))}${button('sample-layout','',{variant:'icon',icon:'grid',label:'Grid view',extra:'aria-pressed="true"'})}${button('sample-layout','',{variant:'icon',icon:'list',label:'List view',extra:'aria-pressed="false"'})}</div></div>`;
 case 'section':return `<section class="section"><button class="section-head" aria-expanded="true" data-action="sample-collapse"><span>Outputs</span><span class="section-count">${owner()?2:1}</span>${icon('chevron')}</button><div class="section-body">${outputRows()}</div></section>`;
 case 'footer':return footer().replaceAll('account-menu','sample-account-menu');
 case 'create':return createControl().replaceAll('create-menu','sample-create-menu');
 case 'buttons':return `<div class="component-preview">${button('note','Add note',{variant:'primary',icon:'plus'})}${button('export','Export',{icon:'download'})}${button('edit-profile','Edit profile',{variant:'quiet'})}${button('history','',{variant:'icon',icon:'clock',label:'Recent activity'})}</div>`;
 case 'simple-list':return referenceListBlock();
 case 'outputs':return outputRows();
 default:return '';
}}
function showComponent(id){const c=componentCatalog.find(c=>c.id===id);if(!c)return;if(id==='form'){editProfile();return;}openDialog(c.label,`<p class="dialog-note">${c.description}</p><div class="component-preview">${componentSample(id)}</div><div class="dialog-actions">${button('copy-component','Copy HTML',{extra:`data-id="${id}"`})}${button('dialog-close','Done',{variant:'primary'})}</div>`,'button');}
function applyNavigationParams(){const routeParams=new URLSearchParams(location.search);view='kitchen';
if(['all','team','simple','activity','outputs'].includes(routeParams.get('tab')))ui.tab=routeParams.get('tab');
if(routeParams.get('screen')==='agent'){view='agent';ui.launcher='agent';}
if(routeParams.get('screen')==='optins'){view='optins';ui.group='shows';ui.launcher='shows';ui.groupTabs={...(ui.groupTabs||{}),shows:'optins'};}
if(['horses','riders'].includes(routeParams.get('roster'))){view=routeParams.get('roster');ui.tab='team';ui.launcher=view;}
if(!libraryPage){if(view!=='optins')ui.group='team';if(!['agent','optins','horses','riders','horse'].includes(view))ui.launcher=ui.tab==='all'?'profile':'team';if(['shows','waze','logs'].includes(routeParams.get('group'))){ui.group=routeParams.get('group');ui.launcher=ui.group;view='group';}}
}
applyNavigationParams();
render();

function renderPreviewTools(){const theme=document.documentElement.dataset.theme,size=document.documentElement.dataset.size,current=location.pathname;const href=(path,params={})=>path+'?'+new URLSearchParams({theme,size,...(previewParams.has('show-footer')?{'show-footer':''}:{}),...(['agent','optins'].includes(view)&&path===current?{screen:view}:{}),...(['horses','riders'].includes(view)&&path===current?{roster:view}:{}),...(view==='group'&&path===current?{group:ui.group}:{}),...params});$('#preview-links').innerHTML=[['icons',href('/test/rs-profile-v3/icon-library/')],['components',href('/test/rs-profile-v3/component-library/')],['desktop lite',href(current,{size:'desktop',theme:'light'})],['desktop dark',href(current,{size:'desktop',theme:'dark'})],['<393 lite',href(current,{size:'compact',theme:'light'})],['<393 dark',href(current,{size:'compact',theme:'dark'})]].map(([label,url])=>`<a href="${escape(url)}">${escape(label)}</a>`).join('<span aria-hidden="true"> | </span>')+'<span aria-hidden="true"> | </span><button class="text-button" data-action="components">components-prefs</button>';
const strip=launcherRoot.querySelector('.launcher-scroll'),left=strip?.scrollLeft||0,top=strip?.scrollTop||0;launcherRoot.innerHTML=launcherMarkup();launcherRoot.querySelector('.launcher-scroll').scrollLeft=left;launcherRoot.querySelector('.launcher-scroll').scrollTop=top;scheduleBusy(scheduleRequested&&!scheduleReady);placeLauncher();}
function launcherMarkup(){const open=!$('#app').hidden;const items=coordinatedLauncher.map(([key,label,glyph])=>key==='schedule'?`<a class="icon-button" target="_self" href="${scheduleHref}" aria-label="${label}" title="${label}">${icon(glyph)}</a>`:`<button class="icon-button" data-action="launch" data-value="${key}" aria-label="${label}" title="${label}" aria-pressed="${open&&(key==='profile'||key===currentDestination())}" aria-controls="app" ${key==='profile'?`aria-expanded="${open}"`:''}>${icon(glyph)}</button>`);return `<div class="launcher-scroll">${items.slice(1).join('')}</div>`;}

function launch(destination){const resident=!!residentLauncherSlot;if(scheduleRequested&&(mobileLauncher.matches||resident)){closeSchedule();if(resident&&destination==='profile')$('#app').hidden=true;}closeDialog();if(libraryPage&&['profile','team','shows','waze','logs','agent','horses','riders'].includes(destination)){location.assign('/test/rs-profile-v3/?'+new URLSearchParams({theme:document.documentElement.dataset.theme,size:document.documentElement.dataset.size,tab:['team','horses','riders'].includes(destination)?'team':'all',...(destination==='agent'?{screen:'agent'}:{}),...(['horses','riders'].includes(destination)?{roster:destination}:{}),...(['shows','waze','logs'].includes(destination)?{group:destination}:{})}));return;}if(destination==='profile'){const panel=$('#app');panel.hidden=!panel.hidden;renderPreviewTools();return;}ui.launcher=destination;$('#app').hidden=false;if(['shows','waze','logs'].includes(destination)){openGroup(destination);return;}if(['horses','riders'].includes(destination)){view=destination;ui.group='team';ui.tab='team';selectedId=state.currentUserId;saveUI();render();return;}if(destination==='agent'){view='agent';saveUI();render();return;}if(destination==='profile'||destination==='team'){ui.group='team';ui.tab=destination==='team'?'team':'all';view='kitchen';selectedId=state.currentUserId;saveUI();render();return;}if(destination==='logout'){void signOutConnected(true);return;}render();openDialog(destination==='alarm'?'Alarm':'Alerts','<p class="dialog-note">Not configured.</p>'+button('dialog-close','Done',{variant:'primary'}),'button');}



function simpleList(){const rows=filteredReferenceRows();return `<div class="simple-list-frame"><table class="simple-list"><thead><tr><th scope="col"><button class="list-sort" data-action="reference-sort">Time ${(ui.referenceOrder||'time-asc')==='time-desc'?'↓':(ui.referenceOrder||'time-asc')==='time-asc'?'↑':'↕'}</button></th><th scope="col">Ring</th><th scope="col">No</th><th scope="col">Class</th><th scope="col">Entries</th></tr></thead><tbody>${rows.map(({row:r,i})=>`<tr><td>${r[0]}</td><td>${r[1]}</td><td class="list-number" ${['110','371'].includes(r[2])?'data-highlight':''}>${r[2]}</td><td${r[2]?'':' colspan="2"'}><div class="list-title">${r[2]?`<button class="list-plus" data-action="reference-detail" data-id="${i}" aria-label="Open ${escape(r[3])}">+</button>`:''}<strong title="${escape(r[3])}">${escape(r[3])}</strong></div></td>${r[2]?`<td>${r[4]}</td>`:''}</tr>`).join('')||'<tr><td colspan="5" class="empty-result">No matching rows.</td></tr>'}</tbody></table></div>`;}

function filteredReferenceRows(){const q=(ui.referenceQuery||'').trim().toLowerCase(),rows=referenceListRows.map((row,i)=>({row,i})).filter(({row})=>row.join(' ').toLowerCase().includes(q)),order=ui.referenceOrder||'time-asc';return rows.sort((a,b)=>order==='ring'?a.row[1].localeCompare(b.row[1])||a.i-b.i:order==='class'?a.row[3].localeCompare(b.row[3])||a.i-b.i:order==='time-desc'?b.i-a.i:a.i-b.i);}
function referenceListTools(){return `<div class="list-tools">${button('reference-search','',{variant:'icon',icon:'search',label:'Search Simple list',extra:`aria-expanded="${!!ui.referenceSearchOpen}" aria-controls="reference-query"`})}${ui.referenceSearchOpen?`<input class="search-control" id="reference-query" type="search" aria-label="Filter Simple list" placeholder="Search…" value="${escape(ui.referenceQuery||'')}">`:''}<label class="ui-button select-icon" data-variant="icon">${icon('sort')}<select id="reference-order" aria-label="Sort Simple list">${[['time-asc','Time: earliest first'],['time-desc','Time: latest first'],['ring','Ring: A–Z'],['class','Class: A–Z']].map(([value,label])=>`<option value="${value}" ${(ui.referenceOrder||'time-asc')===value?'selected':''}>${label}</option>`).join('')}</select></label></div>`;}
function referenceListBlock(){return `<div class="reference-list-component">${referenceListTools()}${simpleList()}</div>`;}
function agentContent(){return `<div class="assistant-start"><h1>How can I help?</h1><form class="assistant-composer" id="agent-form"><button type="button" class="icon-button" disabled aria-label="Attachments are not connected" title="Attachments are not connected">${icon('plus')}</button><input id="agent-message" name="message" aria-label="Ask RingStatus" placeholder="Ask RingStatus…" autocomplete="off" maxlength="500" required><button type="button" class="icon-button" disabled aria-label="Microphone is not connected" title="Microphone is not connected">${icon('mic')}</button><button id="agent-submit" class="ui-button" data-variant="primary" type="submit" disabled aria-label="Voice is not connected">${icon('wave')}</button></form><div class="assistant-suggestions">${button('edit-profile','Edit my profile',{icon:'user'})}${owner()?button('launch','Manage my Team',{icon:'users',extra:'data-value="team"'}):''}<a class="ui-button" target="_self" href="${scheduleHref}">${icon('calendar')}Open schedules</a></div><div class="assistant-messages" aria-live="polite">${(ui.agentMessages||[]).map(m=>`<div class="assistant-message" data-speaker="${m.role}"><p>${escape(m.text)}</p>${m.target?button(m.target,m.label,{variant:'quiet',extra:m.target==='launch'?'data-value="team"':''}):''}${m.schedule?`<a class="ui-button" target="_self" href="${scheduleHref}">Open schedules</a>`:''}</div>`).join('')}</div></div>`;}
function agentSections(){return `<section class="section" data-stack="section-1" aria-label="Agent">${agentContent()}</section><section class="section" data-stack="section-2" hidden></section>`;}
function submitAgent(text){text=text.trim();if(!text)return;const lower=text.toLowerCase();let reply={role:'assistant',text:owner()?'I can open your profile, Team or schedules. Choose a shortcut above.':'I can open your profile or schedules. Choose a shortcut above.'};if(/profile|name|phone|email/.test(lower))reply={role:'assistant',text:'Open your profile editor below.',target:'edit-profile',label:'Edit my profile'};else if(/team|member|people/.test(lower)&&owner())reply={role:'assistant',text:'Open your Team below.',target:'launch',label:'Manage my Team'};else if(/schedule|class|ring/.test(lower))reply={role:'assistant',text:'Open schedules below.',schedule:true};ui.agentMessages=[...(ui.agentMessages||[]),{role:'user',text},reply].slice(-20);saveUI();render();$('#agent-message')?.focus();}

function refreshReferenceLists(){document.querySelectorAll('.reference-list-component').forEach(block=>{if(block.closest('[inert]'))return;block.querySelector('.simple-list-frame').outerHTML=simpleList();const order=block.querySelector('#reference-order');if(order)order.value=ui.referenceOrder||'time-asc';if(!libraryPage&&block.parentElement.id==='section-one-body')$('#section-one .section-count').textContent=filteredReferenceRows().length;});}

function visibleHorses(){return [...new Map([...(canonical?.horses||[]),...(Array.isArray(ui.horses)?ui.horses:[])].filter(h=>h.tenant_id===state.tenant.id).map(h=>[h.id,h])).values()];}
function horseRowsData(){return queryUsers(visibleHorses().map(h=>({...h,first_name:'',last_name:h.name,tags:[],role:String(h.horseId||'Horse')})),'',ui.sort);}
function riderRowsData(){return queryUsers(visiblePeople().filter(u=>u.role==='rider'||u.tags.some(t=>t.toLowerCase()==='rider')),'',ui.sort);}
function domainRosterSections(){if(!canonical?.loaded?.horses&&['horse','horses'].includes(view))return section('one','Horses',scopeMessage('horses','horses'),'')+section('two','Outputs','',0,true);if(view==='horse'){const h=visibleHorses().find(h=>h.id===selectedHorseId);if(!h)return section('one','Horse','<p class="empty-result">Horse not found.</p>','')+section('two','Outputs','',0,true);return section('one','Horse',`${owner()?'<div class="record-actions"><button class="text-button" data-action="edit-horse">Edit</button></div>':''}<div class="detail-grid"><div class="detail-display full"><span class="detail-value">${escape(h.name)}</span><span class="detail-label">Name</span></div><div class="detail-display"><span class="detail-value">${escape(h.horseId)}</span><span class="detail-label">Horse ID</span></div></div>`,'')+section('two','Outputs','',0,true);}
const isHorse=view==='horses',rows=isHorse?horseRowsData():riderRowsData(),add=owner()?`<div class="record-actions"><button class="text-button" data-action="${isHorse?'add-horse':'add-rider'}">${isHorse?'Add horse':'Add rider'}</button></div>`:'';return section('one',isHorse?'Horses':'Riders',add+personRows(rows,isHorse?'horse':'rider',isHorse?'horse-open':'person',isHorse?'No horses yet.':'No riders yet.'),rows.length)+section('two','Outputs','',0,true);}
function editHorse(id=null){if(!owner()){notice('Only the Team Owner can manage horses.');return;}const h=visibleHorses().find(h=>h.id===id),draft={id:h?.id||uid(),existing:!!h};openDialog(h?'Edit horse':'Add horse',`<form id="horse-form"><div class="form-grid"><label class="field full">Name<input name="name" value="${escape(h?.name||'')}" maxlength="120" required autocomplete="off"></label><label class="visibility-row full"><span>Active</span><input name="active" type="checkbox" ${h?.active!==false?'checked':''}></label></div><p class="form-error" id="horse-error" role="alert"></p><div class="dialog-actions">${button('dialog-close','Cancel')}<button class="ui-button" data-variant="primary" type="submit">Save</button></div></form>`);horseEditor=draft;}
function saveHorseForm(fd){try{if(!owner()||!horseEditor)throw Error('Only the Team Owner can manage horses.');const name=String(fd.get('name')||'').trim();if(!name)throw Error('Enter a name.');const existing=visibleHorses().find(h=>h.id===horseEditor.id),now=new Date().toISOString(),record={...existing,id:horseEditor.id,tenant_id:state.tenant.id,name,active:fd.has('active'),created_at:existing?.created_at||now,modified_at:now},items=[...(Array.isArray(ui.horses)?ui.horses:[])],index=items.findIndex(h=>h.id===record.id);if(index<0)items.push(record);else items[index]=record;const next={...ui,horses:items};localStorage.setItem('rs_profile_v3_horses:'+state.currentUserId,JSON.stringify(items));ui=next;log((existing?'Updated horse ':'Added horse ')+name);closeDialog();render();notice('Horse saved in this browser');}catch(err){$('#horse-error').textContent=err.message;}}
function horseSearchItems(){const q=query.toLowerCase();return horseRowsData().filter(h=>h.name.toLowerCase().includes(q)).map(h=>({kind:'horse-open',id:h.id,label:h.name,description:String(h.horseId||''),icon:'horse'}));}
function riderSearchItems(){const q=query.toLowerCase();return riderRowsData().filter(u=>(fullName(u)+' '+u.email+' '+u.tags.join(' ')).toLowerCase().includes(q)).map(u=>({kind:'person',id:u.id,label:fullName(u),description:u.tags.join(' · '),icon:'rider'}));}

function openGroup(group){if(!['team','shows','waze','logs'].includes(group))return;if(libraryPage){location.assign('/test/rs-profile-v3/?'+new URLSearchParams({theme:document.documentElement.dataset.theme,size:document.documentElement.dataset.size,...(group==='team'?{tab:ui.tab}:{group})}));return;}closeDialog();ui.group=group;ui.launcher=group==='team'?(ui.tab==='all'?'profile':'team'):group;view=group==='team'?'kitchen':'group';selectedId=state.currentUserId;$('#app').hidden=false;saveUI();render();}
function groupTabRow(){let selected=(ui.groupTabs||{})[ui.group]||'tab1';if(ui.group==='shows'&&selected==='tab1')selected='optins';const items=ui.group==='shows'?[['optins','Opt-ins'],['tab2','Tab 2'],['tab3','Tab 3']]:[['tab1',ui.group==='waze'?'Ring distance':'Tab 1'],['tab2','Tab 2'],['tab3','Tab 3']];return `<nav class="tab-row" role="tablist" aria-label="${ui.group==='shows'?'Shows':ui.group==='waze'?'Waze':'Logs'} tabs" data-stack="tabs" ${!ui.tabs?'hidden':''}>${items.map(([value,label])=>`<button class="ui-button" role="tab" aria-selected="${selected===value}" aria-controls="section-one" data-action="group-tab" data-value="${value}">${label}</button>`).join('')}</nav>`;}

// Ring 6 test uses the existing location choice, acquisition and logging path.
function ringDistanceSection(){
 const allowed=ui.locationAllowed?.[state.currentUserId]===true&&locationPermission!=='denied';
 const point=allowed?(ringSample?.uid===state.currentUserId?ringSample:canonical?.geoip):null;
 let distance='—',accuracy='—',status='Not confirmed',note=allowed?'Measure your location to test Ring 6.':'Allow precise location to measure your distance.';
 if(point?.source==='gps'){
  try{
   const result=withinRing(point,{latitude:26.634806,longitude:-80.274528},{radiusFeet:100,maxAgeMs:60000,now:Date.now()});
   distance=Math.round(estimateWalk(point,{latitude:26.634806,longitude:-80.274528}).metres/0.3048)+' ft';
   accuracy=Number.isFinite(point.accuracy)?'±'+Math.ceil(point.accuracy/0.3048)+' ft':'—';
   status=result.qualified?'Yes':result.metres!==undefined&&result.metres-point.accuracy>30.48?'No':'Not confirmed';
   note=result.reason?'Location is older than 60 seconds or accuracy is unavailable. Measure again.':'Within 100 feet requires distance plus GPS accuracy to fit inside 100 feet.';
   clearTimeout(ringExpiry);
   const remaining=Date.parse(point.observedAt)+60001-Date.now();
   if(remaining>0)ringExpiry=setTimeout(refreshRingDistance,remaining);
  }catch{note='Location is unavailable. Measure again.';}
 }else if(point){note='Approximate GeoIP cannot confirm distance to a ring.';}
 const fields=[['Distance to Ring 6',distance],['GPS accuracy',accuracy],['Within 100 feet',status]];
 return section('one','Ring 6 distance test',`<div data-ring-distance><div class="detail-grid">${fields.map(([label,value])=>`<div class="detail-display"><span class="detail-value">${value}</span><span class="detail-label">${label}</span></div>`).join('')}</div><p class="help" role="status">${note}</p>${allowed?button('measure-ring','Measure again'):locationField(actor())}</div>`,'');
}
function refreshRingDistance(){if(view==='group'&&ui.group==='waze'&&((ui.groupTabs||{}).waze||'tab1')==='tab1')render();}

function genericGroupSections(){const label={shows:'Shows',waze:'Waze',logs:'Logs'}[ui.group],tab=(ui.groupTabs||{})[ui.group]||'tab1';if(ui.group==='waze'&&tab==='tab1')return ringDistanceSection();return section('one',label,`<p class="empty-result">${label} · Tab ${tab.slice(-1)}</p>`,'')+section('two','Section','',0,true);}

function currentDestination(){if(libraryPage)return ui.launcher||'profile';if(!state.recognized)return ui.launcher==='logout'?'logout':'profile';if(view==='optins')return 'shows';if(view==='group')return ui.group;if(view==='agent')return 'agent';if(['horses','horse'].includes(view))return 'horses';if(view==='riders'||view==='person'&&originRoster==='riders')return 'riders';return ui.tab==='all'?'profile':'team';}

function syncNavigationLocation(){if(libraryPage)return;const route=new URL(location.href);['screen','group','roster','tab'].forEach(key=>route.searchParams.delete(key));if(['agent','optins'].includes(view))route.searchParams.set('screen',view);else if(view==='group')route.searchParams.set('group',ui.group);else if(['horses','horse'].includes(view))route.searchParams.set('roster','horses');else if(view==='riders'||view==='person'&&originRoster==='riders')route.searchParams.set('roster','riders');else route.searchParams.set('tab',ui.tab);history.replaceState(history.state,'',route.pathname+route.search+route.hash);}

function isOptinsView(){return view==='optins'||view==='group'&&ui.group==='shows'&&['optins','tab1'].includes((ui.groupTabs||{}).shows||'tab1');}
function optinSummary(){return `${optinAlerts.filter(a=>optinData.variables[a.key].enabled).length} of 18 selected · ${optinData.enabled?'SMS on':'SMS off'}${optinDirty?' · Unsaved changes':''}`;}
function markOptinsChanged(){optinDirty=true;optinFeedback='';optinFeedbackKind='';}
function refreshOptinFeedback(){const el=$('#optin-feedback');if(el){el.textContent=optinFeedback||optinSummary();el.setAttribute('role',optinFeedbackKind==='error'?'alert':'status');}}
function optinSections(){const groups=['Classes','Rider trips','Groom tasks'],descriptions=['Starts, changes and progress','Reminders, order and results','Reminders at your chosen times'];let body=`<div class="record-actions"><button type="button" class="text-button" data-action="optin-save">Save preferences</button></div><p class="feedback" id="optin-feedback" role="${optinFeedbackKind==='error'?'alert':'status'}">${escape(optinFeedback||optinSummary())}</p>`;if(!optinGroup){body+=`<div class="results">${groups.map((g,i)=>{const rows=optinAlerts.filter(a=>a.group===g);return `<button class="result-row" data-action="optin-group" data-group="${g}">${icon(['list','rider','clock'][i])}<span class="result-copy"><strong>${g}</strong><small>${descriptions[i]}</small></span><span class="tag">${rows.filter(a=>optinData.variables[a.key].enabled).length}/${rows.length}</span>${icon('arrow')}</button>`;}).join('')}</div>`;}else{body+=optinTools()+`<p class="help">${optinGroup==='Groom tasks'?'Local clock times · '+escape(optinData.timeZone):'Choose the updates you want to receive.'}</p><div id="optin-alerts">${renderOptinAlerts()}</div>`;}return section('one',optinGroup||'Preferences',body,'')+section('two','Section','',0,true);}
function optinTools(){return `<div class="list-tools">${button('optin-search','',{variant:'icon',icon:'search',label:'Search opt-ins',extra:`aria-expanded="${optinSearchOpen}"`})}${optinSearchOpen?`<input class="search-control" id="optin-query" type="search" aria-label="Search opt-ins" placeholder="Search…" value="${escape(optinQuery)}">`:''}<label class="ui-button select-icon" data-variant="icon">${icon('sort')}<select id="optin-sort" aria-label="Sort opt-ins">${[['category','Default order'],['name','Name A–Z'],['selected','Selected first']].map(([value,label])=>`<option value="${value}" ${optinSort===value?'selected':''}>${label}</option>`).join('')}</select></label></div>`;}
function filteredOptinAlerts(){const terms=optinQuery.toLowerCase().trim().split(/\s+/).filter(Boolean);return optinAlerts.filter(a=>(!optinGroup||a.group===optinGroup)&&terms.every(t=>`${a.label} ${a.description} ${a.group}`.toLowerCase().includes(t))).sort((a,b)=>optinSort==='name'?a.label.localeCompare(b.label):optinSort==='selected'?Number(optinData.variables[b.key].enabled)-Number(optinData.variables[a.key].enabled)||a.label.localeCompare(b.label):0);}
function renderOptinAlerts(){return filteredOptinAlerts().map(a=>{const p=optinData.variables[a.key];return `<div class="alert-row"><label class="choice"><span>${a.label}<small>${a.description}</small></span><input type="checkbox" role="switch" aria-label="${a.label}" data-optin-alert="${a.key}" ${p.enabled?'checked':''}></label>${p.enabled&&a.input?`<div class="values">${a.presets?`<p>${a.input==='entries'?'Rides before your turn':'Minutes before start'}</p>${p.value&&!a.presets.map(String).includes(p.value)?`<p>Previous value: ${escape(p.value)}. Choose an available option.</p>`:''}<div class="pills" role="group" aria-label="${a.label} timing">${a.presets.map(value=>button('optin-value',`${value} ${a.input==='entries'?'rides':'min'}`,{extra:`data-key="${a.key}" data-value="${value}" aria-pressed="${p.value===String(value)}"`})).join('')}</div>`:`<label class="field">${a.label}: time<input type="time" data-optin-time="${a.key}" value="${escape(p.value)}"></label>`}</div>`:''}</div>`;}).join('')||'<p class="help" role="status">No matching opt-ins.</p>';}
function saveOptinPreferences(){const error=validateOptins(optinData);if(error){optinFeedback=error;optinFeedbackKind='error';refreshOptinFeedback();return;}try{localStorage.setItem(optinKey+':'+state.currentUserId,JSON.stringify({version:2,enabled:optinData.enabled,timeZone:optinData.timeZone,variables:optinData.variables}));optinDirty=false;optinFeedback=optinData.enabled?'Preferences saved in this browser.':'SMS off. Your choices are saved in this browser.';optinFeedbackKind='success';refreshOptinFeedback();notice('Preferences saved in this browser');}catch{optinFeedback='Could not save. Your choices are still here; try again.';optinFeedbackKind='error';refreshOptinFeedback();}}
function isOptinsExit(action,value){if(action==='back')return !optinGroup;if(action==='nav-group')return value!=='shows';if(action==='group-tab')return value!=='optins';if(action==='launch')return value!=='shows';return ['close','logout','not-you','switch-account','icon-home','icon-library','component-library','tab','person','add-account'].includes(action);}
function allowOptinsExit(){if(!optinDirty)return true;if(!confirm('Leave without saving your preference changes?'))return false;try{const raw=localStorage.getItem(optinKey+':'+state.currentUserId);optinData=raw?decodeOptins(raw):freshOptins();}catch{optinData=freshOptins();}optinDirty=false;optinFeedback='';optinFeedbackKind='';return true;}


function accountHeader(){const account=footer().replace(/^<footer[^>]*>/,'').replace(/<\/footer>$/,'');const back=['person','horse'].includes(view)||isOptinsView()&&optinGroup;return `${back?'<button class="back-link" data-action="back" aria-label="Back">‹</button>':''}${account}${themeToggle()}`;}
function footer(){const u=actor();return v3Footer(`${menu('account-menu',button('menu',escape(u?fullName(u):'Accounts'),{variant:'quiet',icon:'user',extra:'data-menu="account-menu" aria-haspopup="menu" aria-expanded="false"'}),`${u?`<div class="menu-caption">${escape(u.email||fullName(u))}</div>`:''}${menuItem('add-account','Add account','plus')}${menuItem(u?'logout':'login',u?'Log out':'Log in',u?'logout':'login')}`,'account-wrap')}<button type="button" class="text-button" data-action="not-you">Not you?</button>`);}
function pendingMarkup(){return canonical?.pendingSave?`<p class="feedback" role="status">${escape(canonical.pendingNeedsPin?'Pending: re-enter your PIN to retry.':canonical.pendingError?'Pending: '+canonical.pendingError:'Pending confirmation…')}</p>`:'';}
function sections(){
 if(!state.recognized)return section('one','Profile',`<p class="feedback" role="status">${escape(connectionError|| (loading?'Loading Profile…':'Sign in to open your Profile.'))}</p>${loading?'':button('login','Log in')}`,'')+section('two','Section','',0,true);
 if(libraryPage)return prototypeSections();
 if(['horses','riders','horse'].includes(view))return domainRosterSections();
 if(view==='person'||(ui.tab==='all'&&!['group','agent','optins','riders'].includes(view))){const u=selected(),own=u?.id===state.currentUserId;return section('one','Profile',`${pendingMarkup()}<div class="detail-grid">${[['first_name','First name'],['last_name','Last name'],['email','Email'],['sms','Phone'],['level','Account level'],['role','Team role']].map(([k,label])=>`<div class="detail-display ${k==='email'?'full':''}"><span class="detail-value">${escape(u?.[k]||'—')}</span><span class="detail-label">${label}</span></div>`).join('')}${locationField(u)}</div>`,'',false,own?'<button type="button" class="text-button" data-action="edit-profile">Edit</button>':'')+section('two','Team',teamControls(),'',!ui.secondary);}
 if(ui.tab==='team'&&!['group','agent','optins','riders'].includes(view)){const people=queryUsers(visiblePeople(),'',ui.sort);return section('one','Team',`${pendingMarkup()}${owner()?'<div class="record-actions"><button class="text-button" data-action="add-member">Add Team member</button></div>':''}${canonical.loaded?.roster===false?scopeMessage('roster','Team members'):personRows(people)}`,people.length)+section('two','Team settings',teamControls(),'');}
 return prototypeSections();
}
function teamControls(){
 if(canonical?.loaded?.team===false)return scopeMessage('team','Team details');
 if(canonical?.teamConflict)return '<p class="form-error">Multiple Team memberships require review.</p>';
 const own=membership(canonical),p=canonical?.profiles.find(p=>p.id===state.currentUserId);let html='';
 for(const i of canonical?.invitations||[]){if(i.userId!==state.currentUserId||i.status!=='pending')continue;const pending=canonical.pendingActions?.some(a=>a.invitationId===i.id&&!a.error);html+=`<p>${escape(canonical.teams?.find(t=>t.id===i.teamId)?.name||'Team')} invitation</p>${pending?'<p class="feedback">Pending confirmation…</p>':`<div class="record-actions">${button('accept-invite','Accept',{extra:`data-id="${escape(i.id)}"`})}${button('decline-invite','Decline',{extra:`data-id="${escape(i.id)}"`})}</div>`}`;}
 if(!own){html+='<p class="empty-result">No Team membership.</p>';if(p?.level==='Preferred'&&!p.hasMembership)html+=canonical.pendingActions?.some(a=>a.operation==='team_create'&&!a.error)?'<p class="feedback">Creating Team…</p>':button('create-team','Create Team');return html;}
 html+=`<p>${escape(state.tenant.name)} · ${escape(own.role)} · ${own.active?'Active':'Inactive'}</p>`;if(!own.active)return html;
 for(const m of canonical.memberships.filter(m=>m.teamId===own.teamId&&m.userId!==state.currentUserId)){const p=state.users.find(p=>p.id===m.userId);if(!p)continue;const hidden=(canonical.hidden||[]).some(x=>x[0]===state.currentUserId&&x[1]===own.teamId&&x[2]===m.userId);html+=`<div class="record-actions"><span>${escape(fullName(p))}</span>${m.active?button('hide-member',hidden?'Show':'Hide',{variant:'quiet',extra:`data-id="${escape(m.userId)}" data-value="${hidden?'show':'hide'}"`}):''}${owner()&&m.role==='Member'?button('member-status',m.active?'Active':'Inactive',{variant:'quiet',extra:`data-id="${escape(m.id)}"`}):''}</div>`;}
 for(const i of canonical.invitations||[])if(i.teamId===own.teamId&&i.status==='pending')html+=`<p class="feedback">Invitation pending: ${escape(fullName(state.users.find(p=>p.id===i.userId)))}</p>`;
 return html;
}
async function editProfile(mode='edit',person=selected()){
 if(mode==='account'){void signOutConnected(true);return;}
 if(mode==='member'||mode==='rider'){void inviteMember(mode);return;}
 if(!person||person.id!==state.currentUserId){notice('Only your own profile can be edited.');return;}
 if(canonical?.loaded?.profile===false){const generation=accountGeneration;try{await connection.load('profile',{priority:true});}catch(error){notice(error.message);return;}if(generation!==accountGeneration||person.id!==state.currentUserId)return;person=state.users.find(u=>u.id===person.id);if(!person)return;}
 const u=person;draftPhoto=u.profile_img||'';
 openDialog('Edit profile',`<form id="profile-form"><div class="profile-photo-editor"><span id="photo-preview">${avatar(u)}</span><div class="photo-options"><label class="ui-button upload-trigger">${icon('upload')}Upload photo<input type="file" id="photo-file" accept="image/png,image/jpeg,image/webp" hidden></label>${button('remove-photo','Remove')}</div></div><div class="form-grid">${[['first_name','First name','text'],['last_name','Last name','text'],['email','Email','email'],['sms','Phone','tel'],['pin','PIN','password']].map(([k,l,t])=>`<label class="field ${k==='email'?'full':''}">${l}<input name="${k}" type="${t}" value="${escape(k==='pin'?'':u[k])}" ${['first_name','last_name'].includes(k)?'maxlength="200"':''} ${k==='pin'?'maxlength="4" inputmode="numeric" placeholder="Leave blank to keep current PIN"':''} autocomplete="off"></label>`).join('')}<label class="field">Account level<input value="${escape(u.level)}" readonly></label><label class="field full">Photo URL<input name="photo_url" type="url" value="${escape(draftPhoto.startsWith('https:')?draftPhoto:'')}" placeholder="https://"></label><fieldset class="full"><legend>Tags</legend><div class="tag-options">${TAGS.map(t=>`<label class="tag-option"><input type="checkbox" name="tags" value="${t}" ${u.tags.includes(t)?'checked':''}><span>${t}</span></label>`).join('')}</div></fieldset></div><p class="form-error" id="form-error" role="alert"></p><div class="dialog-actions">${button('dialog-close','Cancel')}<button class="ui-button" data-variant="primary" type="submit">Save</button></div></form>`);editor={mode:'edit',userId:u.id,revision:u.revision};
}
function saveConnectedProfile(fd){try{if(editor?.userId!==state.currentUserId)throw Error('Profile changed.');const fields=profileFields(fd,draftPhoto);const prior=selected()?.tags||[];fields.tags=[...prior.filter(t=>fields.tags.includes(t)),...fields.tags.filter(t=>!prior.includes(t))];queueSave('profile',{fields,expectedRevision:editor.revision});closeDialog();draftPhoto='';render();}catch(error){$('#form-error').textContent=error.message;}}
function queueSave(operation,values){const generation=accountGeneration;const work=connection.save(operation,values);notice('Pending confirmation…');void work.then(()=>{if(generation===accountGeneration)notice(canonical?.pendingSave?'Pending confirmation…':'Saved');},error=>{if(generation===accountGeneration)notice('Pending: '+error.message);});return work;}
async function inviteMember(kind='member'){if(!owner())return;try{await connection.load('team');if(!owner())return;const pending=canonical.pendingActions||[],available=canonical.profiles.filter(p=>(kind!=='rider'||p.tags?.includes('rider'))&&!membership(canonical,p.id)&&!canonical.invitations.some(i=>i.userId===p.id&&i.status==='pending')&&!pending.some(a=>a.operation==='invitation_create'&&a.userId===p.id&&!a.error));openDialog(kind==='rider'?'Add rider':'Add Team member',`<form id="invite-form"><label class="field">Invite existing person<select name="userId">${available.map(p=>`<option value="${escape(p.id)}">${escape(p.name||[p.first,p.last].join(' '))}</option>`).join('')}</select></label>${available.length?'':'<p class="empty-result">No eligible people are available.</p>'}<p class="form-error" id="form-error" role="alert"></p><div class="dialog-actions">${button('dialog-close','Cancel')}<button class="ui-button" data-variant="primary" ${available.length?'':'disabled'}>Invite</button></div></form>`);}catch(error){notice(error.message);}}
async function loginConnected(fd,form){const generation=accountGeneration,submit=form.querySelector('button[type="submit"],button[data-variant="primary"]');submit.disabled=true;try{const next=await connection.login(String(fd.get('credential')||''));if(!next||next.identity.currentId!==state.currentUserId)return;closeDialog();render();void prepareTeam();}catch(error){if(form.isConnected)form.querySelector('#form-error').textContent=error.message;}finally{if(form.isConnected)submit.disabled=false;}}
async function signOutConnected(login){closeDialog();const work=connection.signOut();view='home';render();try{await work;}catch{notice('Sign-out pending; private data has been cleared.');}}
function acceptConnected(next,error){
 const previous=state.currentUserId,following=next?.identity?.currentId||null;
 if(previous!==following){ringSample=null;clearTimeout(ringExpiry);}
 if(previous!==following){scheduleBoundsObserver?.disconnect();scheduleBoundsObserver=null;scheduleBox=null;scheduleFrame?.remove();scheduleFrame=null;scheduleReady=false;scheduleRequested=false;scheduleTrigger=null;}
 if(previous!==following){delete document.body.dataset.mobileScroll;accountGeneration++;scopeErrors={};clearTimeout(noticeTimer);$('#notice').hidden=true;closeDialog();selectedId=following;query='';draftPhoto='';ui={...defaults,collapsed:{},activity:[]};if(following){try{ui={...ui,...JSON.parse(localStorage.getItem(UI_KEY+following)||'{}'),activity:[],agentMessages:[],horses:JSON.parse(localStorage.getItem('rs_profile_v3_horses:'+following)||'[]')};}catch{}}optinData=freshOptins();optinDirty=false;optinGroup=null;optinFeedback='';if(following){try{const raw=localStorage.getItem(optinKey+':'+following);if(raw)optinData=decodeOptins(raw);}catch{}}}
 if(previous!==following&&following){applyNavigationParams();if(!website)$('#app').hidden=false;}
 canonical=next;state=presentation(next);loading=false;connectionError=error?.message||'';if(!canRead(next,selectedId))selectedId=state.currentUserId;
 render();void recognizeWidget?.refresh().then(syncIdentityButton).catch(()=>{});if(error&&state.recognized)notice((canonical?.pendingSave?'Pending: ':'Unable to load: ')+error.message);
}
function scopeMessage(scope,label){return `<p class="feedback" role="status">${escape(scopeErrors[scope]||'Loading '+label+'…')}</p>${scopeErrors[scope]?button('retry-scope','Retry',{extra:`data-value="${scope}"`}):''}`;}
async function requestScope(scope,priority=false){if(!state.recognized)return;const generation=accountGeneration;delete scopeErrors[scope];try{await connection.load(scope,{priority,background:!priority});}catch(error){if(generation===accountGeneration){scopeErrors[scope]=error.message;render();}}}
async function prepareTeam(){if(!state.recognized)return;const generation=accountGeneration,prioritizeHorses=['horses','horse'].includes(view);if(prioritizeHorses)await requestScope('horses');await requestScope('roster');if(generation===accountGeneration&&state.recognized)await requestScope('team');if(generation===accountGeneration&&state.recognized&&!prioritizeHorses)await requestScope('horses');}
function handleDataAction(e){const b=e.target.closest('[data-action]');if(!b||!state.recognized)return;const {action,id,value}=b.dataset;
 if(action==='theme'){
  const theme=document.documentElement.dataset.theme==='light'?'dark':'light';
  document.documentElement.dataset.theme=theme;recognizeRoot.dataset.theme=theme;
  document.querySelector('meta[name="theme-color"]').content=theme==='light'?'#ffffff':'#191919';
  b.outerHTML=themeToggle();renderPreviewTools();return;
 }
 if(action==='measure-ring'){if(ui.locationAllowed?.[state.currentUserId]===true)requestProfileLocation(true);return;}
 if(action==='retry-scope'){void requestScope(value,true);return;}
 if(action==='launch'&&value==='horses'&&!canonical?.loaded?.horses)void requestScope('horses',true);
 if((action==='tab'&&value==='team')||(action==='launch'&&['team','riders'].includes(value))||(action==='nav-group'&&value==='team')){if(canonical.loaded?.roster===false)void requestScope('roster',true);if(canonical.loaded?.team===false)void requestScope('team',true);}
 if(action==='hide-member'){connection.hide(id,value==='hide');return;}
 if(action==='member-status'&&owner()){const m=canonical.memberships.find(m=>m.id===id);if(m&&m.role==='Member'&&m.teamId===membership(canonical)?.teamId)queueSave('member_status',{membershipId:m.id,active:!m.active,expectedRevision:m.revision});return;}
 if(action==='accept-invite'||action==='decline-invite'){const i=canonical.invitations.find(i=>i.id===id&&i.userId===state.currentUserId&&i.status==='pending');if(i)queueSave('invitation_response',{invitationId:id,accept:action==='accept-invite',expectedRevision:i.revision});return;}
 if(action==='create-team'){openDialog('Create Team',`<form id="team-form"><label class="field">Team name<input name="name" required maxlength="120"></label><div class="dialog-actions">${button('dialog-close','Cancel')}<button class="ui-button" data-variant="primary">Create Team</button></div></form>`);}
}
document.addEventListener('click',handleDataAction);
document.addEventListener('submit',e=>{const form=e.target,fd=new FormData(form);if(form.id==='invite-form'&&owner()){queueSave('invitation_create',{userId:String(fd.get('userId'))});closeDialog();}if(form.id==='team-form'){queueSave('team_create',{name:String(fd.get('name')||'').trim()});closeDialog();}});
connection=createConnection(service,recognitionAdapter,acceptConnected);
function syncIdentityButton(){const button=recognizeRoot.shadowRoot?.querySelector('#launcher');if(!button)return;const open=state.recognized&&!$('#app').hidden;button.setAttribute('aria-pressed',String(open));if(state.recognized){button.setAttribute('aria-expanded',String(open));button.setAttribute('aria-label','Profile');}}
recognizeWidget=mountRecognize(recognizeRoot,{
 profileIcon:icon('user'),onProfile:()=>launch('profile'),
 css:'@import url("/test/rs-profile-v3/primitives.css");@import url("/test/rs-profile-v3/styles.css");',
 markup:`<form id="signin" class="v3-panel" data-height="content" data-placement="floating" aria-label="Sign in" hidden><header class="stack stack-top"><div class="back-row" data-stack="back" aria-hidden="true"></div>${brand().replace('data-action="close"','id="close" type="button"').replace('Close Profile','Close sign in')}</header><div class="stack-scroll"><div class="stack stack-body"><section class="section" data-stack="section"><div class="form-grid"><div class="field"><input id="credential" name="credential" aria-label="Phone, PIN, or Invite" placeholder="Phone, PIN, or Invite" autocomplete="off" inputmode="numeric" required></div><p class="form-error" id="error" role="alert"></p><button class="ui-button primary" data-variant="primary" type="submit">Sign in</button></div></section></div></div><footer class="footer-row" data-stack="footer" aria-hidden="true"></footer></form><button id="launcher" type="button" aria-controls="signin" aria-expanded="false"></button>`,
 adapter:{current:async()=>state.currentUserId,recognize:async value=>(await connection.login(value))?.identity?.currentId||null,signOut:()=>connection.signOut()},onRecognized:()=>{render();void prepareTeam();}});
let stopRefresh=null;
function beginRefresh(){if(!stopRefresh&&!libraryPage)stopRefresh=startProfileRefresh({refresh:()=>service.refreshProfile(),ready:()=>state.recognized});}
void connection.start().then(()=>prepareTeam()).catch(()=>{}).finally(beginRefresh);
addEventListener('pagehide',()=>{stopRefresh?.();stopRefresh=null;});
addEventListener('pageshow',event=>{if(event.persisted)beginRefresh();});
addEventListener('online',()=>{void connection.start().then(()=>prepareTeam()).catch(()=>{});});
addEventListener('storage',e=>{if(['rs_recognition_pending_signout_v1','rs_recognition_device_token_v1'].includes(e.key)){service.clearProfile();void connection.start().then(()=>prepareTeam()).catch(()=>{});}});

if(website){
 let boundsPending=false;
 const reportBounds=()=>{if(boundsPending)return;boundsPending=true;requestAnimationFrame(()=>{
  boundsPending=false;
  const roots=[$('#app'),launcherRoot.ownerDocument===document?launcherRoot:null,$('#dialog'),$('#notice'),scheduleFrame];
  if(!recognizeRoot.hidden)roots.push(...recognizeRoot.shadowRoot.querySelectorAll('#signin,#launcher'));
  const boxes=roots.filter(el=>el&&(!el.hidden||el.getAnimations().some(a=>a.playState==='running'))&&(el.tagName!=='DIALOG'||el.open)).map(el=>el===scheduleFrame&&scheduleBox?scheduleBox:el.getBoundingClientRect()).filter(r=>r.width&&r.height);
  const top=boxes.length?Math.max(0,Math.min(...boxes.map(r=>r.top))-4):innerHeight;
  const left=boxes.length?Math.max(0,Math.min(...boxes.map(r=>r.left))-4):innerWidth;
  const right=boxes.length?Math.max(0,innerWidth-Math.max(...boxes.map(r=>r.right))-4):0;
  const bottom=boxes.length?Math.max(0,innerHeight-Math.max(...boxes.map(r=>r.bottom))-4):0;
  const drawerPanels=[$('#app'),recognizeRoot.shadowRoot.querySelector('#signin')];
  const animating=drawerPanels.some(el=>el.getAnimations().some(a=>a.playState==='running'));
  const appOpen=drawerPanels.some(el=>!el.hidden)||animating||!!(scheduleFrame&&!scheduleFrame.hidden);
  const theme=scheduleFrame&&!scheduleFrame.hidden?scheduleFrame.contentDocument?.documentElement.dataset.theme:document.documentElement.dataset.theme;
  if(document.body.dataset.appOpen!==String(appOpen))document.body.dataset.appOpen=String(appOpen);
  const launcherHeight=hostSchedule&&!residentLauncherSlot&&mobileLauncher.matches&&!launcherRoot.hidden&&$('#app').hidden?launcherRoot.getBoundingClientRect().height:0;
  parent.postMessage({type:'rs-profile-bounds',top,right,bottom,left,appOpen,theme,...(hostSchedule?{launcherHeight}:{})},location.origin);
  if(animating)reportBounds();
 });};
 const observer=new MutationObserver(reportBounds);observer.observe(document.documentElement,{attributes:true,attributeFilter:['data-theme']});observer.observe(document.body,{subtree:true,childList:true,attributes:true});observer.observe(recognizeRoot.shadowRoot,{subtree:true,childList:true,attributes:true});const resizeObserver=new ResizeObserver(reportBounds);[$('#app'),launcherRoot,$('#dialog'),...recognizeRoot.shadowRoot.querySelectorAll('#signin,#launcher')].forEach(el=>resizeObserver.observe(el));addEventListener('resize',reportBounds);reportBounds();
}

// Lock standalone desktop page scrolling only while its existing drawer is visible.
const drawerDesktop=matchMedia('(min-width:601px)');
let drawerScrollState=null;
function syncDrawerScroll(){
 const open=drawerDesktop.matches&&[$('#app'),recognizeRoot.shadowRoot?.querySelector('#signin')].some(el=>el&&getComputedStyle(el).display!=='none');
 if(open&&!drawerScrollState){drawerScrollState={x:scrollX,y:scrollY,overflow:document.body.style.overflow};document.body.style.overflow='hidden';}
 else if(!open&&drawerScrollState){const saved=drawerScrollState;drawerScrollState=null;document.body.style.overflow=saved.overflow;window.scrollTo(saved.x,saved.y);}
}
const drawerObserver=new MutationObserver(syncDrawerScroll);
for(const el of [$('#app'),recognizeRoot.shadowRoot.querySelector('#signin')]){drawerObserver.observe(el,{attributes:true,attributeFilter:['hidden']});el.addEventListener('transitionend',syncDrawerScroll);el.addEventListener('transitioncancel',syncDrawerScroll);}
drawerDesktop.addEventListener('change',syncDrawerScroll);syncDrawerScroll();

// Location uses the existing scoped Astro location endpoint.
let locationPermission='prompt',locationRequest=0;
function locationField(user){
 if(user?.id!==state.currentUserId)return '';
 const enabled=ui.locationAllowed?.[state.currentUserId]===true&&locationPermission!=='denied';
 return `<div class="detail-display" data-location-field><span class="detail-label">Location</span><label class="visibility-row"><span>Allow precise location</span><input type="checkbox" data-location-permission ${enabled?'checked':''}></label><span class="detail-label">Your browser will ask for permission.</span><button type="button" class="text-button" data-action="location-policy">Location privacy policy</button></div>`;
}
function updateLocationField(){
 refreshRingDistance();
 const input=document.querySelector('[data-location-permission]');
 if(input){input.checked=ui.locationAllowed?.[state.currentUserId]===true&&locationPermission!=='denied';input.disabled=false;}
}
async function readLocationPermission(){
 try{const permission=await navigator.permissions.query({name:'geolocation'});locationPermission=permission.state;updateLocationField();permission.onchange=()=>{locationPermission=permission.state;if(permission.state!=='granted'){locationRequest++;void service.updateProfileLocation(null);}updateLocationField();};}catch{}
}
document.addEventListener('change',event=>{
 const input=event.target;if(!input.matches('[data-location-permission]'))return;
 requestProfileLocation(input.checked);
});
function requestProfileLocation(allowed){
 const person=state.currentUserId,preferences=ui,request=++locationRequest;
 if(!person)return;
 // Save the choice immediately; coordinates and log confirmation run in the background.
 if(!allowed)ringSample=null;
 ui.locationAllowed={...ui.locationAllowed,[person]:allowed};saveUI();updateLocationField();
 const current=()=>request===locationRequest&&state.recognized&&state.currentUserId===person&&ui===preferences;
 if(!allowed||!navigator.geolocation){void service.updateProfileLocation(null);return;}
 navigator.geolocation.getCurrentPosition(position=>{
  if(!current())return;
  ringSample={uid:person,source:'gps',latitude:position.coords.latitude,longitude:position.coords.longitude,accuracy:position.coords.accuracy,observedAt:new Date(position.timestamp||Date.now()).toISOString()};
  locationPermission='granted';updateLocationField();
  const {latitude,longitude,accuracy,observedAt}=ringSample;
  void service.updateProfileLocation({latitude,longitude,accuracy,observedAt});
 },error=>{
  if(!current())return;
  ringSample=null;
  if(error.code===1){locationPermission='denied';ui.locationAllowed={...ui.locationAllowed,[person]:false};saveUI();updateLocationField();notice('Location was not allowed. You can change this in your browser site settings.');}
  // Timeout or unavailable coordinates do not change permission. Fall back quietly.
  void service.updateProfileLocation(null);refreshRingDistance();
 },{enableHighAccuracy:true,timeout:10000,maximumAge:0});
}
document.addEventListener('click',event=>{
 if(!event.target.closest('[data-action="location-policy"]'))return;
 openDialog('Location privacy policy','<p class="dialog-note">Precise location is optional. Checking this box requests location permission from your browser for this device. When allowed, coordinates are sent through the existing RingStatus Astro connection and saved to your Profile in Airtable, with a working copy on this device. Approximate GeoIP is used when precise location is unavailable.</p><p class="dialog-note">Your choice is saved on this device for your Profile. Unchecking stops requests from this control; it does not revoke browser permission. You can revoke permission in your browser site settings. Unchecking clears the current location cache and uses approximate GeoIP instead. Previously captured logs remain pending until saved, and confirmed location history is retained.</p><div class="dialog-actions">'+button('dialog-close','Done',{variant:'primary'})+'</div>','button');
});
void readLocationPermission();
addEventListener('focus',readLocationPermission);
