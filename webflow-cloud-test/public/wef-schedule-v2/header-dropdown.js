import {icon as v3Icon} from '../rs-profile-v3/icons.js';
import {v3Control,v3Search} from '../rs-profile-v3/components.js';
// Date menu and searchable trainer modal retain the existing select/change state.
export function headerDropdowns(host){
 const menu=document.createElement('div');
 menu.className='ui-menu v2-filter-menu';menu.id='v2-filter-menu';menu.setAttribute('popover','auto');menu.setAttribute('role','listbox');document.body.append(menu);
 const trainerDialog=document.createElement('dialog');trainerDialog.className='ui-dialog v2-trainer-dialog';trainerDialog.setAttribute('aria-labelledby','v2-trainer-title');
 trainerDialog.innerHTML=`<header class="dialog-head"><h2 id="v2-trainer-title">Trainers</h2><button type="button" class="icon-button" aria-label="Close trainers">${v3Icon('close')}</button></header>${v3Search('Search trainers','trainer-search',v3Icon('search'),'Search trainers')}<div class="search-dialog-results results v2-trainer-options" data-layout="list" data-striped role="listbox" aria-label="Trainers"></div><p class="empty-result v2-trainer-empty" role="status" hidden>No matching trainers</p>`;
 document.body.append(trainerDialog);
 const search=trainerDialog.querySelector('input'),trainerList=trainerDialog.querySelector('[role=listbox]'),empty=trainerDialog.querySelector('[role=status]');
 let trainerActive=null;
 function renderTrainers(){
  trainerList.replaceChildren();const query=search.value.trim().toLocaleLowerCase();let count=0;
  Array.from(trainerActive.select.options).forEach((option,index)=>{
   if(query&&index!==0&&!option.textContent.toLocaleLowerCase().includes(query))return;
   if(query&&index===0)return;
   const item=document.createElement('button');item.type='button';item.className='result-row';item.setAttribute('role','option');item.setAttribute('aria-selected',String(option.selected));const copy=document.createElement('span');copy.className='result-copy';const name=document.createElement('strong');name.textContent=option.textContent;copy.append(name);item.append(copy);item.title=option.textContent;
   item.disabled=option.disabled||trainerActive.select.options.length===1;
   item.addEventListener('click',()=>{trainerActive.select.selectedIndex=index;trainerActive.select.dispatchEvent(new Event('change',{bubbles:true}));trainerDialog.close();});trainerList.append(item);count++;
  });empty.hidden=count>0;
 }
 search.addEventListener('input',renderTrainers);
 trainerDialog.querySelector('[aria-label="Close trainers"]').addEventListener('click',()=>trainerDialog.close());
 trainerDialog.addEventListener('close',()=>{trainerActive?.button.setAttribute('aria-expanded','false');trainerActive?.button.focus();});
 let active=null;
 function close(){menu.hidePopover();active?.button.setAttribute('aria-expanded','false');}
 function choose(index){
  if(!active)return;
  const {select,button}=active;
  select.selectedIndex=index;select.dispatchEvent(new Event('change',{bubbles:true}));close();button.focus();
 }
 function open(select,button){
  if(select.getAttribute('aria-label')==='Trainer'){
   close();trainerActive={select,button};search.value='';trainerDialog.querySelector('h2').textContent='Trainers - '+select.dataset.dayLabel;search.closest('.search-control').hidden=select.options.length===1;search.hidden=select.options.length===1;renderTrainers();trainerList.hidden=select.options.length===1;if(search.hidden){empty.hidden=false;empty.textContent='No trainer-linked entries for '+select.dataset.dayLabel+'.';}else empty.textContent='No matching trainers';trainerDialog.showModal();button.setAttribute('aria-expanded','true');if(!search.hidden)search.focus();return;
  }
  active={select,button};menu.replaceChildren();menu.setAttribute('aria-label',select.getAttribute('aria-label'));
  Array.from(select.options).forEach((option,index)=>{
   const item=document.createElement('button');item.type='button';item.className='v2-filter-option';item.setAttribute('role','option');item.setAttribute('aria-selected',String(option.selected));
   item.textContent=/^\d{4}-\d{2}-\d{2}$/.test(option.value)?(option.textContent==='Today'?'Today - ':'')+new Intl.DateTimeFormat('en-US',{weekday:'long',month:'short',day:'numeric',timeZone:'UTC'}).format(new Date(option.value+'T12:00:00Z')):option.textContent;item.title=item.textContent;item.disabled=option.disabled||select.options.length===1;item.addEventListener('click',()=>choose(index));menu.append(item);
  });
 }
 function positionMenu(){
  if(!active)return;
  const {button}=active,rect=button.getBoundingClientRect(),width=menu.getBoundingClientRect().width;
  const above=button.dataset.placement==='top';
  menu.style.maxHeight=Math.max(44,Math.min(320,above?rect.top-16:innerHeight-rect.bottom-16))+'px';
  menu.style.left=Math.max(8,Math.min(innerWidth-width-8,rect.right-width))+'px';
  menu.style.top=(above?Math.max(8,rect.top-menu.getBoundingClientRect().height-8):rect.bottom+8)+'px';
  (menu.querySelector('[aria-selected=true]:not(:disabled)')||menu.querySelector('button:not(:disabled)')||menu).focus();
 }
 menu.tabIndex=-1;
 menu.addEventListener('toggle',()=>{const opened=menu.matches(':popover-open');active?.button.setAttribute('aria-expanded',String(opened));if(opened)positionMenu();});
 menu.addEventListener('keydown',event=>{
  const items=Array.from(menu.querySelectorAll('button:not(:disabled)')),index=items.indexOf(document.activeElement);
  if(['ArrowDown','ArrowUp','Home','End'].includes(event.key)){
   event.preventDefault();const next=event.key==='Home'?0:event.key==='End'?items.length-1:event.key==='ArrowDown'?(index+1)%items.length:(index-1+items.length)%items.length;items[next]?.focus();
  }else if(event.key==='Escape'){event.preventDefault();close();active?.button.focus();}
 });
 addEventListener('resize',close);
 return (select,svg,{className='v2-header-filter',placement='bottom'}={})=>{
  const button=document.createElement('button');button.type='button';button.className=className;button.dataset.placement=placement;button.innerHTML=svg;v3Control(button,{icon:className==='v2-header-filter'});
  const trainer=select.getAttribute('aria-label')==='Trainer';trainerDialog.id='v2-trainer-dialog';button.setAttribute('aria-label',select.getAttribute('aria-label'));button.setAttribute('aria-haspopup',trainer?'dialog':'listbox');button.setAttribute('aria-controls',trainer?trainerDialog.id:menu.id);button.setAttribute('aria-expanded','false');
  select.hidden=true;select.tabIndex=-1;host.append(button,select);
  if(!trainer)button.setAttribute('popovertarget',menu.id);
  button.addEventListener('click',()=>open(select,button));
  button.addEventListener('keydown',event=>{if(event.key==='ArrowDown'){event.preventDefault();button.click();}});
  return button;
 };
}
