import {mountPersonalPreferences} from '../rs-inputs/native-alerts.js';
import {definitions} from './optins-catalog.js';

const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const groups=['Classes','Rider trips','Groom tasks'];
const paths=['<path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/>','<path d="m7 17 10-10M7 7h10v10"/>','<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>'];
const icon=path=>`<svg class="icon" viewBox="0 0 24 24" aria-hidden="true">${path}</svg>`;

export function optinsMarkup(profile){
 return `<div data-profile-optins><div data-rs-main><form data-rs-form="alerts">
 <input data-rs-field="phone" value="${escape(profile.phone)}" hidden readonly><span data-rs-phone-hint hidden></span>
 <div data-optins-overview><label class="choice"><span>SMS alerts<small>Pause without losing your choices.</small></span><input type="checkbox" data-rs-sms role="switch" aria-label="SMS alerts"></label>
 <div class="section-label">Preferences</div>
 ${groups.map((g,i)=>`<button type="button" class="nav-row" data-optins-group="${g}">${icon(paths[i])}<span class="row-copy"><span class="row-title">${g}</span><span class="row-meta">${['Starts, changes and progress','Reminders, order and results','Reminders at your chosen times'][i]}</span></span><span class="count" data-optins-count="${g}"></span><span class="chevron">›</span></button>`).join('')}
 <div class="mobile-summary"><div><span class="mobile-label">Mobile number</span><span data-optins-phone>${escape(profile.phone||'Managed in Profile')}</span></div><button type="button" class="text-button" data-go="profile">Edit in Profile ›</button></div>
 <p class="prototype-note">Preferences are saved to your RingStatus profile. Alert delivery is not active.</p></div>
 <div class="list-tools" data-optins-tools hidden><button type="button" class="search-button" data-optins-search-toggle aria-label="Search opt-ins" aria-expanded="false">${icon('<circle cx="10" cy="10" r="6"/><path d="m15 15 6 6"/>')}<span>Search</span></button><label class="field" data-optins-search-box hidden><input type="search" data-optins-search aria-label="Search opt-ins" placeholder="Search opt-ins" autocomplete="off"></label><div class="sort-wrap"><button type="button" class="sort-toggle" data-optins-sort-toggle aria-label="Sort opt-ins" aria-expanded="false"><span data-optins-sort-label>Default order</span><span aria-hidden="true">⌄</span></button><div class="sort-menu" data-optins-sort-menu hidden>${[['category','Default order'],['name','A–Z Name'],['selected','Selected first']].map(([key,label])=>`<button type="button" data-optins-sort="${key}" aria-pressed="${key==='category'}">${label}</button>`).join('')}</div></div></div>
 <div data-optins-detail hidden><p class="help" data-optins-help>Choose the updates you want to receive.</p><p class="help" data-rs-time-zone hidden></p><p class="help" data-optins-empty hidden>No matching opt-ins.</p>
 <div data-optins-rows>${definitions.map(a=>`<div class="alert-row" data-optins-row="${a.key}"><label class="choice"><span>${escape(a.label)}<small>${escape(a.description)}</small></span><input type="checkbox" role="switch" aria-label="${escape(a.label)}" data-rs-alert="${a.key}"></label>${a.input?`<div class="values" data-rs-alert-panel="${a.key}" hidden>${a.presets?`<p>${a.input==='entries'?'Rides before your turn':'Minutes before start'}</p><p class="help" data-rs-prior-value="${a.key}" hidden></p><div class="pills" role="group" aria-label="${escape(a.label)} timing">${a.presets.map(v=>`<button type="button" data-rs-action="alert-value" data-key="${a.key}" data-value="${v}" aria-pressed="false">${v} ${a.input==='entries'?'rides':'min'}</button>`).join('')}</div>`:`<label class="field">${escape(a.label)}: time<input type="time" data-rs-alert-time="${a.key}" aria-label="${escape(a.label)}: time"></label>`}</div>`:''}</div>`).join('')}</div></div>
 <footer class="screen-footer"><p class="form-error" data-rs-error role="alert" hidden></p><p class="feedback" data-rs-status role="status" aria-live="polite" hidden></p><p class="feedback" data-rs-alert-count></p><button type="submit" class="primary">Save preferences</button></footer>
 </form></div></div>`;
}

export function mountProfileOptins(root,{panel,getActor,getProfile}){
 let group=null,query='',sort='category',latest=null,disposed=false;
 const $=selector=>root.querySelector(selector),title=panel.querySelector('.screen-title'),back=panel.querySelector('.back-row button');
 const rows=new Map([...root.querySelectorAll('[data-optins-row]')].map(el=>[el.dataset.optinsRow,el]));
 function refresh(){
  if(disposed)return;
  $('[data-optins-overview]').hidden=!!group;$('[data-optins-tools]').hidden=!group;$('[data-optins-detail]').hidden=!group;
  title.textContent=group||'Opt-ins';back.innerHTML=`<span aria-hidden="true">‹</span> ${group?'Opt-ins':'Profile'}`;back.setAttribute('aria-label',group?'Back to Opt-ins':'Back to Profile');
  const variables=latest?.data.variables||{};
  for(const g of groups){const list=definitions.filter(a=>a.group===g);$(`[data-optins-count="${g}"]`).textContent=`${list.filter(a=>variables[a.key]?.enabled).length}/${list.length}`;}
  const terms=query.toLowerCase().trim().split(/\s+/).filter(Boolean);
  const sorted=[...definitions].sort((a,b)=>sort==='name'?a.label.localeCompare(b.label):sort==='selected'?Number(!!variables[b.key]?.enabled)-Number(!!variables[a.key]?.enabled)||a.label.localeCompare(b.label):groups.indexOf(a.group)-groups.indexOf(b.group));
  let found=0;for(const a of sorted){const row=rows.get(a.key);const shown=a.group===group&&terms.every(t=>`${a.label} ${a.description}`.toLowerCase().includes(t));row.hidden=!shown;if(shown)found++;$('[data-optins-rows]').append(row);}
  $('[data-optins-empty]').hidden=!group||!!found;$('[data-rs-time-zone]').hidden=group!=='Groom tasks';$('[data-optins-help]').hidden=group==='Groom tasks';
  const profile=getProfile();$('[data-optins-phone]').textContent=profile?.phone||'Managed in Profile';
  if(latest){const selected=definitions.filter(a=>variables[a.key]?.enabled).length;$('[data-rs-alert-count]').textContent=`${selected} of 18 selected · ${latest.data.enabled?'SMS on':'SMS off'}`;}
 }
 function closeMenu(){$('[data-optins-sort-menu]').hidden=true;$('[data-optins-sort-toggle]').setAttribute('aria-expanded','false');}
 function resetGroup(){group=null;query='';sort='category';$('[data-optins-search]').value='';$('[data-optins-search-box]').hidden=true;$('[data-optins-search-toggle]').setAttribute('aria-expanded','false');closeMenu();$('[data-optins-sort-label]').textContent='Default order';for(const b of root.querySelectorAll('[data-optins-sort]'))b.setAttribute('aria-pressed',String(b.dataset.optinsSort==='category'));refresh();}
 function onBack(event){if(!group)return;event.preventDefault();event.stopImmediatePropagation();const previous=group;resetGroup();panel.scrollTop=0;$(`[data-optins-group="${previous}"]`).focus();}
 function onClick(event){if(!event.target.closest('.sort-wrap'))closeMenu();const button=event.target.closest('button');if(!button)return;
  if(button.dataset.optinsGroup){group=button.dataset.optinsGroup;query='';refresh();panel.scrollTop=0;back.focus();}
  else if(button.hasAttribute('data-optins-search-toggle')){const box=$('[data-optins-search-box]');box.hidden=!box.hidden;button.setAttribute('aria-expanded',String(!box.hidden));if(!box.hidden)$('[data-optins-search]').focus();else{query='';$('[data-optins-search]').value='';refresh();}}
  else if(button.hasAttribute('data-optins-sort-toggle')){const menu=$('[data-optins-sort-menu]');menu.hidden=!menu.hidden;button.setAttribute('aria-expanded',String(!menu.hidden));}
  else if(button.dataset.optinsSort){sort=button.dataset.optinsSort;for(const b of root.querySelectorAll('[data-optins-sort]'))b.setAttribute('aria-pressed',String(b===button));$('[data-optins-sort-label]').textContent=button.textContent;$('[data-optins-sort-menu]').hidden=true;$('[data-optins-sort-toggle]').setAttribute('aria-expanded','false');refresh();}
 }
 function onInput(event){if(event.target.hasAttribute('data-optins-search')){query=event.target.value;refresh();}}
 function onKeydown(event){if(event.key==='Escape'&&!$('[data-optins-sort-menu]').hidden){event.stopPropagation();closeMenu();$('[data-optins-sort-toggle]').focus();}}
 back.addEventListener('click',onBack);root.addEventListener('click',onClick);root.addEventListener('input',onInput);root.addEventListener('keydown',onKeydown);
 const core=mountPersonalPreferences(root,{getActor,getPhone:()=>getProfile()?.phone||'',saveLabel:'Save preferences',onUpdate:value=>{latest=value;refresh();}});
 refresh();
 return {ready:core.ready,destroy(){disposed=true;core.destroy();back.removeEventListener('click',onBack);root.removeEventListener('click',onClick);root.removeEventListener('input',onInput);root.removeEventListener('keydown',onKeydown);}};
}
