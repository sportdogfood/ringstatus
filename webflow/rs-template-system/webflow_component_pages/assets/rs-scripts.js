// RS Skeleton JS — locked base contract v2026-05-21
(function(){
  const body=document.body,megaToggle=document.getElementById('data-mega-toggle'),megaMenu=document.getElementById('data-mega-menu'),drawerToggle=document.getElementById('data-drawer-toggle'),drawerMenu=document.getElementById('data-drawer-menu'),drawerClose=document.getElementById('data-drawer-close'),scrim=document.getElementById('data-nav-scrim');
  if(!body||!megaToggle||!megaMenu||!drawerToggle||!drawerMenu||!drawerClose||!scrim)return;
  function setMega(open){megaMenu.classList.toggle('is-open',open);megaMenu.setAttribute('aria-hidden',String(!open));megaToggle.setAttribute('aria-expanded',String(open));megaToggle.classList.toggle('is-active',open)}
  function setDrawer(open){drawerMenu.classList.toggle('is-open',open);scrim.classList.toggle('is-open',open);body.classList.toggle('is-nav-locked',open);drawerMenu.setAttribute('aria-hidden',String(!open));drawerToggle.setAttribute('aria-expanded',String(open));drawerToggle.classList.toggle('is-active',open);if(open)setMega(false)}
  megaToggle.addEventListener('click',()=>{const open=megaMenu.classList.contains('is-open');setDrawer(false);setMega(!open)});
  drawerToggle.addEventListener('click',()=>setDrawer(!drawerMenu.classList.contains('is-open')));
  drawerClose.addEventListener('click',()=>setDrawer(false));scrim.addEventListener('click',()=>setDrawer(false));
  document.addEventListener('keydown',e=>{if(e.key==='Escape'){setMega(false);setDrawer(false)}});
  document.addEventListener('click',e=>{if(!megaMenu.contains(e.target)&&!megaToggle.contains(e.target))setMega(false)});
})();
(function(){
  document.querySelectorAll('[data-list-limit]').forEach(panel=>{
    const step=Number(panel.getAttribute('data-list-limit'))||10,rows=[...panel.querySelectorAll('.rs-full-list-row')],more=panel.querySelector('[data-list-toggle]'),less=panel.querySelector('[data-list-less]');let visible=step;
    function render(){rows.forEach((row,i)=>row.classList.toggle('is-hidden',i>=visible));if(more){const remaining=rows.length-visible;more.hidden=remaining<=0;more.textContent=remaining>step?'See 10 more':'See '+remaining+' more'}if(less)less.hidden=visible<=step}
    if(more)more.addEventListener('click',()=>{visible=Math.min(visible+step,rows.length);render()});
    if(less)less.addEventListener('click',()=>{visible=step;render();panel.scrollIntoView({behavior:'smooth',block:'start'})});
    render();
  });
})();
