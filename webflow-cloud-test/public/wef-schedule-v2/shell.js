import {v3Back,v3Brand,v3Shell,v3Footer,v3Control} from '../rs-profile-v3/components.js';
const shell=document.querySelector('.rings-shell');
if(window.frameElement?.id==='profile-schedule-view')document.body.dataset.embedded='true';
const back=document.querySelector('.rings-profile-nav a');back.className='back-link';
const oldHeader=document.querySelector('.rings-header');
const controls=Array.from(oldHeader.children).filter(node=>!node.classList.contains('rings-brand')) ;
const theme=controls.splice(controls.findIndex(node=>node.classList.contains('v2-theme-toggle')),1)[0];
const content=document.querySelector('.rings-scroll');oldHeader.remove();
const bottom=document.querySelector('.v2-page-nav');
const horses=document.querySelector('.rings-horse-actions');
shell.classList.add('v3-panel');shell.setAttribute('role','main');shell.setAttribute('aria-label',content.getAttribute('aria-label')||'Schedule');shell.dataset.width='schedule';
shell.innerHTML=v3Shell(v3Back(back.outerHTML)+v3Brand(`<div class="action-icons"></div>`),content.innerHTML,v3Footer('<span class="rings-shell-footer ui-button" data-variant="quiet" role="status"></span>'),horses.outerHTML+bottom.outerHTML);
shell.querySelector('.action-icons').append(...controls);
const status=shell.querySelector('.rings-shell-footer');
shell.querySelector('.back-row').append(status,theme);
shell.querySelector(':scope > .footer-row').remove();
const launcherSlot=document.createElement('div');launcherSlot.className='schedule-launcher-slot';launcherSlot.dataset.profileLauncherSlot='';launcherSlot.hidden=true;shell.append(launcherSlot);
shell.querySelector('.stack-scroll').classList.add('rings-scroll');
shell.querySelector('.brand-row').classList.add('rings-header');
shell.querySelectorAll('.rings-pill').forEach(node=>v3Control(node));
shell.querySelectorAll('.v2-theme-toggle').forEach(node=>v3Control(node,{icon:true}));

// Direction comes from user input, never from layout-driven scroll clamping.
const scroll=shell.querySelector('.stack-scroll');
const anchors=shell.querySelector('.rings-actions');
shell.querySelector('.stack-top').append(anchors);
let touch=null,travel=0,lastDirection=0;
shell.dataset.controlsHidden='false';
function setControlsHidden(hidden){
 if(shell.dataset.controlsHidden===String(hidden))return;
 const before=scroll.getBoundingClientRect().top,offset=scroll.scrollTop;
 shell.dataset.controlsHidden=String(hidden);
 // Keep the same records at the same screen position when the top rows return.
 scroll.scrollTop=offset<=0?0:Math.max(0,offset+scroll.getBoundingClientRect().top-before);
 for(const node of [shell.querySelector('.stack-top'),shell.querySelector('.rings-horse-actions'),shell.querySelector('.v2-page-nav')])node.inert=hidden;
}
function move(delta){
 if(Math.abs(delta)<1||document.querySelector('dialog[open]'))return;
 const direction=Math.sign(delta);
 travel=direction===lastDirection?travel+Math.abs(delta):Math.abs(delta);lastDirection=direction;
 if(travel<(direction>0?32:16))return;
 if(direction<0)setControlsHidden(false);
 else if(scroll.scrollHeight>scroll.clientHeight+1)setControlsHidden(true);
 travel=0;
}
shell.addEventListener('wheel',event=>{if(Math.abs(event.deltaY)>Math.abs(event.deltaX))move(event.deltaY);},{passive:true});
shell.addEventListener('touchstart',event=>{const p=event.touches[0];touch=p?{x:p.clientX,y:p.clientY}:null;},{passive:true});
shell.addEventListener('touchmove',event=>{
 const p=event.touches[0];if(!p||!touch)return;
 const dx=touch.x-p.clientX,dy=touch.y-p.clientY;
 if(Math.abs(dy)>Math.abs(dx))move(dy);
 touch={x:p.clientX,y:p.clientY};
},{passive:true});
for(const name of ['touchend','touchcancel'])shell.addEventListener(name,()=>{touch=null;},{passive:true});
shell.addEventListener('keydown',event=>{
 if(event.defaultPrevented||event.target.closest('input,textarea,select,[contenteditable="true"]'))return;
 if(['ArrowDown','PageDown','End'].includes(event.key)||event.key===' '&&!event.shiftKey)move(32);
 if(['ArrowUp','PageUp','Home'].includes(event.key)||event.key===' '&&event.shiftKey)move(-16);
});
// Preserve the existing ring-toggle reset and native keyboard return-to-top behavior.
scroll.addEventListener('scroll',()=>{if(scroll.scrollTop<=0&&lastDirection<0)setControlsHidden(false);},{passive:true});
anchors.addEventListener('click',()=>setControlsHidden(false));
