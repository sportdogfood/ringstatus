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
shell.querySelector('.back-row').append(theme);
const launcherSlot=document.createElement('div');launcherSlot.className='schedule-launcher-slot';launcherSlot.dataset.profileLauncherSlot='';launcherSlot.hidden=true;shell.append(launcherSlot);
shell.querySelector('.stack-scroll').classList.add('rings-scroll');
shell.querySelector('.brand-row').classList.add('rings-header');
shell.querySelectorAll('.rings-pill').forEach(node=>v3Control(node));
shell.querySelectorAll('.v2-theme-toggle').forEach(node=>v3Control(node,{icon:true}));

// Keep the same shell slots; only the phone data viewport grows while scrolled.
const phone=matchMedia('(max-width:600px)');
const scroll=shell.querySelector('.stack-scroll');
const anchors=shell.querySelector('.rings-actions');
const anchorParent=anchors.parentElement,anchorNext=anchors.nextSibling;
let direction=0,touch=null,clampedAtTop=false;
function setPhoneScroll(scrolled){
 const next=String(phone.matches&&scrolled);if(shell.dataset.phoneScrolled===next)return;
 shell.dataset.phoneScrolled=next;
 // Growing a short list can clamp its offset to zero; that is not a return gesture.
 clampedAtTop=next==='true'&&scroll.scrollTop<=0;
}
function placeAnchors(){
 if(phone.matches)shell.querySelector('.stack-top').append(anchors);
 else anchorParent.insertBefore(anchors,anchorNext);
 direction=0;touch=null;setPhoneScroll(phone.matches&&scroll.scrollTop>8);
}
function move(delta){
 if(!phone.matches||Math.abs(delta)<1)return;
 direction=Math.sign(delta);
 if(direction>0&&scroll.scrollHeight>scroll.clientHeight)setPhoneScroll(true);
 else if(direction<0&&scroll.scrollTop<=0)setPhoneScroll(false);
}
scroll.addEventListener('scroll',()=>{
 if(!phone.matches)return;
 if(scroll.scrollTop>8){clampedAtTop=false;setPhoneScroll(true);}
 else if(scroll.scrollTop<=0&&(!clampedAtTop||direction<0))setPhoneScroll(false);
},{passive:true});
scroll.addEventListener('wheel',event=>move(event.deltaY),{passive:true});
scroll.addEventListener('touchstart',event=>{touch=event.touches[0]?{x:event.touches[0].clientX,y:event.touches[0].clientY}:null;},{passive:true});
scroll.addEventListener('touchmove',event=>{
 const point=event.touches[0];if(!point||!touch)return;
 const dx=touch.x-point.clientX,dy=touch.y-point.clientY;
 if(Math.abs(dy)>Math.abs(dx))move(dy);
 touch={x:point.clientX,y:point.clientY};
},{passive:true});
for(const name of ['touchend','touchcancel'])scroll.addEventListener(name,()=>{touch=null;},{passive:true});
scroll.addEventListener('keydown',event=>{
 if(event.target.closest('input,textarea,select,[contenteditable="true"]'))return;
 if(['ArrowDown','PageDown','End'].includes(event.key)||event.key===' '&&!event.shiftKey)move(12);
 if(['ArrowUp','PageUp','Home'].includes(event.key)||event.key===' '&&event.shiftKey)move(-12);
});
phone.addEventListener('change',placeAnchors);placeAnchors();
