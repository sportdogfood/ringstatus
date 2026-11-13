// V2 drawer/flyup cadence; desktop modality belongs to the Schedule shell.
const desktop=matchMedia('(min-width:601px)');
const localDialogs=new Map();
const swipeDialogs=new WeakSet();

function enableHandleDismiss(dialog){
 if(swipeDialogs.has(dialog))return;swipeDialogs.add(dialog);let start=null;
 dialog.addEventListener('pointerdown',event=>{
  if(desktop.matches||!event.isPrimary||event.button!==0||!event.target.closest('.rings-handle'))return;
  start={id:event.pointerId,x:event.clientX,y:event.clientY};dialog.setPointerCapture(event.pointerId);
 });
 dialog.addEventListener('pointermove',event=>{if(start&&event.pointerId===start.id&&event.clientY-start.y>8)event.preventDefault();});
 dialog.addEventListener('pointerup',event=>{
  if(!start||event.pointerId!==start.id)return;const dy=event.clientY-start.y,dx=Math.abs(event.clientX-start.x);start=null;
  if(dialog.hasPointerCapture(event.pointerId))dialog.releasePointerCapture(event.pointerId);
  if(!desktop.matches&&dy>=56&&dy>dx*1.5)dialog.close();
 });
 for(const name of ['pointercancel','lostpointercapture','close'])dialog.addEventListener(name,()=>{start=null;});
}
desktop.addEventListener('change',()=>{document.querySelectorAll('#rings-surface[open],#v2-trainer-dialog[open]').forEach(dialog=>dialog.close());});
export function openShellDialog(dialog){
 if(dialog.open)return;
 enableHandleDismiss(dialog);
 if(!desktop.matches){dialog.showModal();return;}
 for(const opened of localDialogs.keys())opened.close();
 const shell=document.querySelector('.rings-shell');
 shell.append(dialog);dialog.dataset.shellLocal='true';
 const shade=document.createElement('div');shade.className='rings-dialog-shade';shade.setAttribute('aria-hidden','true');
 shade.addEventListener('click',()=>dialog.close());shell.insertBefore(shade,dialog);
 const siblings=[...shell.children].filter(node=>node!==dialog&&node!==shade).map(node=>[node,node.inert]);
 siblings.forEach(([node])=>node.inert=true);
 const escape=event=>{if(event.key==='Escape'){event.preventDefault();dialog.close();}};
 dialog.addEventListener('keydown',escape);
 localDialogs.set(dialog,shade);
 dialog.addEventListener('close',()=>{shade.remove();siblings.forEach(([node,inert])=>node.inert=inert);dialog.removeEventListener('keydown',escape);localDialogs.delete(dialog);},{once:true,capture:true});
 dialog.show();
}
