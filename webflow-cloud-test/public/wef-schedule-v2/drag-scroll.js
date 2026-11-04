// Mouse dragging for horizontal action lanes. Touch keeps native scrolling.
export function enableDragScroll(lane) {
  let pointer=null;
  let suppressClick=false;
  const update=()=>lane.classList.toggle('can-grab',lane.scrollWidth>lane.clientWidth+1);
  const finish=()=>{
    const id=pointer?.id;
    pointer=null;
    lane.classList.remove('is-grabbing');
    if(id!=null&&lane.hasPointerCapture(id))lane.releasePointerCapture(id);
  };
  lane.classList.add('drag-scroll');
  const resize=new ResizeObserver(update);resize.observe(lane);
  const mutation=new MutationObserver(update);mutation.observe(lane,{childList:true,subtree:true});
  update();
  lane.addEventListener('pointerdown',event=>{
    suppressClick=false;
    if(event.pointerType!=='mouse'||event.button!==0||!event.isPrimary)return;
    update();
    if(!lane.classList.contains('can-grab'))return;
    pointer={id:event.pointerId,x:event.clientX,y:event.clientY,left:lane.scrollLeft,dragging:false};
  });
  lane.addEventListener('pointermove',event=>{
    if(!pointer||event.pointerId!==pointer.id)return;
    const dx=event.clientX-pointer.x;
    if(!pointer.dragging){
      if(Math.abs(dx)<6||Math.abs(dx)<Math.abs(event.clientY-pointer.y))return;
      pointer.dragging=true;
      suppressClick=true;
      lane.setPointerCapture(pointer.id);
      lane.classList.add('is-grabbing');
    }
    event.preventDefault();
    lane.scrollLeft=pointer.left-dx;
  });
  lane.addEventListener('pointerup',finish);
  lane.addEventListener('pointercancel',finish);
  lane.addEventListener('lostpointercapture',finish);
  lane.addEventListener('pointerleave',()=>{if(pointer&&!pointer.dragging)finish();});
  window.addEventListener('blur',finish);
  lane.addEventListener('dragstart',event=>{if(pointer)event.preventDefault();});
  lane.addEventListener('click',event=>{
    if(!suppressClick||event.detail===0)return;
    suppressClick=false;
    event.preventDefault();
    event.stopImmediatePropagation();
  },true);
  return ()=>{finish();resize.disconnect();mutation.disconnect();window.removeEventListener('blur',finish);};
}
