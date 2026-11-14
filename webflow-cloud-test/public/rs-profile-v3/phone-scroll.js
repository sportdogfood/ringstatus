// Profile adaptation of Schedule phone-scroll.js: the outer page scrolls real content.
export function profilePhoneScroll(shell,identity){
 const phone=matchMedia('(max-width:600px)'),frames=[],documents=[document];
 let owner=window,sameOrigin=true;
 try{while(owner.parent!==owner){void owner.parent.document.documentElement;frames.push(owner.frameElement);owner=owner.parent;documents.push(owner.document);}}catch{sameOrigin=false;}
 let active=false,paused=false,position=0,scheduled=0,restore=[],styles=[],identityBottom=6;
 const hostCSS=`
 html[data-profile-phone-host]{height:auto!important;min-height:100%;overflow-x:clip!important;overflow-y:auto!important;overscroll-behavior-y:auto!important;background:var(--profile-phone-background)!important;scroll-behavior:auto!important;overflow-anchor:none}
 html[data-profile-phone-host]>body{display:block!important;position:static!important;inset:auto!important;width:100%!important;height:auto!important;min-height:100%;max-height:none!important;margin:0!important;padding:0!important;overflow:visible!important;overscroll-behavior:auto!important;background:var(--profile-phone-background)!important}
 [data-profile-phone-path]{display:block!important;position:static!important;inset:auto!important;width:100%!important;height:auto!important;min-height:0!important;max-height:none!important;margin:0!important;padding:0!important;border:0!important;overflow:visible!important;transform:none!important;zoom:1!important;clip-path:none!important}
 iframe[data-profile-phone-path]{height:var(--profile-phone-page-height)!important;max-width:100%!important}
 [data-profile-phone-underlay]{display:none!important}
 html[data-profile-phone-host][data-profile-phone-paused]{overflow:hidden!important}
 html[data-profile-phone-host][data-profile-phone-paused]>body{position:fixed!important;top:calc(-1 * var(--profile-phone-position))!important}
 `;
 const events=new AbortController();
 function attribute(node,name,value){const before=node.getAttribute(name);node.setAttribute(name,value);restore.push(()=>before===null?node.removeAttribute(name):node.setAttribute(name,before));}
 function variable(node,name,value){const before=node.style.getPropertyValue(name),priority=node.style.getPropertyPriority(name);node.style.setProperty(name,value);restore.push(()=>before?node.style.setProperty(name,before,priority):node.style.removeProperty(name));}
 function layout(){
  scheduled=0;if(!active)return;
  if(!paused)position=owner.scrollY;
  const viewport=owner.visualViewport,height=viewport?.height||owner.innerHeight,top=position+(viewport?.offsetTop||0);
  const header=shell.querySelector('.stack-top'),nav=shell.querySelector('.launcher-widget');
  if(!header||!nav)return;
  const set=(k,v)=>{if(shell.style.getPropertyValue(k)!==v)shell.style.setProperty(k,v);};
  set('--profile-header-height',header.offsetHeight+'px');set('--profile-nav-height',nav.offsetHeight+'px');
  set('--profile-header-top',top+'px');set('--profile-nav-top',(top+height-nav.offsetHeight)+'px');
  identity.getRootNode().host.style.setProperty('--profile-identity-top',(top+height-identity.offsetHeight-identityBottom)+'px');
  const min=height+'px';if(shell.style.minHeight!==min)shell.style.minHeight=min;
  const pageHeight=Math.ceil(Math.max(height,shell.getBoundingClientRect().height))+'px',background=document.documentElement.dataset.theme==='light'?'#fff':'#000';
  for(const doc of documents.slice(1)){doc.documentElement.style.setProperty('--profile-phone-page-height',pageHeight);doc.documentElement.style.setProperty('--profile-phone-background',background);}
 }
 function queue(){if(active&&!scheduled)scheduled=requestAnimationFrame(layout);}
 function update(){
  const visible=sameOrigin&&phone.matches&&!shell.hidden&&frames.every(f=>f.isConnected&&!f.hidden);
  if(visible!==active){
   if(!visible){active=false;paused=false;cancelAnimationFrame(scheduled);scheduled=0;styles.forEach(s=>s.remove());styles=[];restore.reverse().forEach(fn=>fn());restore=[];shell.style.removeProperty('min-height');for(const doc of documents)doc.documentElement.removeAttribute('data-profile-phone-paused');return;}
   active=true;identityBottom=parseFloat(getComputedStyle(identity).bottom)||6;attribute(identity.getRootNode().host,'data-phone-page','');variable(identity.getRootNode().host,'--profile-identity-top','0px');attribute(document.documentElement,'data-profile-phone-page','');
   for(const doc of documents.slice(1)){
    attribute(doc.documentElement,'data-profile-phone-host','');
    for(const name of ['--profile-phone-page-height','--profile-phone-background','--profile-phone-position'])variable(doc.documentElement,name,'0px');
    const style=doc.createElement('style');style.textContent=hostCSS;doc.head.append(style);styles.push(style);
   }
   for(const frame of frames)for(let node=frame;node&&node!==frame.ownerDocument.body;node=node.parentElement){attribute(node,'data-profile-phone-path','');for(const sibling of node.parentElement.children)if(sibling!==node&&!['SCRIPT','STYLE','LINK'].includes(sibling.tagName))attribute(sibling,'data-profile-phone-underlay','');}
   owner.scrollTo({top:position,behavior:'instant'});
  }
  if(active){
   const next=Boolean(document.querySelector('dialog[open]'));
   if(next!==paused){if(next)position=owner.scrollY;paused=next;for(const doc of documents){doc.documentElement.style.setProperty('--profile-phone-position',position+'px');doc.documentElement.toggleAttribute('data-profile-phone-paused',paused);}if(!paused)owner.scrollTo({top:position,behavior:'instant'});}
   queue();
  }
 }
 const observer=new MutationObserver(update);observer.observe(shell,{subtree:true,childList:true,attributes:true,attributeFilter:['hidden']});observer.observe(document.body,{subtree:true,attributes:true,attributeFilter:['open']});observer.observe(document.documentElement,{attributes:true,attributeFilter:['data-theme']});
 for(const frame of frames)observer.observe(frame,{attributes:true,attributeFilter:['hidden']});
 const resize=new ResizeObserver(queue);resize.observe(shell);
 owner.addEventListener('scroll',queue,{passive:true,signal:events.signal});owner.addEventListener('resize',queue,{signal:events.signal});owner.visualViewport?.addEventListener('resize',queue,{signal:events.signal});owner.visualViewport?.addEventListener('scroll',queue,{signal:events.signal});phone.addEventListener('change',update,{signal:events.signal});
 shell.addEventListener('pointerdown',e=>{if(active&&e.target.closest('.stack-top button,.launcher-widget button,.launcher-widget a'))e.preventDefault();},{signal:events.signal});
 identity.addEventListener('pointerdown',e=>{if(active)e.preventDefault();},{signal:events.signal});
 window.addEventListener('pagehide',e=>{if(!e.persisted){cancelAnimationFrame(scheduled);for(const doc of documents)doc.documentElement.removeAttribute('data-profile-phone-paused');events.abort();observer.disconnect();resize.disconnect();styles.forEach(s=>s.remove());restore.reverse().forEach(fn=>fn());}},{once:true});
 update();
 return {get native(){return active;},get scrollTop(){return active?position:document.scrollingElement.scrollTop;},set scrollTop(value){if(active){position=value;owner.scrollTo({top:value,behavior:'instant'});queue();}else document.scrollingElement.scrollTop=value;}};
}
