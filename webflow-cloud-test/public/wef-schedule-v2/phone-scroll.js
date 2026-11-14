// The phone's outer document scrolls the actual records. Frames contain the
// existing Schedule; they do not intercept vertical scrolling or mirror it.
export function phoneScroll(shell,content,onDirection){
 const phone=matchMedia('(max-width:600px)'),header=shell.querySelector('.stack-top');
 const nav=shell.querySelector('.v2-page-nav'),horses=shell.querySelector('.rings-horse-actions');
 const frames=[],documents=[document];
 const events=new AbortController();
 let owner=window,sameOrigin=true;
 try{while(owner.parent!==owner){const parent=owner.parent;void parent.document.documentElement;frames.push(owner.frameElement);owner=parent;documents.push(owner.document);}}catch{sameOrigin=false;}
 let active=false,paused=false,position=0,resumePosition=null,scheduled=0,headerHeight=0,navHeight=0,programmatic=false,userInput=false;
 let restore=[],styles=[],horseHeight=0;
 const hostCSS=`
 html[data-schedule-phone-host]{height:auto!important;min-height:100%;overflow-x:clip!important;overflow-y:auto!important;overscroll-behavior-y:auto!important;background:var(--schedule-phone-background)!important;scroll-behavior:auto!important;overflow-anchor:none}
 html[data-schedule-phone-host]>body{display:block!important;position:static!important;inset:auto!important;width:100%!important;height:auto!important;min-height:100%;max-height:none!important;margin:0!important;padding:0!important;overflow:visible!important;overscroll-behavior:auto!important;background:var(--schedule-phone-background)!important}
 [data-schedule-phone-path]{display:block!important;position:static!important;inset:auto!important;width:100%!important;height:auto!important;min-height:0!important;max-height:none!important;margin:0!important;padding:0!important;border:0!important;overflow:visible!important;transform:none!important;zoom:1!important;clip-path:none!important}
 iframe[data-schedule-phone-path]{height:var(--schedule-phone-page-height)!important;max-width:100%!important}
 [data-schedule-phone-underlay]{display:none!important}
 html[data-schedule-phone-host][data-schedule-phone-paused]{overflow:hidden!important}
 html[data-schedule-phone-host][data-schedule-phone-paused]>body{position:fixed!important;top:calc(-1 * var(--schedule-phone-position))!important}
 `;
 function attribute(node,name,value){const before=node.getAttribute(name);node.setAttribute(name,value);restore.push(()=>before===null?node.removeAttribute(name):node.setAttribute(name,before));}
 function variable(node,name,value){const before=node.style.getPropertyValue(name),priority=node.style.getPropertyPriority(name);node.style.setProperty(name,value);restore.push(()=>before?node.style.setProperty(name,before,priority):node.style.removeProperty(name));}
 function visible(){return sameOrigin&&phone.matches&&frames.every(frame=>frame?.isConnected&&!frame.hidden);}
 function setPaused(next){
  if(!active||paused===next)return;
  if(next)position=owner.scrollY;
  paused=next;
  const root=owner.document.documentElement;
  root.style.setProperty('--schedule-phone-position',position+'px');
  root.toggleAttribute('data-schedule-phone-paused',paused);
  if(!paused)owner.scrollTo({top:position,behavior:'instant'});
  queue();
 }
 function overlayOpen(){
  const overlay=owner.document.getElementById('rs-profile-v3-frame');
  // A Profile containing this Schedule is an ancestor, not a covering overlay.
  const panel=overlay&&!frames.includes(overlay)?overlay.contentDocument?.getElementById('app'):null;
  return Boolean(document.querySelector('dialog[open]')||(panel&&!panel.hidden));
 }
 function layout(readPosition=true){
  scheduled=0;if(!active)return;
  if(readPosition&&!paused)position=owner.scrollY;
  const viewport=owner.visualViewport;
  const height=viewport?.height||owner.innerHeight,top=position+(viewport?.offsetTop||0);
  headerHeight=header.offsetHeight;
  if(nav.offsetHeight)navHeight=nav.offsetHeight;
  horseHeight=horses.hidden?0:horses.offsetHeight||horseHeight;
  const hidden=shell.dataset.controlsHidden==='true';
  header.inert=hidden&&position>=headerHeight;
  shell.style.setProperty('--phone-header-height',headerHeight+'px');
  shell.style.setProperty('--phone-bottom-height',(navHeight+horseHeight)+'px');
  shell.style.setProperty('--phone-header-top',(hidden?Math.max(0,position-headerHeight):position)+'px');
  shell.style.setProperty('--phone-nav-top',(top+height-navHeight)+'px');
  shell.style.setProperty('--phone-horses-top',(top+height-navHeight-horseHeight)+'px');
  document.documentElement.style.setProperty('--phone-visible-top',(owner===window?(viewport?.offsetTop||0):top)+'px');
  document.documentElement.style.setProperty('--phone-visible-height',height+'px');
  shell.style.minHeight=height+'px';
  // Measure only content, not a previous frame height or an overlay.
  const pageHeight=Math.max(height,headerHeight+content.offsetHeight+navHeight+horseHeight);
  const background=document.documentElement.dataset.theme==='light'?'#fff':'#000';
  for(const doc of documents.slice(1)){
   doc.documentElement.style.setProperty('--schedule-phone-page-height',Math.ceil(pageHeight)+'px');
   doc.documentElement.style.setProperty('--schedule-phone-background',background);
  }
 }
 function queue(){if(active&&!scheduled)scheduled=requestAnimationFrame(()=>layout());}
 function update(){
  if(visible()===active){if(active){setPaused(overlayOpen());queue();}return;}
  if(!visible()){
   const saved=position;resumePosition=phone.matches?saved:null;active=false;paused=false;
   cancelAnimationFrame(scheduled);scheduled=0;
   styles.forEach(style=>style.remove());styles=[];
   restore.reverse().forEach(fn=>fn());restore=[];
   owner.document.documentElement.removeAttribute('data-schedule-phone-paused');
   shell.style.removeProperty('min-height');
   content.scrollTop=saved;
   return;
  }
  position=resumePosition??content.scrollTop;resumePosition=null;active=true;userInput=false;programmatic=true;
  attribute(document.documentElement,'data-schedule-phone-page','');
  for(const doc of documents.slice(1)){
   const root=doc.documentElement;attribute(root,'data-schedule-phone-host','');
   for(const name of ['--schedule-phone-page-height','--schedule-phone-background','--schedule-phone-position'])variable(root,name,'0px');
   const style=doc.createElement('style');style.textContent=hostCSS;doc.head.append(style);styles.push(style);
  }
  for(const frame of frames){
   for(let node=frame;node&&node!==frame.ownerDocument.body;node=node.parentElement){
    attribute(node,'data-schedule-phone-path','');
    for(const sibling of node.parentElement.children){
     if(sibling===node||['SCRIPT','STYLE','LINK'].includes(sibling.tagName)||sibling.id==='rs-profile-v3-frame')continue;
     attribute(sibling,'data-schedule-phone-underlay','');
    }
   }
  }
  layout(false);
  owner.scrollTo({top:position,behavior:'instant'});setPaused(overlayOpen());
 }
 const resize=new ResizeObserver(queue);[header,content,nav,horses].forEach(node=>resize.observe(node));
 const mutations=new MutationObserver(update);
 mutations.observe(shell,{attributes:true,attributeFilter:['data-controls-hidden']});
 mutations.observe(horses,{attributes:true,attributeFilter:['hidden']});
 mutations.observe(document.documentElement,{attributes:true,attributeFilter:['data-theme']});
 mutations.observe(document.body,{subtree:true,attributes:true,attributeFilter:['open']});
 for(const frame of frames)mutations.observe(frame,{attributes:true,attributeFilter:['hidden']});
 owner.addEventListener('scroll',()=>{if(active&&!paused&&visible()){const delta=owner.scrollY-position;position=owner.scrollY;if(userInput&&!programmatic)onDirection(delta);queue();content.dispatchEvent(new Event('scroll'));}},{passive:true,signal:events.signal});
 owner.document.addEventListener('scrollend',()=>{programmatic=false;userInput=false;},{signal:events.signal});
 for(const doc of documents)for(const type of ['touchstart','touchmove','wheel','keydown'])doc.addEventListener(type,()=>{programmatic=false;userInput=true;},{passive:true,signal:events.signal});
 owner.addEventListener('resize',queue,{signal:events.signal});
 owner.visualViewport?.addEventListener('resize',queue,{signal:events.signal});
 owner.visualViewport?.addEventListener('scroll',queue,{signal:events.signal});
 owner.addEventListener('message',event=>{
  if(event.origin!==location.origin||event.source!==owner.document.getElementById('rs-profile-v3-frame')?.contentWindow||event.data?.type!=='rs-profile-bounds')return;
  update();
 },{signal:events.signal});
 phone.addEventListener('change',update,{signal:events.signal});
 // Pointer focus in a tall frame otherwise scrolls the outer page to a
 // viewport-relative control. Keyboard focus and text inputs stay native.
 shell.addEventListener('pointerdown',event=>{
  if(active&&event.target.closest('.stack-top button,.stack-top a,.v2-page-nav button,.v2-page-nav a,.rings-horse-actions button'))event.preventDefault();
 },{signal:events.signal});
 window.addEventListener('pagehide',event=>{
  if(event.persisted)return;
  events.abort();resize.disconnect();mutations.disconnect();cancelAnimationFrame(scheduled);
  if(active){active=false;styles.forEach(style=>style.remove());restore.reverse().forEach(fn=>fn());owner.document.documentElement.removeAttribute('data-schedule-phone-paused');}
 });
 update();
 return {
  get native(){return active;},
  get scrollTop(){return active?(paused?position:owner.scrollY):content.scrollTop;},
  set scrollTop(value){this.scrollTo({top:value,behavior:'instant'});},
  get scrollHeight(){return active?owner.document.scrollingElement.scrollHeight:content.scrollHeight;},
  get clientHeight(){return active?(owner.visualViewport?.height||owner.innerHeight):content.clientHeight;},
  scrollTo(options){if(active){programmatic=Math.abs((options.top??position)-position)>1;position=options.top??position;owner.scrollTo(options);queue();}else content.scrollTo(options);},
  scrollToElement(target,behavior){
   if(active){this.scrollTo({top:Math.max(0,(window===owner?owner.scrollY:0)+target.getBoundingClientRect().top-headerHeight),behavior});return;}
   const scale=content.getBoundingClientRect().height/content.offsetHeight||1;
   const anchors=shell.querySelector('.rings-actions');
   const anchorHeight=content.contains(anchors)?anchors.getBoundingClientRect().height:0;
   const y=content.scrollTop+(target.getBoundingClientRect().top-content.getBoundingClientRect().top-anchorHeight)/scale;
   content.scrollTo({top:Math.max(0,y),behavior});
  },
  addEventListener(...args){content.addEventListener(...args);},
  getBoundingClientRect(){return content.getBoundingClientRect();}
 };
}

export function scheduleViewport(){
 const root=document.documentElement;
 if(root.hasAttribute('data-schedule-phone-page')){
  const top=parseFloat(root.style.getPropertyValue('--phone-visible-top'))||0;
  const height=parseFloat(root.style.getPropertyValue('--phone-visible-height'))||innerHeight;
  return {top,bottom:top+height,left:0,right:document.documentElement.clientWidth};
 }
 return document.querySelector('.rings-shell').getBoundingClientRect();
}
