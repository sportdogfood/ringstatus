// Same-origin website overlay; the Profile document owns identity, data and UI.
if(!document.getElementById('rs-profile-v3-frame')){
 const website=location.pathname.replace(/\/$/,'')!=='/profile';
 const schedule=document.getElementById('wef-schedule-v2');
 const scheduleUrl=schedule?.tagName==='IFRAME'?new URL(schedule.src,location.href):null;
 const hostSchedule=website&&scheduleUrl?.origin===location.origin&&/^\/test\/wef-schedule-v2\/schedule(?:\.html)?\/?$/.test(scheduleUrl.pathname)?schedule:null;
 const scheduleHeight=hostSchedule?.style.getPropertyValue('height'),scheduleHeightPriority=hostSchedule?.style.getPropertyPriority('height');
 let launcherHeight=0;
 const fitSchedule=()=>{if(!hostSchedule)return;const height=matchMedia('(max-width:600px)').matches?launcherHeight:0;if(height>0)hostSchedule.style.setProperty('height',`${Math.max(0,(window.visualViewport?.height||innerHeight)-height)}px`);else if(scheduleHeight)hostSchedule.style.setProperty('height',scheduleHeight,scheduleHeightPriority);else hostSchedule.style.removeProperty('height');};
 const frame=document.createElement('iframe');frame.id='rs-profile-v3-frame';frame.title='RingStatus Profile';
 frame.src='/test/rs-profile-v3/'+(website?'?website=1'+(hostSchedule?'&host-schedule=1':'')+(new URLSearchParams(location.search).has('show-footer')?'&show-footer':''):location.search);
 frame.style.cssText='display:block;position:fixed;inset:0;width:100%;height:100dvh;border:0;z-index:10000;'+(website?'clip-path:inset(100%);background:transparent;':'');
 if(hostSchedule){
  let themeObserver;
  const syncTheme=()=>{const theme=hostSchedule.contentDocument?.documentElement.dataset.theme;if((theme==='light'||theme==='dark')&&frame.contentDocument?.documentElement)frame.contentDocument.documentElement.dataset.theme=theme;};
  const watchTheme=()=>{themeObserver?.disconnect();const root=hostSchedule.contentDocument?.documentElement;if(root){themeObserver=new MutationObserver(syncTheme);themeObserver.observe(root,{attributes:true,attributeFilter:['data-theme']});syncTheme();}};
  hostSchedule.addEventListener('load',watchTheme);frame.addEventListener('load',syncTheme);watchTheme();
 }
 if(website){
  addEventListener('message',event=>{
   if(event.origin!==location.origin||event.source!==frame.contentWindow||event.data?.type!=='rs-profile-bounds')return;
   const {top,right,bottom,left}=event.data;
   if([top,right,bottom,left].every(n=>Number.isFinite(n)&&n>=0))frame.style.clipPath=`inset(${top}px ${right}px ${bottom}px ${left}px)`;
   if(hostSchedule&&Number.isFinite(event.data.launcherHeight)&&event.data.launcherHeight>=0&&event.data.launcherHeight<=innerHeight){launcherHeight=event.data.launcherHeight;fitSchedule();}
  });
  frame.addEventListener('load',()=>{
   const doc=frame.contentDocument;
   if(doc?.location.pathname.startsWith('/test/wef-schedule-v2/')){
    frame.style.clipPath='inset(0)';
    const back=doc.querySelector('.rings-profile-nav a,.back-row a.back-link');
    if(back){back.href='/test/rs-profile-v3/?website=1';back.target='_self';}
   }
  });
 }
 const viewport=window.visualViewport;
 const fitViewport=()=>{const phone=matchMedia('(max-width:600px)').matches;frame.style.height=phone&&viewport?`${viewport.height}px`:'100dvh';frame.style.top=phone&&viewport?`${viewport.offsetTop}px`:'0';fitSchedule();};
 viewport?.addEventListener('resize',fitViewport);viewport?.addEventListener('scroll',fitViewport);addEventListener('resize',fitViewport);fitViewport();
 document.body.append(frame);
}
