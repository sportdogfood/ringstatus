// Same-origin website overlay; the Profile document owns identity, data and UI.
if(!document.getElementById('rs-profile-v3-frame')){
 const website=location.pathname.replace(/\/$/,'')!=='/profile';
 const frame=document.createElement('iframe');frame.id='rs-profile-v3-frame';frame.title='RingStatus Profile';
 frame.src='/test/rs-profile-v3/'+(website?'?website=1'+(new URLSearchParams(location.search).has('show-footer')?'&show-footer':''):location.search);
 frame.style.cssText='display:block;position:fixed;inset:0;width:100%;height:100dvh;border:0;z-index:10000;'+(website?'clip-path:inset(100%);background:transparent;':'');
 if(website){
  addEventListener('message',event=>{
   if(event.origin!==location.origin||event.source!==frame.contentWindow||event.data?.type!=='rs-profile-bounds')return;
   const {top,right,bottom,left}=event.data;
   if([top,right,bottom,left].every(n=>Number.isFinite(n)&&n>=0))frame.style.clipPath=`inset(${top}px ${right}px ${bottom}px ${left}px)`;
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
 const fitViewport=()=>{const phone=matchMedia('(max-width:600px)').matches;frame.style.height=phone&&viewport?`${viewport.height}px`:'100dvh';frame.style.top=phone&&viewport?`${viewport.offsetTop}px`:'0';};
 viewport?.addEventListener('resize',fitViewport);viewport?.addEventListener('scroll',fitViewport);addEventListener('resize',fitViewport);fitViewport();
 document.body.append(frame);
}
