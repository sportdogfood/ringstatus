// Candidate /profile page loader. Not referenced by any live page.
// The same-origin document preserves private cookies and the locked v3 CSS
// without allowing the Webflow host stylesheet to restyle its components.
if(!document.getElementById('rs-profile-v3-frame')){
 const frame=document.createElement('iframe');
 frame.id='rs-profile-v3-frame';frame.title='RingStatus Profile';
 frame.src='/test/rs-profile-v3/'+location.search;
 frame.style.cssText='display:block;position:fixed;inset:0;width:100%;height:100dvh;border:0;';
 document.body.append(frame);
}
