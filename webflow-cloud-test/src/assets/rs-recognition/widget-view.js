import widgetCss from './widget.css?raw';

// Existing widget presentation; identity belongs to its server adapter.
const fingerprint='<svg viewBox="0 0 32 36" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 8C10 1 20 0 27 6M3 25V20C3 15 4 12 7 10M10 8C17 4 27 8 29 15C30 18 30 21 30 24M5 30C9 28 8 23 8 20C8 9 25 9 25 20C25 26 24 31 23 34M10 33C14 29 13 23 13 20C13 16 20 16 20 20C20 27 19 31 17 35"/></svg>';
const user='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="9" cy="7" r="3.5"/><path d="M3 20v-1.5a6 6 0 0 1 12 0V20M15 12h7m-3-3 3 3-3 3"/></svg>';

export function mountRecognize(root,{adapter,onProfile=()=>{},onRecognized=()=>{},onSignedOut=()=>{}}){
 if(!root||!adapter||!['current','recognize','signOut'].every(k=>typeof adapter[k]==='function'))throw new TypeError('root and identity adapter required');
 const shadow=root.attachShadow({mode:'open'});shadow.innerHTML=`<style>${widgetCss}</style><section id="signin" class="panel" role="dialog" aria-labelledby="title" hidden><div class="panel-top"><span class="profile-brand" aria-label="RingStatus Pro"><span class="rs-wordmark" aria-hidden="true"><span class="rs-wordmark-ring">RING</span><span class="rs-wordmark-status">STATUS</span></span><span class="profile-pro">PRO</span></span><button id="close" class="icon-button" aria-label="Close sign in" type="button">×</button></div><h2 id="title" class="signin-title">Sign in</h2><form class="signin-form"><div class="field"><input id="credential" aria-label="Phone, PIN, or Recognition Invite" placeholder="Phone, PIN, or Recognition Invite" autocomplete="off" inputmode="numeric" required></div><p id="error" role="alert"></p><button class="primary" type="submit">Sign in</button></form></section><button id="launcher" type="button" aria-controls="signin" aria-expanded="false"></button>`;
 const $=s=>shadow.querySelector(s),panel=$('#signin'),launcher=$('#launcher'),input=$('#credential'),error=$('#error');let userId=null,busy=false,generation=0;
 function show(open){panel.hidden=!open;launcher.setAttribute('aria-expanded',String(open));launcher.setAttribute('aria-label',open?'Close sign in':userId?'Open profile':'Open sign in');if(open)input.focus()}
 function paint(){launcher.innerHTML=userId?user:fingerprint;launcher.classList.toggle('recognized',!!userId);launcher.setAttribute('aria-label',userId?'Open profile':'Open sign in')}
 function identity(id){return typeof id==='string'&&id.trim()?id:null}
 async function refresh({showFormWhenUnrecognized=false}={}){const check=++generation;try{const found=identity(await adapter.current());if(check!==generation)return userId;userId=found;paint();show(!userId && showFormWhenUnrecognized);return userId}catch(error){if(check!==generation)return userId;userId=null;paint();if(showFormWhenUnrecognized)show(true);$('#error').textContent='Recognition is unavailable. You can try signing in.';throw error}}
 launcher.addEventListener('click',()=>{if(userId){onProfile(userId);return}show(panel.hidden)});$('#close').addEventListener('click',()=>show(false));
 input.addEventListener('input',()=>{error.textContent='';input.removeAttribute('aria-invalid')});
 shadow.querySelector('form').addEventListener('submit',async e=>{e.preventDefault();if(busy)return;generation++;busy=true;const button=shadow.querySelector('.primary');button.disabled=true;try{const found=identity(await adapter.recognize(input.value));input.value='';if(!found){error.textContent='That phone, PIN, or recognition invite was not found.';input.setAttribute('aria-invalid','true');return}userId=found;error.textContent='';paint();show(false);onRecognized(userId)}catch{error.textContent='Sign in is unavailable. Try again.'}finally{busy=false;button.disabled=false}});
 async function signOut(){generation++;const work=adapter.signOut();userId=null;input.value='';error.textContent='';paint();show(true);onSignedOut();await work}
 paint();
 return {refresh,signOut,getUserId:()=>userId,destroy:()=>{shadow.replaceChildren()}};
}
