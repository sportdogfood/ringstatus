(() => {
 if(document.getElementById('rs-disposable-agent-test'))return;
 const adapter=RecognizeTestAdapter(document),host=document.createElement('aside');host.id='rs-disposable-agent-test';
 const shadow=host.attachShadow({mode:'closed'});
 shadow.innerHTML=`<style>
    :host{position:fixed;right:18px;bottom:18px;width:min(370px,calc(100vw - 36px));z-index:2147483647;font:14px/1.45 system-ui;color:#18312a}
    *{box-sizing:border-box}section{background:#fff;border:1px solid #a3b9b0;border-radius:14px;box-shadow:0 8px 32px #0003;overflow:hidden}
    header{background:#173e32;color:#fff;padding:12px 15px;display:flex;justify-content:space-between;align-items:center}header button{background:transparent;color:white;border:0;cursor:pointer}
    main{padding:14px}p{margin:0 0 10px}.note{font-size:12px;color:#536b60}#chat{max-height:220px;overflow:auto;white-space:pre-wrap}#chat p{padding:9px;background:#eff5f1;border-radius:8px}#chat p.user{background:#f5f5f5}
    input,button{font:inherit}input{width:100%;padding:9px;border:1px solid #a3b9b0;border-radius:7px}button.primary{background:#173e32;color:white;border:0;border-radius:7px;padding:9px 12px;cursor:pointer;margin-top:8px}button:disabled{opacity:.5;cursor:default}
    #private{border-top:1px solid #d4dfd8;padding-top:12px;margin-top:12px}#status{font-size:12px;margin-top:10px;color:#536b60}[hidden]{display:none!important}
  </style><button id="open" class="primary">Recognition help</button><section id="panel" hidden><header><strong>Recognition help</strong><button id="hide">×</button></header><main>
 <div id="chat" role="log" aria-live="polite"></div>
 <form id="ask"><label for="message">Ask for help — keep numbers in the private fields</label><input id="message" maxlength="800" autocomplete="off"><button class="primary" id="send">Ask</button></form>
 <button class="primary" id="action" hidden></button>
 <p><button type="button" data-choice="phone">Phone</button> <button type="button" data-choice="pin">PIN</button> <button type="button" data-choice="invite">Invite</button> <button type="button" data-choice="sms">SMS option</button></p>
 <form id="private"><label id="label" for="credential">10-digit phone</label><input id="credential" inputmode="numeric" autocomplete="tel" required><label for="phone">Phone if needed to identify a shared PIN</label><input id="phone" inputmode="tel" autocomplete="tel"><button class="primary" id="verify">Continue</button></form>
 <p id="status" role="status"></p></main></section>`;
 document.documentElement.append(host);
 const get=id=>shadow.getElementById(id);let pending=null,busy=false,history=[];
 const redact=value=>String(value).replace(/https?:\/\/\S+/gi,'[link removed]').replace(/(?:\d[\s().-]*){4,}/g,'[private numeric input removed]').replace(/[A-Za-z0-9_-]{32,}/g,'[private token removed]');
 function say(text){const p=document.createElement('p');p.textContent=text;get('chat').append(p);}
 function update(){
  const state=adapter.snapshot();get('status').textContent=state.stage==='tenant_ready'?'Recognized; Tenants access confirmed.':state.stage==='recognized'?'Recognized.':state.stage==='unavailable'?'Recognition is unavailable; use the native controls.':state.stage==='code'?'Enter the SMS code in this browser.':state.busy?'Checking…':'Enter your details privately.';
  get('verify').disabled=busy||state.busy;get('send').disabled=busy||state.busy||state.stage==='tenant_ready';
  get('private').hidden=['recognized','tenant_ready','unavailable'].includes(state.stage);
  const mode=state.stage==='code'?'code':state.entryMode||'phone';get('label').textContent={phone:'10-digit phone',pin:'4-digit PIN',invite:'7-digit invitation',sms:'10-digit phone for optional SMS',code:'SMS code'}[mode];
  get('phone').hidden=mode!=='pin';get('phone').previousElementSibling.hidden=mode!=='pin';
  get('credential').setAttribute('autocomplete',mode==='code'?'one-time-code':mode==='phone'||mode==='sms'?'tel':'off');
  get('verify').textContent=mode==='sms'?'Send SMS code':'Continue';
 }
 async function run(action,value){if(busy)return;busy=true;get('action').hidden=true;pending=null;try{await adapter.perform(action,value);}catch{say('Use the native result; no automatic retry was made.');}finally{busy=false;update();}}
 for(const button of shadow.querySelectorAll('[data-choice]'))button.onclick=()=>{get('credential').value='';void run('show_'+button.dataset.choice);};
 get('private').onsubmit=event=>{event.preventDefault();const state=adapter.snapshot(),value=get('credential').value,phone=get('phone').value;get('credential').value='';get('phone').value='';void run(state.stage==='code'?'verify_code':state.entryMode==='sms'?'request_code':'submit_credential',state.entryMode==='pin'?{value,phone}:value);};
 get('ask').onsubmit=async event=>{
  event.preventDefault();if(busy)return;const message=redact(get('message').value.trim());if(!message)return;get('message').value='';busy=true;update();
  try{const response=await chrome.runtime.sendMessage({type:'recognize-agent',message,state:adapter.snapshot(),history:history.slice(-6)});
   if(!response?.ok){say('Agent unavailable. Native recognition remains available.');return;}
   history.push({role:'user',content:message},{role:'assistant',content:response.decision.message});say(response.decision.message);
   if(response.decision.action!=='none'){pending=response.decision.action;get('action').textContent={show_phone:'Enter phone',show_pin:'Enter PIN',show_invite:'Enter invitation',show_sms:'Show SMS option',not_you:'Not you? Switch identity',retry:'Retry existing request'}[pending];get('action').hidden=false;}
  }catch{say('Agent unavailable. Native recognition remains available.');}finally{busy=false;update();}
 };
 get('action').onclick=()=>{if(pending)void run(pending);};
 get('open').onclick=()=>{get('panel').hidden=false;get('open').hidden=true;update();};get('hide').onclick=()=>{get('panel').hidden=true;get('open').hidden=false;};
 new MutationObserver(update).observe(document.documentElement,{subtree:true,attributes:true,attributeFilter:['data-rs-recognition-state']});update();
})();
