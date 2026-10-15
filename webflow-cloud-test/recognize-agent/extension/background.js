let configPromise;
function config() { return configPromise ||= fetch(chrome.runtime.getURL('local-config.json')).then(r=>r.json()); }
chrome.runtime.onMessage.addListener((message,sender,reply) => {
  let url;
  try {url = new URL(sender.url);} catch {reply({ok:false,error:'page_not_allowed'});return false;}
  if (url.origin !== 'https://ringstatus.com' || !/^\/rs-(recognize|member|barn-onboarding-setup-v24)\/?$/.test(url.pathname) || message?.type !== 'recognize-agent') {
    reply({ok:false,error:'page_not_allowed'}); return false;
  }
  void (async()=>{
    try {
      const local = await config();
      const response = await fetch(local.url+'/agent',{method:'POST',headers:{'Content-Type':'application/json','X-Recognize-Test-Token':local.token},
        body:JSON.stringify({message:message.message,state:message.state,history:message.history}),signal:AbortSignal.timeout(50000)});
      reply(await response.json());
    } catch {reply({ok:false,error:'local_agent_unavailable'});}
  })();
  return true;
});
