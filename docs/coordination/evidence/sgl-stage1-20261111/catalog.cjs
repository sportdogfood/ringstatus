// Static catalog builder. Reads source/evidence only; no SGL or Airtable calls.
const fs=require('fs'),path=require('path');const dir=__dirname,p=require('./requests.json'),existing=require('./existing-records.json');
const API='https://sglapi.wellingtoninternational.com',SGL='https://sgl.wellingtoninternational.com';
const main=p.source.url,doc='C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/sgl-application-endpoint-opportunities-20261111.md';
const fieldMap={'source-route':'fldxQQ6UTlp70pbKA','observed-result':'fldZ27lP3CDD15cD6',scope:'fldi4pYANPGvJ0Bap',method:'flddurg9g6cnM7lth','test-status':'fld4wtDdlPouYd3eQ','public-access':'fldwDJzXJQdgp9ihH','access-evidence':'fldEsnqX34ysAm342','next-check':'fldOM6kFukETUPgn3','example-url':'fldk9g2uVrZxX9uUB','source-js':'fld5mGBMgnh6RQL1e',evidence:'fldkKHtxMscPne4vk','reviewed-on':'fldrHwSoWUSqx903F'};
const unique=a=>[...new Set(a)];
function canon(url){
 url=url.replace(/\{[^}]*\}/g,'{id}').split('?')[0];
 return url.replace(/\/iphone.php\/\{id\}\/webservice\//,'/iphone.php/esp/webservice/').replace(/\/+$/,'');
}
const catalog=new Map(),excluded=[];
for(const r of p.found){
 const id=canon(r.url);
 if(/video|livestream|channel|subscriptions/i.test(id)){excluded.push(r);continue;} // preserve owner's earlier video exclusion
 if(!/^https:\/\/(?:sglapi\.wellingtoninternational\.com|sgl\.wellingtoninternational\.com|sglapp\.showgroundslive\.com)\//.test(id))continue;
 if(!catalog.has(id))catalog.set(id,{route:id,requests:[]});catalog.get(id).requests.push(r);
}
for(const saved of existing){const c=saved.cellValuesByFieldId,id=canon(c[fieldMap['source-route']]);if(!catalog.has(id))catalog.set(id,{route:id,requests:[]});const r=catalog.get(id);r.recordId=saved.id;r.prior={};for(const [name,fid]of Object.entries(fieldMap)){let v=c[fid];if(v?.name)v=v.name;if(v!==undefined)r.prior[name]=v;}}
// Special source-backed calls missing from main.js's HTTP-call AST: iframe URL and legacy inline requests retained from prior browser evidence.
const all=[];
function family(path){
 if(/\/(schedule|shows)(?:\/|$)|GetShowData/.test(path))return 'Show discovery, whole-show inventory and schedules';
 if(/liveclass|LiveScoreWidget|getLiveClassStatus/.test(path))return 'Live class/group/ring activity and observations';
 if(/^\/classes(?:\/|$)/.test(path))return 'Class inventory or detailed class/trip data';
 if(/^\/ring(?:\/|$)/.test(path))return 'Ring discovery, order-of-go and estimated schedule';
 if(/results/.test(path))return 'Show/division results and standings';
 if(/classsignup/.test(path))return 'Class-group order/signups and reservation workflow';
 if(/people|Person|Membership|horses|Horse/i.test(path))return 'Horse/person identity, memberships, relationships or association workflow; exact fields are in caller parameters';
 if(/^\/entries(?:\/|$)/.test(path))return 'Show entries, participant classes or entry workflow';
 if(/utilities|get-config|auth|LoggedIn|Login|Session|UserViaJWT|\/users\//i.test(path))return 'Authentication, session, token/configuration or user-state support';
 if(/version\.json/.test(path))return 'Application build version; existing show-time uses its HTTP Date header';
 if(/billing|checkout|Payment|Card|Bank|Fee|Account|viewPDF/.test(path))return 'Account/billing/payment workflow; not a public collection source';
 if(/registration|verification|Verify|digital-signature|ElectronicDocument|user-requests|milestone|trainer-teams/i.test(path))return 'User/team/account/document/onboarding workflow; not general show collection';
 return 'Application service: '+path.split('/').filter(Boolean).join(' / ');
}
for(const item of catalog.values()){
 const route=item.route,pathOnly=route.replace(/^https?:\/\/[^/]+/,'');
 const requests=item.requests,methods=unique([...requests.map(r=>r.method),...String(item.prior?.method||'').split(/\s*\/\s*/).filter(Boolean)]);
 const readPost=/LiveScoreWidget|\/getDigitalSignatureVerification$|\/getElectronicDocuments$|\/IsLoggedIn$/.test(pathOnly);
 const action=methods.some(x=>['POST','PATCH','PUT','DELETE'].includes(x))&&!readPost||/Save|Delete|Remove|Reserve|Cancel|Submit|SelectClass|SelectDivision|SelectShowFee|Authenticate|CheckClassSignUpsExpire|Verify|confirm|reset|logout|login|signAgreement|ClearSession|clear-session|skip-verify|SkipUpdatePassword|ChangeUserEmail|updateRequiredActionStatus/i.test(pathOnly);
 const account=/billing|checkout|Account|Payment|Card|Bank|auth\/|registration|verification|Verify|user-requests|digital-signature|ElectronicDocument|milestone|trainer-teams|associated-|GetAssocaited|\/users\/|LoggedIn|CheckUserAccessForAdminFunction|\/entries\/my|\/schedule\/my|\/results\/my|SecureDetails/i.test(pathOnly);
 const critical=/^\/(schedule|shows)$|^\/classes\/\{id\}$|GetShowData|getLiveClassData|ClassStatus|\/Detail$|LiveScoreWidget|getLiveClassStatus|utilities\/get-config|log-protection-error|version\.json/.test(pathOnly);
 const prior=item.prior||{};
 const params=unique(requests.flatMap(r=>r.query.map(q=>q.name))),body=unique(requests.flatMap(r=>r.body.map(q=>q.name)));
 const pathParams=(pathOnly.match(/\{[^}]+\}/g)||[]).map((_,i)=>'path value '+(i+1));
 let role='Supporting candidate',basis='No current collector/proxy implementation verified for this route; evaluate its useful data before selecting a primary or fallback.',built='Not found in the inspected active source readers/proxy allowlists.';
 if(action||account){role='Not for general collection';basis='Account/action-specific scope. An HTTP GET can still change state; no source action was invoked.';}
 if(/GetShowData/.test(pathOnly)){role='Fallback candidate for basic show inventory';basis='Browser returned 429 classes/82 divisions. Full ring schedule and trip coverage is not established.';}
 if(pathOnly==='/schedule'){role='Current primary';basis='Fast requests dated schedule through shared proxy. Shows browser discovery also captures schedule. Existing runner-first and independent header/token fallback are built.';built='Shared Worker /schedule; runner+decoder+fallback; Fast source-reader.js; Shows browser-discovery.js. Source verified, no new scheduled/live proof in Stage 1.';}
 if(pathOnly==='/classes/{id}'){role='Current primary; failure reported';basis='Fast uses shared /classes/{id}. Legacy live data is a candidate, not a proven full replacement. Existing request fallback is built; data-response _t recovery differs from main.js.';built='Shared Worker + runner decoder + five-profile fallback; Fast source-reader.js. Live decoded route exists. Current failure remains open; no collector run.';}
 if(pathOnly==='/shows'){role='Current primary for browser show discovery';basis='Shows browser-discovery.js captures default_show from this response and compares schedule/URL identity.';built='Shows browser discovery. /shows is not in shared Worker route allowlist.';}
 if(/getLiveClassData$/.test(pathOnly)){role='Current primary for Live; fallback candidate for Fast';basis='Live reads this legacy route directly. Browser sample has trips even with is_live=false. Coverage versus protected class detail is not established.';built='wef-live/source.js detail + lanes.js; also supported as a public proxy route in public-live.mjs.';}
 if(/ListAjax$|LiveScoreWidget$/.test(pathOnly)){role='Current primary for Live';basis='Live source.js supplies these feeds; public-live.mjs also supports proxy routing. Source does not establish anonymous access.';built='Direct Live reader + shared proxy public route; exact parameter allowlist in public-live.mjs.';}
 if(/getLiveClassStatus$/.test(pathOnly)){role='Supporting status feed';basis='Supported shared public route. Current Live source.js has no separate status URL; do not claim scheduled Live usage.';built='workers/wef-schedules-decoded/public-live.mjs route supported; consumer not verified.';}
 if(pathOnly==='/utilities/get-config'){role='Current token bootstrap/renewal';basis='Runner and fallback cache anonymous _t from config. They do not currently adopt _t from an actual data response like main.js.';built='runner-relay.mjs sourceToken/renewSourceToken; fallback.mjs sourceToken/renewFallbackToken. No credential edits.';}
 if(pathOnly==='/version.json'){role='Current show-time clock source';basis='Clock-only recurring task reads version.json HTTP Date. Build version is not source-data health.';built='show-time-health.mjs CLOCK_URL; schedule/class health is separate.';}
 if(['/ring','/ring/{id}','/people/{id}','/entries','/classes'].includes(pathOnly)){built='Shared proxy supported route and decoder/fallback. Support is not proof of a scheduled consumer. '+(pathOnly==='/entries'?'Fast source-reader supports it; scheduled Fast does not separately collect entries.':'');}
 let sourceEvidence=requests.map(r=>({method:r.method,source_url_template:r.url,caller:r.caller,line:r.line,column:r.column,offset:r.offset,query:r.query,body:r.body,headers:r.headers,source_request:r.request}));
 const parameters=[...(pathParams.length?['Path parameters: '+(String(prior['source-route']||route).match(/\{[^}]+\}/g)||pathParams).join(', ')+'; original expressions in source-call-evidence.']:[]),...methods.map(method=>{
  const q=unique(requests.filter(r=>r.method===method).flatMap(r=>r.query.map(p=>p.name))),b=unique(requests.filter(r=>r.method===method).flatMap(r=>r.body.map(p=>p.name)));
  const embedded=unique(requests.filter(r=>r.method===method).flatMap(r=>{const query=r.url.replace(/\{[^}]*\}/g,'VALUE').split('?')[1];return query?query.split('&').map(k=>k.split('=')[0]):[];}));
  return method+': query ['+unique([...q,...embedded]).join(', ')+']; body ['+b.join(', ')+']. '+(q.includes('(dynamic fields)')||b.includes('(dynamic fields)')?'Dynamic object expression retained in source evidence; runtime keys are not invented.':'');
 })].join('\n');
 const fields={...prior,'source-route':prior['source-route']||route,method:methods.length?methods.join(' / '):prior.method,'scope':prior.scope||family(pathOnly),'observed-result':prior['observed-result']||'Static request and caller inspected in browser-loaded main.js. No source invocation in Stage 1.','test-status':prior['test-status']||(action?'Action — not tested':'Source only'),'public-access':prior['public-access']||'Not verified','access-evidence':prior['access-evidence']||'Static source only. Axios requests normally attach stored bearer/cookies and shared SGL request headers; no new runtime access claim.','next-check':prior['next-check']||(action||account?'Disposition is outside general collection. Do not invoke mutations while evaluating data routes.':'Stage 2: verify representative payload, identity, empty-data behavior and access; compare primary/fallback coverage.'),'source-js':prior['source-js']||main,evidence:doc+'\nStage 1: docs/coordination/evidence/sgl-stage1-20261111/requests.json; bundle SHA-256 '+p.source.sha256,'reviewed-on':'2026-11-11',priority:critical?'1 — Critical data and recovery':action||account?'3 — Account or action scope':'2 — Supporting data','evaluation-outcome':prior['test-status']==='Browser data'?'Useful in browser; access unresolved':prior['test-status']==='Browser empty'?'Valid empty browser result':prior['test-status']==='Browser denied'?'Access blocked':prior['test-status']==='Browser blocked'?'Browser blocked':action||account?'Not a collection source':'Pending','parameters':parameters||'Request occurs in retained legacy inline scripts/prior evidence; parameters in example-url and source evidence.','recommended-role':role,'role-basis':basis,'already-built':built,'source-call-evidence':JSON.stringify(sourceEvidence),'request-access':'Shared main.js Axios: withCredentials=true; bearer from stored access_token; SGL-REQUEST-ORIGIN=SGL-API; Cache-Control=no-cache, Pragma=no-cache, Expires=0. Config fetch uses credentials:omit. Direct legacy HTML/getJSON is separate; do not assume same headers.',};
 if(/getLiveClassData$/.test(pathOnly)){fields.parameters='GET: query [show_id, cid, t]. Legacy inline GetData constructs the URL; timestamp t is generated by Date.getTime().';fields['source-call-evidence']=JSON.stringify([{method:'GET',caller:'GetData',source:'Legacy liveclassv2/Detail inline script',query:['show_id','cid','t'],evidence:prior['source-js']}]);}
 fields['trigger-and-recovery']=/getLiveClassData$|\/Detail$/.test(pathOnly)?'Pusher class-ID channel, updateavailable event -> GetData/SendDataToParent. Conditional 20-second refresh. Parent frame getliveclass/sendliveclass messages. Event payload not runtime-verified.':/classsignupv2\/Index$/.test(pathOnly)?'Pusher show_id + company_id + cgid channel, rowupdate event -> JSON parse and grid update. Source verified; event payload not captured.':route.startsWith(API)?'Main.js saves response _t with Qe, recursively decodes with Ue, retries a GET once with __anonRetried. Decoder does not yet adopt data-response _t. This is a source comparison, not proof of a live failure.':'No independent trigger/recovery verified for this route; inspect its exact caller in source-call-evidence.';
 all.push({recordId:item.recordId,route,fields,requestCount:requests.length});
}
const output={source:p.source,total:all.length,rows:all,excludedVideoCalls:excluded.length,excludedVideoRoutes:unique(excluded.map(x=>canon(x.url))),unresolved:p.unresolved.map(x=>({...x,disposition:x.method==='REQUEST'?'Existing GET retry with original request config; no new route':/fetch\([et],/.test(x.request)?'Download of runtime video comparison asset, excluded by owner scope':'Subscriptions wrapper expanded from literal /web/* call sites; video subscription excluded by owner scope'}))};
fs.writeFileSync(path.join(dir,'catalog.json'),JSON.stringify(output,null,2));
console.log(JSON.stringify({records:all.length,new:all.filter(r=>!r.recordId).length,existing:all.filter(r=>r.recordId).length,missingMethods:all.filter(r=>!r.fields.method).map(x=>x.route),priorityCounts:all.reduce((a,x)=>(a[x.fields.priority]=(a[x.fields.priority]||0)+1,a),{}),routes:all.map(x=>({route:x.fields['source-route'],method:x.fields.method,query:x.fields.parameters})),excludedVideoRoutes:output.excludedVideoRoutes.length},null,2));
