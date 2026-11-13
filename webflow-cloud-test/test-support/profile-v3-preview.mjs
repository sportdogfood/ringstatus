// Local synthetic UI proof only. No credentials, external requests or Airtable writes.
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),root=path.join(base,'public/rs-profile-v3');
let actor='owner',delay=1200,requests=[],failScope=null;
const profiles=[{id:'owner',first:'Jamie',last:'Morgan',email:'jamie@example.test',phone:'2025550150',tags:['owner','trainer'],level:'Preferred',revision:0},{id:'member',first:'Alex',last:'Morgan',tags:['rider'],level:'Independent',revision:0},{id:'candidate',first:'Casey',last:'Test',tags:['groom'],level:'Preferred',revision:0}];
const memberships=[{id:'owner-membership',userId:'owner',teamId:'team',role:'Owner',active:true,revision:0},{id:'member-membership',userId:'member',teamId:'team',role:'Member',active:true,revision:0}];
const invitations=[];
function snapshot(view='team'){return {identity:{currentId:actor},profiles:profiles.filter(p=>view==='team'||view==='roster'&&memberships.some(m=>m.userId===p.id&&m.active)||p.id===actor).map(p=>p.id===actor?{...p}:{...p,email:'',phone:''}),teams:[{id:'team',name:'Test Team'}],memberships:memberships.filter(m=>view!=='profile'||m.userId===actor),invitations:view==='team'?invitations:[],loaded:{roster:view!=='profile',team:view==='team'},hidden:[]};}
const mime={'.js':'text/javascript','.css':'text/css','.html':'text/html','.ttf':'font/ttf','.svg':'image/svg+xml','.json':'application/json'};
http.createServer(async(req,res)=>{const url=new URL(req.url,'http://localhost');const send=(value,type='application/json',status=200)=>{res.writeHead(status,{'Content-Type':type,'Cache-Control':'no-store'});res.end(typeof value==='string'||Buffer.isBuffer(value)?value:JSON.stringify(value));};try{
 if(url.pathname==='/__fixture/identity'){if(req.method==='POST'){let raw='';for await(const c of req)raw+=c;const b=JSON.parse(raw);actor=b.signout?null:b.value==='1111'?'owner':b.value==='2222'?'member':b.value==='3333'?'candidate':null;}return send({uid:actor});}
 if(url.pathname==='/__fixture/evidence')return send({actor,requests});
 if(url.pathname==='/website')return send('<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><h1>Website content</h1><button id="website-button" onclick="this.textContent=\'Website remains usable\'">Website action</button><script type="module" src="/test/rs-profile-v3/embed.js"></script></body></html>','text/html');
 if(url.pathname==='/test/wef-schedule-v2/schedule.html')return send('<!doctype html><html><body><nav class="rings-profile-nav"><a href="/profile" target="_top">Profile</a></nav><h1>Schedule</h1></body></html>','text/html');
 if(url.pathname==='/profile'){
  const html=await readFile(path.join(base,'../docs/profile/v3-evidence/live-profile-source.html'),'utf8');
  return send(html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace('</body>','<script type="module" src="/test/rs-profile-v3/embed.js"></script></body>'),'text/html');
 }
 if(url.pathname==='/test/rs-recognition/widget.js'){const view=(await readFile(path.join(base,'src/assets/rs-recognition/widget-view.js'),'utf8')).replace("import widgetCss from './widget.css?raw';",'const widgetCss='+JSON.stringify(await readFile(path.join(base,'src/assets/rs-recognition/widget.css'),'utf8'))+';').replace("import screenCss from '../rs-ui/screen.css?raw';",'const screenCss='+JSON.stringify(await readFile(path.join(base,'src/assets/rs-ui/screen.css'),'utf8'))+';');return send(view+`let fixtureFingerprint=null;async function identity(body){const response=await fetch('/__fixture/identity',body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{});const data=await response.json();fixtureFingerprint=data.uid?'fixture-device-'+data.uid:null;return data.uid;}export const recognitionAdapter={current:()=>identity(),profileDeviceFingerprint:()=>fixtureFingerprint,recognize:value=>identity({value}),signOut:()=>identity({signout:true})};`,'text/javascript');}
 if(url.pathname==='/test/rs-profile/service.js')return send(await readFile(path.join(base,'src/assets/rs-profile/service.js')),'text/javascript');
 if(url.pathname==='/test/rs-recognition/native-client.js')return send(await readFile(path.join(base,'src/assets/rs-recognition/native-client.js')),'text/javascript');
 if(url.pathname==='/test/rs-profile/data'){
  if(!actor||req.headers['x-rs-actor-scope']!==actor)return send({ok:false,error:'unauthorized'},'application/json',403);
  const view=url.searchParams.get('view')||'team';requests.push({method:req.method,view,actor});
  if(req.method==='GET'&&view===failScope){failScope=null;return send({ok:false,error:'Fixture connection interrupted. Please retry.'},'application/json',503);}
  if(req.method==='POST'){let raw='';for await(const c of req)raw+=c;const b=JSON.parse(raw);requests.at(-1).operation=b.operation;requests.at(-1).body=b;const who=actor;await new Promise(r=>setTimeout(r,delay));if(who!==actor)return send({ok:false,error:'actor_changed'},'application/json',403);
   if(view==='geoip')return send({ok:true,saved:true,person_uid:actor,geoip:{latitude:b.latitude??26.634806,longitude:b.longitude??-80.274528,source:Number.isFinite(b.latitude)?'gps':'geoip',accuracy:b.accuracy,observedAt:b.observedAt||new Date().toISOString()}});
   if(b.operation==='profile'){const p=profiles.find(p=>p.id===actor),fields={...b.fields};delete fields.pin;Object.assign(p,fields,{revision:p.revision+1});}
   if(b.operation==='member_status'){const m=memberships.find(m=>m.id===b.membershipId);if(actor!=='owner'||m.role!=='Member')return send({ok:false,error:'forbidden'},'application/json',403);Object.assign(m,{active:b.active,revision:m.revision+1});}
   if(b.operation==='invitation_create')invitations.push({id:'invitation-1',userId:b.userId,teamId:'team',status:'pending',revision:0});
   if(b.operation==='invitation_response'){const i=invitations.find(i=>i.id===b.invitationId);if(i.userId!==actor)return send({ok:false,error:'forbidden'},'application/json',403);i.status=b.accept?'accepted':'declined';if(b.accept)memberships.push({id:'new-membership',userId:actor,teamId:'team',active:true,role:'Member',revision:0});}
   if(b.operation==='team_create')memberships.push({id:'new-team-owner',userId:actor,teamId:'team',active:true,role:'Owner',revision:0});
  }
  return send({ok:true,person_uid:actor,state:snapshot(view)});
 }
 if(!url.pathname.startsWith('/test/rs-profile-v3/'))return send('Not found','text/plain',404);
 if(['roster','team'].includes(url.searchParams.get('fixturefail')))failScope=url.searchParams.get('fixturefail');
 const relative=decodeURIComponent(url.pathname.slice('/test/rs-profile-v3/'.length))||'index.html',file=path.resolve(root,relative.endsWith('/')?relative+'index.html':relative);
 if(!file.startsWith(root+path.sep))return send('Not found','text/plain',404);
 return send(await readFile(file),mime[path.extname(file)]||'application/octet-stream');
 }catch(error){send({error:error.message},'application/json',500);}
}).listen(Number(process.env.PROFILE_V3_PREVIEW_PORT||8796),'127.0.0.1',()=>console.log('Synthetic v3 preview ready (localhost only).'));

