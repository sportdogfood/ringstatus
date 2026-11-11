// Read-only static inventory of the browser-loaded bundle; never invokes SGL.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),acorn=require('acorn');
const dir=__dirname,source=fs.readFileSync(path.join(dir,'main.loaded.js'),'utf8');
const ast=acorn.parse(source,{ecmaVersion:'latest',locations:true});
const API='https://sglapi.wellingtoninternational.com',SGL='https://sgl.wellingtoninternational.com';
const scopes=[],calls=[],returns=[],members=[],rendered=[],parents=new WeakMap(),scopeOf=new WeakMap();
let root={parent:null,bindings:new Map(),node:ast};scopes.push(root);
const key=n=>n?.type==='Identifier'?n.name:n?.type==='Literal'?String(n.value):null;
const code=n=>n?source.slice(n.start,n.end):'';
function bind(s,name,node){if(!name)return;if(!s.bindings.has(name))s.bindings.set(name,[]);s.bindings.get(name).push(node);}
function walk(n,s,parent){
 if(!n||typeof n.type!=='string')return;
 if(parent)parents.set(n,parent);
 if(/Function/.test(n.type)){s={parent:s,bindings:new Map(),node:n};scopes.push(s);for(const p of n.params||[])bind(s,key(p),{argument:key(p),start:n.start});}
 scopeOf.set(n,s);
 if(n.type==='VariableDeclarator')bind(s,key(n.id),n.init);
 if(n.type==='AssignmentExpression'&&n.left.type==='Identifier'){let target=s;while(target.parent&&!target.bindings.has(n.left.name))target=target.parent;bind(target,n.left.name,n.right);}
 if(n.type==='CallExpression'||n.type==='NewExpression')calls.push(n);
 if(n.type==='ReturnStatement'&&n.argument)returns.push(n);
 if(n.type==='MemberExpression')members.push(n);
 if(n.type==='Property'&&['src','href'].includes(key(n.key)))rendered.push(n);
 for(const [k,v]of Object.entries(n)){if(['loc','start','end'].includes(k))continue;if(Array.isArray(v))for(const c of v)walk(c,s,n);else if(v&&typeof v==='object'&&v.type)walk(v,s,n);}
}
walk(ast,root,null);
const getters=new Map();
for(const n of calls){for(const arg of n.arguments||[]){if(arg.type!=='ArrayExpression')continue;for(const obj of arg.elements){if(obj?.type!=='ObjectExpression')continue;const props=Object.fromEntries(obj.properties.filter(p=>p.type==='Property').map(p=>[key(p.key),p.value]));if(props.key?.type==='Literal'&&props.get){const ret=returns.find(r=>r.start>props.get.start&&r.end<props.get.end);if(ret){const name=String(props.key.value);if(!getters.has(name))getters.set(name,[]);getters.get(name).push(ret.argument);}}}}}
function findBinding(n,name){let s=scopeOf.get(n);while(s){if(s.bindings.has(name))return s.bindings.get(name).filter(Boolean).sort((a,b)=>Math.abs(a.start-n.start)-Math.abs(b.start-n.start));s=s.parent;}return [];}
const uniq=a=>[...new Set(a)];
function combine(a,b){return uniq(a.flatMap(x=>b.map(y=>x+y))).slice(0,20);}
function value(n,depth=0,seen=new Set()){
 if(!n||depth>14)return ['{dynamic}'];
 if(seen.has(n))return ['{cycle}'];seen=new Set(seen);seen.add(n);
 const v=x=>value(x,depth+1,seen);
 if(n.argument&&!n.type)return ['{'+n.argument+'}'];
 if(n.type==='Literal')return [String(n.value??'')];
 if(n.type==='TemplateLiteral'){let out=[''];n.quasis.forEach((q,i)=>{out=combine(out,[q.value.cooked||q.value.raw]);if(n.expressions[i])out=combine(out,v(n.expressions[i]));});return out;}
 if(n.type==='Identifier'){
  const defs=findBinding(n,n.name);if(!defs.length)return ['{'+n.name+'}'];
  const known=uniq(defs.slice(0,4).flatMap(v));return known.length?known:['{'+n.name+'}'];
 }
 if(n.type==='BinaryExpression'&&n.operator==='+')return combine(v(n.left),v(n.right));
 if(n.type==='SequenceExpression')return v(n.expressions.at(-1));
 if(n.type==='ConditionalExpression')return uniq([...v(n.consequent),...v(n.alternate)]).slice(0,20);
 if(n.type==='LogicalExpression')return uniq([...v(n.left),...v(n.right)]).slice(0,20);
 if(n.type==='MemberExpression'){
  const p=key(n.property);
  if(['nestApiUrl','sglNestApiUrl','api_base_url','SGL_API_BASE'].includes(p))return [API];
  if(p==='sglApiUrl')return [SGL];
  if(p==='company_urlpath'||p==='company_url_path')return ['esp'];
  if(p?.startsWith('sgl_webservice_')||p==='apiBase'||p==='user_info_url')return uniq((getters.get(p)||[]).flatMap(v));
  if(p==='REACT_APP_NEST_API')return [API];if(p==='REACT_APP_SGL_API')return [SGL];
  return ['{'+code(n).replace(/[^\w.]/g,'').slice(0,65)+'}'];
 }
 if(n.type==='CallExpression'){
  const method=key(n.callee.property);
  if(method==='concat'){let out=v(n.callee.object);for(const a of n.arguments)out=combine(out,v(a));return out;}
  if(method==='getQueryVariable'||method==='get'){
   const a=n.arguments[0];if(a?.type==='Literal')return ['{'+String(a.value)+'}'];
  }
  if(['replace','replaceAll','toString'].includes(method))return v(n.callee.object);
  if(['String','Number','parseInt','encodeURIComponent'].includes(key(n.callee)))return v(n.arguments[0]);
 }
 return ['{'+code(n).replace(/\s+/g,' ').slice(0,75)+'}'];
}
function objectNode(n,depth=0){if(!n||depth>6)return null;if(n.type==='ObjectExpression')return n;if(n.type==='Identifier')return objectNode(findBinding(n,n.name)[0],depth+1);if(n.type==='CallExpression'&&n.callee.object?.name==='JSON'&&key(n.callee.property)==='stringify')return objectNode(n.arguments[0],depth+1);return null;}
function property(n,name){const o=objectNode(n);return o?.properties.find(p=>p.type==='Property'&&key(p.key)===name)?.value;}
function fields(n){const o=objectNode(n);if(!o)return n?[{name:'(dynamic fields)',expression:code(n).slice(0,250)}]:[];return o.properties.map(p=>p.type==='SpreadElement'?{name:'(spread)',expression:code(p.argument).slice(0,120)}:{name:key(p.key)||'{computed}',expression:/password|secret|token|authorization/i.test(key(p.key)||'')?'[sensitive input; value omitted]':code(p.value).slice(0,160)});}
function functionName(n){let p=n;while(p){if(/Function/.test(p.type)){if(p.id?.name)return p.id.name;let q=parents.get(p);if(q?.type==='Property'){const container=parents.get(q);const k=container?.properties?.find(x=>key(x.key)==='key')?.value;if(k?.type==='Literal')return String(k.value);}if(q?.type==='VariableDeclarator')return key(q.id);}p=parents.get(p);}return '(anonymous application function)';}
const found=[],unresolved=[],other=[],rawCalls=[];
for(const n of calls){
 const member=n.callee?.type==='MemberExpression',verb=member?key(n.callee.property):key(n.callee);
 if(!['get','getJSON','post','put','patch','delete','request','fetch','open','sendBeacon'].includes(verb))continue;
 if(n.start<3050000)continue; // app module begins after bundled third-party modules
 const isWindowOpen=verb==='open'&&n.callee.object?.name==='window';
 const target=n.arguments[verb==='open'&&!isWindowOpen?1:0];if(!target)continue;
 let urls=value(target);
 // A reset-token helper receives its API base explicitly from its caller.
 if(code(target)==='"".concat(e,"/auth/verify-reset-token")')urls=[API+'/auth/verify-reset-token'];
 const method=verb==='fetch'?(value(property(n.arguments[1],'method'))[0]||'GET'):isWindowOpen?'GET':verb==='open'?value(n.arguments[0])[0]:verb==='getJSON'?'GET':verb==='sendBeacon'?'POST':verb.toUpperCase();
 const cleanMethod=verb==='fetch'&&!property(n.arguments[1],'method')?'GET':method;
 const config=verb==='fetch'?n.arguments[1]:n.arguments[cleanMethod==='GET'||cleanMethod==='DELETE'?1:2];
 const bodyNode=verb==='fetch'?property(config,'body'):n.arguments[1];
 let matched=false;
 for(let url of urls){
  if(url.startsWith('add-entry/'))url=API+'/'+url;
  if(/^\/web\//.test(url)&&verb==='post')url=API+'/subscriptions'+url;
  if(url.startsWith('/'))url=(url.startsWith('/iphone')?SGL:API)+url;
  if(!/^https:\/\/(?:(?:sglapi|sgl)\.wellingtoninternational\.com|sglapp\.showgroundslive\.com)\//.test(url))continue;
  if(url===API+'/'||url===SGL+'/'||url===API+'/subscriptions{e}')continue;
  matched=true;
  found.push({url,method:cleanMethod,query:fields(property(config,'params')),body:['POST','PUT','PATCH'].includes(cleanMethod)?fields(bodyNode):[],headers:fields(property(config,'headers')),caller:functionName(n),offset:n.start,line:n.loc.start.line,column:n.loc.start.column,request:code(n).slice(0,1800)});
 }
 const expr=code(target);
 if(!matched){const item={method:cleanMethod,expression:expr.slice(0,300),values:urls.slice(0,5),offset:n.start,caller:functionName(n),request:code(n).slice(0,1000)};
  if(/ApiUrl|webservice|nestApi|sglApi|fetch\(/.test(code(n))||/^(?:me\(\)|iu|Ot|Xc)\./.test(code(n)))unresolved.push(item);else other.push(item);
 }
 rawCalls.push({offset:n.start,verb,matched});
}
for(const n of rendered){if(n.start<3050000)continue;for(const url of value(n.value)){
 if(!/^https:\/\/(?:sglapi|sgl)\.wellingtoninternational\.com\//.test(url))continue;
 if(url===API+'/'||url===SGL+'/')continue;
 found.push({url,method:'GET',query:[],body:[],headers:[],caller:functionName(n)+' (rendered '+key(n.key)+')',offset:n.start,line:n.loc.start.line,column:n.loc.start.column,request:code(n).slice(0,1800)});
}}
const out={source:{url:'https://sglapp.showgroundslive.com/static/js/main.js',sha256:crypto.createHash('sha256').update(source).digest('hex'),characters:source.length,normalizedCharacters:source.replace(/\r\n/g,'\n').length},found,unresolved,other,rawCalls};
fs.writeFileSync(path.join(dir,'requests.json'),JSON.stringify(out,null,2));
console.log(JSON.stringify({resolvedCalls:found.length,unresolvedCalls:unresolved.length,otherCalls:other.length,rawCalls:rawCalls.length,urls:uniq(found.map(x=>x.url)).length,unresolved:unresolved.map(x=>({method:x.method,url:x.expression,caller:x.caller}))},null,2));
