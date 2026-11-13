import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {estimateWalk,withinRing} from '../public/rs-profile-v3/ring-distance.js';
const app=readFileSync(new URL('../public/rs-profile-v3/app.js',import.meta.url),'utf8');
const block=app.slice(app.indexOf('function ringDistanceSection()'),app.indexOf('function genericGroupSections()'));
function markup(point,allowed=true){const ctx={ui:{locationAllowed:{a:allowed}},state:{currentUserId:'a'},locationPermission:'prompt',ringSample:point?{uid:'a',...point}:null,canonical:null,ringExpiry:null,Date,estimateWalk,withinRing,clearTimeout(){},setTimeout(){},section:(_,title,body)=>title+body,button:(_,label)=>label,locationField:()=>'<input type="checkbox">',actor:()=>({id:'a'})};vm.createContext(ctx);vm.runInContext(block,ctx);return vm.runInContext('ringDistanceSection()',ctx);}
const point=(accuracy=5)=>({latitude:26.634806,longitude:-80.274528,accuracy,source:'gps',observedAt:new Date().toISOString()});
test('Ring 6 UI renders feet and conservative inside/outside/uncertain results',()=>{assert.match(markup(point()),/0 ft.*±17 ft.*Yes/s);assert.match(markup({...point(),latitude:26.635028,longitude:-80.273806}),/249 ft.*No/s);assert.match(markup(point(40)),/Not confirmed/);});
test('GeoIP, stale GPS and withdrawn choice never qualify',()=>{assert.match(markup({...point(),source:'geoip'}),/GeoIP cannot confirm/);assert.match(markup({...point(),observedAt:new Date(Date.now()-61000).toISOString()}),/Not confirmed.*older than 60/s);assert.doesNotMatch(markup(point(),false),/0 ft/);assert.match(markup(point(),false),/Allow precise location/);});
