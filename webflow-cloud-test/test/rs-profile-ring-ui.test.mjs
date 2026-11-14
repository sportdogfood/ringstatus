import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {estimateWalk,withinRing} from '../public/rs-profile-v3/ring-distance.js';
const app=readFileSync(new URL('../public/rs-profile-v3/app.js',import.meta.url),'utf8');
const block=app.slice(app.indexOf('function ringDistanceSection()'),app.indexOf('function genericGroupSections()'));
function markup(point,allowed=true){const ctx={ui:{locationAllowed:{a:allowed}},state:{currentUserId:'a'},locationPermission:'prompt',ringSample:point?{uid:'a',...point}:null,canonical:null,ringExpiry:null,Date,estimateWalk,withinRing,clearTimeout(){},setTimeout(){},section:(_,title,body)=>title+body,button:(_,label)=>label,locationField:()=>'<input type="checkbox">',actor:()=>({id:'a'})};vm.createContext(ctx);vm.runInContext(block,ctx);return vm.runInContext('ringDistanceSection()',ctx);}
const point=(accuracy=5)=>({latitude:26.634806,longitude:-80.274528,accuracy,source:'gps',observedAt:new Date().toISOString()});
test('walking estimate remains separate from qualification',()=>{assert.match(markup(point()),/~0.0 min.*±17 ft.*Yes/s);assert.match(markup({...point(),latitude:26.635028,longitude:-80.273806}),/~0.9 min.*No/s);assert.match(markup(point(40)),/~0.0 min.*Not confirmed/s);});
test('GeoIP and stale coordinates retain walking estimate without qualification',()=>{assert.match(markup({...point(),source:'geoip'}),/~0.0 min.*Not confirmed.*GeoIP cannot confirm/s);assert.match(markup({...point(),observedAt:new Date(Date.now()-61000).toISOString()}),/~0.0 min.*Not confirmed.*older than 60/s);assert.doesNotMatch(markup(point(),false),/~0.0 min/);});
function north(feet){return {...point(),latitude:26.634806+(feet*0.3048/6371008.8)*180/Math.PI};}
test('walking shown below 2000ft; accuracy and qualification remain above cutoff',()=>{assert.match(markup(north(1999)),/Walk to Ring 6/);assert.doesNotMatch(markup(north(2001)),/Walk to Ring 6/);assert.match(markup(north(2001)),/Location accuracy.*Within 100 feet/s);});
