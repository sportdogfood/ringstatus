const test=require('node:test'),assert=require('node:assert/strict');
const {logFields}=require('./candidate/wef-live/run-log-fields');
test('missing IDs stay blank, no parsing or invented IDs',()=>{
 assert.deepEqual(logFields('classes','run',{table:'live-trips',key:'15|75|day|3|30||400'}),{lane:'classes','record-type':'trip','record-key':'15|75|day|3|30||400'});
});
test('multiple class links do not select a class or ring',()=>{
 const data={'live-entries':[{id:'entry',fields:{'live-entry-key':'key','trip-id':400,class:['a','b']}}],'live-classes':[{id:'a',fields:{'class-id':1}},{id:'b',fields:{'class-id':2}}]};
 assert.deepEqual(logFields('classes','run',{table:'live-entries',key:'key'},data),{lane:'classes','record-type':'entry','record-key':'key','trip-id':400});
});
test('six approved fields present with numeric IDs and text descriptors',()=>{
 const {tables,field}=require('./candidate/wef-live/schema');for(const name of ['lane','record-type','record-key','trip-id','class-id','ring-id']){assert(tables['run-logs'].includes(name));assert.equal(field(name).type,name.endsWith('-id')?'number':'singleLineText');}
});
