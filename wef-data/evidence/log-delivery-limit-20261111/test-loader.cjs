'use strict';
const Module=require('node:module'),path=require('node:path');
const original=Module._load;
const candidate=path.resolve(__dirname,'candidate/attempt-log.js');
const suite=path.resolve(__dirname,'../../tests/attempt-log.test.js');
Module._load=function(request,parent,isMain){
 if(request==='../attempt-log'&&parent?.filename===suite)return original.call(this,candidate,parent,isMain);
 return original.call(this,request,parent,isMain);
};
