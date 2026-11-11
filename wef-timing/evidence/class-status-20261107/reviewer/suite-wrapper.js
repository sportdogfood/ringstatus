'use strict';
const fs=require('node:fs'),path=require('node:path');
const original=fs.writeFileSync;
fs.writeFileSync=function(file,...args){
 const name=path.basename(String(file));
 if(['yesterday-class-status.json','test-summary.json'].includes(name))file=path.join(__dirname,name);
 return original.call(fs,file,...args);
};
require('../offline-test');
