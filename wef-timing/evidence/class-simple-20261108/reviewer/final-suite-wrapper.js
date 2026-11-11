'use strict';
const fs=require('node:fs'),path=require('node:path');const original=fs.writeFileSync;
fs.writeFileSync=function(file,...args){const name=path.basename(String(file));if(['saved-class-snapshots.json','test-summary.json'].includes(name))file=path.join(__dirname,'final-'+name);return original.call(fs,file,...args);};
require('../test');
