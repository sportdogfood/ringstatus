'use strict';
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const directory=path.resolve(__dirname,'..'),file=path.join(directory,'preflight.cjs');
// Execute the actual offline preflight; redirect only its evidence output so the
// writer's preflight.json remains intact. All other attempted writes fail closed.
const scopedFs={...fs,writeFileSync:(target,...args)=>{
 if(path.resolve(target)!==path.join(directory,'preflight.json'))throw Error('Unexpected preflight write');
 return fs.writeFileSync(path.join(__dirname,'independent-preflight.json'),...args);
}};
vm.runInNewContext(fs.readFileSync(file,'utf8'),{require:name=>['fs','node:fs'].includes(name)?scopedFs:require(name),__dirname:directory,console,process},{filename:file});
