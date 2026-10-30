'use strict';
const fs=require('node:fs'),path=require('node:path');
function journal(root=require('./settings').RUNTIME){
 const file=path.join(root,'publication-state.json');
 let data={schedules:{pending:{},cursor:0},data:{pending:{},cursor:0}};
 if(fs.existsSync(file))data=JSON.parse(fs.readFileSync(file,'utf8'));
 for(const lane of ['schedules','data'])if(!data[lane]?.pending||!Number.isInteger(data[lane].cursor))throw Error('Invalid publication journal');
 function save(){fs.mkdirSync(root,{recursive:true});const body=JSON.stringify(data),tmp=file+'.tmp';const fd=fs.openSync(tmp,'w');try{fs.writeFileSync(fd,body);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}if(fs.readFileSync(tmp,'utf8')!==body)throw Error('Publication journal readback failed');fs.renameSync(tmp,file);if(fs.readFileSync(file,'utf8')!==body)throw Error('Publication journal replacement failed');}
 return {data,save};
}
module.exports={journal};
