"use strict";
const {scheduled}=require('./lanes');
if(require.main===module)scheduled().then(reports=>{
 console.log(JSON.stringify(reports));
 process.exitCode=Array.isArray(reports)&&reports.some(r=>!r.ok)?1:0;
}).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={scheduled};
