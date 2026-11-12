'use strict';
// Existing linking task entry. Saved Data only; no feeds or collection.
const path=require('node:path');
require(path.join(__dirname,'..','fast-entry.js')).main({linking:true}).catch(e=>{console.error(e.message);process.exitCode=1;});
