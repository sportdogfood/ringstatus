// Offline test process only: stage the one schedules settings change without
// switching the scheduled collector before the new endpoint is tested.
const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');
const target=path.resolve(__dirname,'../../../wef-schedules/settings.js');
const original=Module._extensions['.js'];
Module._extensions['.js']=function(module,filename){
 if(path.resolve(filename)===target)return module._compile(fs.readFileSync(path.join(__dirname,'settings-after.js'),'utf8'),filename);
 return original(module,filename);
};
