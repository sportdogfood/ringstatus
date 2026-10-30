const fs=require('fs'),path=require('path'),Module=require('module');
const root=path.resolve(__dirname,'../..'),stage=path.join(__dirname,'staged'),source=path.join(root,'day-source.js');
const actual=Module._resolveFilename;Module._resolveFilename=function(name,parent,...rest){if(name==='./day-archive'&&parent?.filename===source)return path.join(stage,'day-archive.js');return actual.call(this,name,parent,...rest);};
const load=Module._extensions['.js'];Module._extensions['.js']=function(module,file){if(file===source){let text=fs.readFileSync(path.join(stage,'day-source.js'),'utf8').replace("path.join(__dirname,'day-replay.py')",JSON.stringify(path.join(stage,'day-replay.py')));return module._compile(text,file);}return load(module,file);};

