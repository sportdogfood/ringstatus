const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');
const root=path.resolve(__dirname,'../..'),stage=path.join(__dirname,'staged');
for(const name of ['collector.js','airtable.js']){const file=path.join(root,name),m=new Module(file,module);m.filename=file;m.paths=Module._nodeModulePaths(root);const original=Module.createRequire(file);m.require=id=>id==='./audit'?require(path.join(stage,'audit')):original(id);m._compile(fs.readFileSync(path.join(stage,name),'utf8'),file);require.cache[file]=m;}
const file=path.join(root,'tests/shows.test.js'),m=new Module(file,module);m.filename=file;m.paths=Module._nodeModulePaths(path.dirname(file));m._compile(fs.readFileSync(path.join(stage,'tests/shows.test.js'),'utf8'),file);
