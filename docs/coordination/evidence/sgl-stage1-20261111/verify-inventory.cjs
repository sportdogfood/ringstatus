// Offline inventory integrity checks. No endpoint requests or business writes.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),assert=require('assert/strict');
const dir=__dirname,read=name=>JSON.parse(fs.readFileSync(path.join(dir,name),'utf8'));
const source=fs.readFileSync(path.join(dir,'main.loaded.js'),'utf8'),raw=read('requests.json'),catalog=read('catalog.json'),prior=read('existing-records.json');
assert.equal(crypto.createHash('sha256').update(source).digest('hex'),raw.source.sha256);
assert.equal(source.replace(/\r\n/g,'\n').length,5472083);
assert.equal(catalog.total,catalog.rows.length);
assert.equal(new Set(catalog.rows.map(r=>r.route)).size,catalog.total);
assert.equal(new Set(catalog.rows.map(r=>r.fields['source-route'])).size,catalog.total);
assert.equal(catalog.rows.filter(r=>r.recordId).length,prior.length);
for(const row of catalog.rows){
 for(const field of ['source-route','method','parameters','scope','priority','recommended-role','role-basis','already-built','source-call-evidence','request-access','next-check'])assert.ok(row.fields[field],row.route+' missing '+field);
 assert.ok(!/video|livestream|channel|subscriptions/i.test(row.route),'Excluded video route included');
 assert.ok(!row.fields.parameters.includes('body [method, headers'),'Fetch options incorrectly treated as body');
 assert.ok(JSON.parse(row.fields['source-call-evidence']));
 assert.ok(!Object.values(row.fields).some(v=>/Bearer eyJ|symfony=|sk-proj-/.test(v)),'Credential literal in inventory');
}
for(const request of raw.found){
 assert.equal(source.slice(request.offset,request.offset+request.request.length),request.request);
 assert.ok(request.line>0&&request.column>=0);
}
const boundary=3050000,needles=['sglapi.wellingtoninternational.com','sgl.wellingtoninternational.com','/utilities/get-config','nestApiUrl','sglApiUrl'];
const firstSourceReferences=Object.fromEntries(needles.map(n=>[n,source.indexOf(n)]));
const report={checkedAt:new Date().toISOString(),result:'PASS',source:raw.source,records:catalog.total,existingPreserved:prior.length,newRecords:catalog.total-prior.length,sourceRequests:raw.found.length,excludedVideoRoutes:catalog.excludedVideoRoutes.length,classifiedDynamicCalls:catalog.unresolved.map(r=>({offset:r.offset,disposition:r.disposition})),firstSourceReferences,scanBoundary:boundary,priorityCounts:catalog.rows.reduce((a,r)=>(a[r.fields.priority]=(a[r.fields.priority]||0)+1,a),{}),builtSourceRows:catalog.rows.filter(r=>!r.fields['already-built'].startsWith('Not found')).length};
fs.writeFileSync(path.join(dir,'offline-verification.json'),JSON.stringify(report,null,2));
console.log(JSON.stringify(report,null,2));
