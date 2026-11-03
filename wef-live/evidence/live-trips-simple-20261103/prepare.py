from pathlib import Path
r=Path(__file__).resolve().parent
p=r/'candidate'/'live-trips.js';s=p.read_text(encoding='utf-8').replace('const rows=[],results=[];','const rows=[];').replace("const result={'result-key':key,'show-id':s.show,date:s.date,'last-seen':at};",'const result={};')
s=s.replace("  result['source-json']=JSON.stringify({identity:Object.fromEntries(Object.values(RESULTS).filter(k=>supplied(raw[k])).map(k=>[k,raw[k]])),...(observed?{live:observed}:{})});\n  rows.push(row);results.push(result);", "  row['results-json']=JSON.stringify(result);\n  for(const name of ['placing','results-verified','class-status'])if(supplied(result[name]))row[name]=result[name];\n  if(model.number(result.position)!==null)row.position=model.number(result.position);\n  rows.push(row);")
s=s.replace('return{rows,results};','return{rows};');p.write_text(s,encoding='utf-8')
p=r/'candidate'/'lanes.js';s=p.read_text(encoding='utf-8');start=s.index("   const savedTrips=await db.list('live-trips'),tripResults=[];");end=s.index('\n  }\n }catch(e)',start);s=s[:start]+s[end:];p.write_text(s,encoding='utf-8')
p=r/'candidate'/'schema.js';s=p.read_text(encoding='utf-8');s='\n'.join(line for line in s.split('\n') if not any('"'+name+'":' in line for name in ['live-trip-results','live-trip-messages','live-trip-comments']));s=s.replace('"estimated-go-time","record-status"]','"estimated-go-time","record-status","placing","position","results-verified","class-status","results-json"]');s=s.replace('  "source-json",\n  "summary",','  "source-json",\n  "results-json",\n  "summary",');p.write_text(s,encoding='utf-8')
p=r/'candidate'/'cleanup.js';s=p.read_text(encoding='utf-8').replace("'live-trip-results','live-trip-messages','live-trip-comments',",'');p.write_text(s,encoding='utf-8')
p=r/'trips.test.js';s=p.read_text(encoding='utf-8').replace('assert.equal(r.results.length,1);','').replace('result=r.results[0]','result=JSON.parse(t[\'results-json\'])').replace("assert.equal(result['result-key'],t['trip-key']);", "assert.equal(t.placing,'2');").replace('r.results[0].position',"JSON.parse(r.rows[0]['results-json']).position");p.write_text(s,encoding='utf-8')
p=r/'lanes.test.js';s=p.read_text(encoding='utf-8');s=s.replace("assert.equal(a.db['live-trip-results'].length,2);", "assert(!a.db['live-trip-results']);")
start=s.index(" const result=a.db['live-trip-results'].find");end=s.index('\n const ids=',start)
s=s[:start]+" assert.equal(trip.fields.placing,'2');assert.equal(JSON.parse(trip.fields['results-json']).score1,'0');assert(!a.db['live-trip-results']);assert(!a.db['live-trip-messages']);assert(!a.db['live-trip-comments']);"+s[end:]
s=s.replace("trip.fields.messages=['recMessage'];trip.fields.comments=['recComment'];", "const links=structuredClone(trip.fields.class);")
s=s.replace("assert.deepEqual(a.db['live-trips'][0].fields.messages,['recMessage']);assert.deepEqual(a.db['live-trips'][0].fields.comments,['recComment']);assert.equal(a.db['live-trip-results'][0].fields.placing,'1');", "assert.deepEqual(a.db['live-trips'][0].fields.class,links);assert.equal(a.db['live-trips'][0].fields.placing,'1');assert(!a.db['live-trip-results']);")
s=s.replace('trip result changes retain trip ID and existing message/comment links','direct trip result changes retain trip ID and existing class links');p.write_text(s,encoding='utf-8')
(r/'TASK.md').write_text('''# Simplify live-trips — November3,2026

Latest owner correction says live-trip-results/messages/comments are unnecessary, following the approved trips build. Remove those three newly created empty tables and their new reciprocal fields/control rows. Each was GET-verified empty before preparation. Keep results directly on live-trips (placing,position,results-verified,class-status,results-json), with full source facts preserved. Downstream messages/comments reference the trip key; no local child tables or sending workflow.

Approved correction changes only live-trips.js, lanes.js, schema.js and cleanup.js plus new trip result fields and removal of the three empty tables/configuration rows. Preserve original trip key,existing class/ring/group/schedule/legacy links,source mapping,day0behavior,legacy live-entries,logging,active/current protection,delete-only cleanup and existing task/cadence. No manual collector/source calls,backfill,record repairs,new schedule,gates,dependencies or other-base changes. General lock remains outside this correction.

Baseline25JS/task captured before edit. Test/review before deploy. Remove obsolete schema only after collector no longer references it; GET-readback all changes. Natural inactive scheduled runs are not populated trip proof.
''',encoding='utf-8')
print('Single-table candidate and revised tests prepared')
