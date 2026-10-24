"use strict";
const model=require('./model');
function unique(rows,key){const seen=new Set();for(const r of rows){const value=r.fields[key];if(value==null||value===''||seen.has(value))throw Error('Duplicate or missing synced '+key);seen.add(value);}}
function sameLinks(actual,expected,label){if(!Array.isArray(actual)&&expected.length)throw Error('Incomplete sync reference '+label);if(JSON.stringify([...(actual||[])].sort())!==JSON.stringify([...expected].sort()))throw Error('Incomplete sync reference '+label);}
function validateReferences({shows,rings,schedules,now}){
 const show=model.validateShow(shows.map(r=>r.fields),now),showRecord=shows.find(r=>r.fields['show-key']===show['show-key']),sid=Number(show.show_id),dates=model.showDays(show);
 unique(shows,'show-key');const ringRows=rings.filter(r=>Number(r.fields.show_id)===sid),scheduleRows=schedules.filter(r=>Number(r.fields['show-id'])===sid);
 unique(ringRows,'ring-key');unique(scheduleRows,'stage-key');
 if(dates.some(date=>!ringRows.some(r=>r.fields.ring_date===date)))throw Error('Synced rings do not cover every show day');
 sameLinks(show.rings,ringRows.map(r=>r.id),'show rings');sameLinks(show.schedules,scheduleRows.map(r=>r.id),'show schedules');
 for(const ring of ringRows){const f=ring.fields;if(!dates.includes(f.ring_date))throw Error('Synced ring date outside show');sameLinks(f['show-details'],[showRecord.id],'ring show');sameLinks(f.schedules,scheduleRows.filter(s=>s.fields.date===f.ring_date&&Number(s.fields.ring)===Number(f.ring_number)).map(s=>s.id),'ring schedules');}
 for(const schedule of scheduleRows){const f=schedule.fields,matching=ringRows.filter(r=>r.fields.ring_date===f.date&&Number(r.fields.ring_number)===Number(f.ring));if(matching.length!==1)throw Error('Ambiguous or missing synced schedule ring');sameLinks(f['show-details'],[showRecord.id],'schedule show');sameLinks(f.rings,[matching[0].id],'schedule ring');}
 return{show,showRecord,rings:ringRows,schedules:scheduleRows};
}
module.exports={validateReferences};
