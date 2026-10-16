'use strict';
// Reuse the existing SGL browser loader and structured schedule HTML parser.
// No legacy Airtable writes, heartbeat flags, or order-of-go work here.
const {parseScheduleHtmlTimeOverlay}=require('./collector-core');

async function captureScheduleHtml(showId,date,{chromium}={}) {
  chromium ||= require('playwright').chromium;
  const browser=await chromium.launch({headless:true,channel:'chrome'});
  try {
    const page=await browser.newPage({viewport:{width:1440,height:1200}});
    const url=new URL('https://www.wellingtoninternational.com/showgrounds/show-schedule/');
    url.searchParams.set('date',date);url.searchParams.set('sid',String(showId));
    await page.goto(url.href,{waitUntil:'domcontentloaded',timeout:45000});
    await page.waitForTimeout(5000);
    const actual=new URL(page.url());
    if(actual.searchParams.get('sid')!==String(showId)||actual.searchParams.get('date')!==date)
      throw Error('Schedule page redirected outside requested show/day');
    return await page.locator('#sgl-root').innerHTML({timeout:15000});
  } finally {await browser.close();}
}

function matchTime(fields,items,groupClasses=[]) {
  const same=(a,b)=>a!=null&&b!=null&&String(a)===String(b);
  // The existing parser exposes group/ring/class identities. Do not use its
  // first-class-number map or the old scraper's substring/nearby-text match.
  const ring=items.filter(item=>same(item.ring_number,fields.ring_number));
  const group=ring.filter(item=>same(item.class_group_id,fields.class_group_id));
  // When links omit IDs, match the existing source group's complete class set.
  // Never select the first occurrence of an open-card/repeated class number.
  const signature=values=>[...new Set(values.map(String))].sort().join(',');
  const context=signature(groupClasses);
  const candidates=group.length?group:ring.filter(item=>
    item.class_group_id==null && item.class_numbers.some(n=>same(n,fields.class_number)) &&
    (groupClasses.length>1?signature(item.class_numbers)===context:true));
  return candidates.length===1?candidates[0].estimated_start_time:null;
}

async function enrichTimes({rows,previous=[],showId,date,capture=captureScheduleHtml}) {
  const old=new Map(previous.map(row=>[row.key,row]));
  const summary={show_id:showId,date,matched:0,preserved:0,manual:0,unmatched:0,error:null};
  let items=[];
  if(rows.length) {
    try {items=parseScheduleHtmlTimeOverlay(await capture(showId,date)).parsedRows;
      if(!items.length) throw Error('No timed schedule rows found');
    } catch(error) {summary.error=String(error.message||error);}
  }
  const inScope=fields=>String(fields.show_id)===String(showId)&&fields.show_date===date;
  const groupKey=fields=>JSON.stringify([fields.ring_number,fields.class_group_id]);
  const groups=new Map();
  for(const row of rows) {
    if(row.inactive||!inScope(row.fields)||row.fields.class_group_id==null) continue;
    const key=groupKey(row.fields);
    if(!groups.has(key)) groups.set(key,[]);
    groups.get(key).push(row.fields.class_number);
  }
  const updated=rows.map(row=>{
    if(row.inactive||!inScope(row.fields)) return row;
    const prior=old.get(row.key), fields={...row.fields};
    const manual=fields.manual_time_override===true||fields.manual_time_overide===true||
      prior?.fields.manual_time_override===true||prior?.fields.manual_time_overide===true;
    if(manual||row.refs?.schedule_html_time_overlay) {
      summary.manual++;
      if(manual) {
        fields.manual_time_override=true;
        fields.estimated_start_time=prior?.fields.estimated_start_time??fields.estimated_start_time??null;
      }
      return {...row,fields};
    }
    const time=summary.error?null:matchTime(fields,items,groups.get(groupKey(fields))||[]);
    if(time) {fields.estimated_start_time=time;summary.matched++;}
    else {
      summary.unmatched++;
      if(prior?.fields.estimated_start_time) {fields.estimated_start_time=prior.fields.estimated_start_time;summary.preserved++;}
    }
    return {...row,fields};
  });
  return {rows:updated,summary};
}
module.exports={captureScheduleHtml,matchTime,enrichTimes};
