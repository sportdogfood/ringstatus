const H="ringstatus.schedule.prototype.lastGood.v1",D="/schedule/data/latest/watch_schedule.json",U="/schedule/data/latest/watch_trips.json";function P(e){return Array.isArray(e)?e:Array.isArray(e?.rows)?e.rows:Array.isArray(e?.schedule)?e.schedule:Array.isArray(e?.trips)?e.trips:Array.isArray(e?.records)?e.records:[]}async function V(e){const t=await fetch(e,{cache:"no-store"});if(!t.ok)throw new Error(`${e} ${t.status}`);return t.json()}function ne(){try{return JSON.parse(localStorage.getItem(H)||"null")}catch(e){return console.warn("schedule_cache_read_failed",e),null}}function ie(e){try{localStorage.setItem(H,JSON.stringify(e))}catch(t){console.warn("schedule_cache_write_failed",t)}}async function oe(){const e=new Date().toISOString();try{const[t,s]=await Promise.all([V(D),V(U)]),l=P(t),r=P(s),a={schedule:l,trips:r,meta:{scheduleSource:t?.source||D,tripsSource:s?.source||U,lastFetchedAt:e,lastGeneratedAt:t?.generated_at||s?.generated_at||e,usedCache:!1}};return(l.length||r.length)&&ie(a),{ok:!0,...a}}catch(t){console.warn("schedule_data_fetch_failed",t);const s=ne();return s?.schedule||s?.trips?{ok:!1,schedule:s.schedule||[],trips:s.trips||[],meta:{...s.meta||{},lastFetchedAt:e,usedCache:!0},error:"Could not refresh data. Showing last known data if available."}:{ok:!1,schedule:[],trips:[],meta:{lastFetchedAt:e,usedCache:!1},error:"Could not refresh data. Showing last known data if available."}}}function c(e){return String(e??"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#039;")}function b(e){return String(e??"").trim().toLowerCase().replace(/\s+/g," ")}function $(e){return e.filter(t=>t!=null&&String(t).trim()!=="")}function d(e,t){const s=e?.fields&&typeof e.fields=="object"?e.fields:e;for(const l of t){const r=s?.[l];if(Array.isArray(r)&&r.length)return r[0];if(r!=null&&String(r).trim()!=="")return r}return null}function S(e){if(e==null||e==="")return null;const t=Number(String(e).replace(/,/g,""));return Number.isFinite(t)?t:null}function ce(e){if(e===!0)return!0;const t=b(e);return t==="true"||t==="1"||t==="yes"||t==="y"}function k(e){return String(e??"").trim()||"unknown"}function L(e,t,s=`${t}s`){const l=Number(e)||0;return`${l} ${l===1?t:s}`}function C(e,t){const s=S(e.sortValue??e.ringNumber??e.classNumber),l=S(t.sortValue??t.ringNumber??t.classNumber);return s!==null&&l!==null&&s!==l?s-l:s!==null&&l===null?-1:s===null&&l!==null?1:String(e.label||e.name||"").localeCompare(String(t.label||t.name||""))}function z(e,t={}){const s=S(t.completedTrips??t.completed_trips??t.completed),l=S(t.remainingTrips??t.remaining_trips??t.remaining),r=t.estimatedStart||t.startDisplay||t.latestStart||t.estimatedGO||t.latestGO,a=b(e||t.status||t.latestStatus||t.latest_status);return s!==null&&s>0&&l===0||/complete|completed|done|result|placed|score|finished/.test(a)?"completed":/live|current|running|now|active|in progress/.test(a)?"live":r?"upcoming":"unknown"}function ue(e,t){const s=new Set,l=[];for(const r of e){const a=t(r);!a||s.has(a)||(s.add(a),l.push(r))}return l}function J(e){return e?.fields&&typeof e.fields=="object"?e.fields:e}function W(e){const t=d(e,["ring_number","ringNumber","ring_no","ring"]),s=d(e,["ring_id","ringId"]),l=d(e,["ringName","ring_name","ring_nickname"]);return b(t||s||l||"unmatched-ring")||"unmatched-ring"}function X(e){return $([d(e,["class_number","classNumber"]),d(e,["class_name","className","group_name","group_display"])]).join(" - ")}function de(e){return $([e.classGroupId&&`class-group:${e.classGroupId}`,e.classId&&`class:${e.classId}`,e.ringKey&&e.classNumber&&`ring-class-number:${e.ringKey}:${b(e.classNumber)}`,e.ringKey&&e.classLabel&&`ring-class-label:${e.ringKey}:${b(e.classLabel)}`,e.classTextKey&&`class-text:${e.classTextKey}`])}function pe(e){return $([e.classGroupId&&`class-group:${e.classGroupId}`,e.classId&&`class:${e.classId}`,e.ringKey&&e.classNumber&&`ring-class-number:${e.ringKey}:${b(e.classNumber)}`,e.ringKey&&e.classLabel&&`ring-class-label:${e.ringKey}:${b(e.classLabel)}`,e.classTextKey&&`class-text:${e.classTextKey}`])}function ge(e,t){const s=J(e),l=d(s,["ring_number","ringNumber","ring_no","ring"]),r=d(s,["ringName","ring_name","ring_nickname"])||(l?`Ring ${l}`:null),a=d(s,["class_group_id","group_id","class_groupxclasses_id"]),i=d(s,["class_groupxclasses_id","class_group_id","group_id"]),g=d(s,["class_id","entryxclasses_uuid"]),o=d(s,["class_number","classNumber"]),p=d(s,["class_name","className"]),w=d(s,["group_display","group_name","group_label"])||p||"unknown group",v=X(s)||w||"unknown class",n=W(s),_=b(a||`${n}:${w}`)||`group:${t}`,f=b(g||i||`${n}:${v}`)||`class:${t}`,h=S(d(s,["completed_trips","completedTrips","completed"])),m=S(d(s,["remaining_trips","remainingTrips","remaining"])),T=S(d(s,["total_trips","totalTrips","rollup_trips","rollup_entries"])),M=d(s,["status","latestStatus","latest_status","scope_status"]),ae=d(s,["estimated_start_time","estimatedStart","latestStart","start_display","start_display_short"]),I={id:`schedule:${f||_||t}`,source:e,index:t,showId:d(s,["show_id","sid","app_show_id","app_show_idv2"]),showDate:d(s,["show_date","dt","app_sql_date","app_sql_datev2","scheduled_date"]),showLabel:d(s,["show_days_report_title","show_days_display_date","show_name"]),ringNumber:l,ringName:r,ringLabel:r||(l?`Ring ${l}`:"unknown ring"),ringKey:n,groupId:a,groupLabel:w,groupKey:_,classGroupId:i,classId:g,classNumber:o,className:p,classLabel:v,classKey:f,classTextKey:b($([o,p,w]).join(" ")),estimatedStart:ae,startDisplay:d(s,["start_display","start_display_short","latestStart","estimated_start_time"]),statusRaw:M,completedTrips:h,remainingTrips:m,totalTrips:T,matchingTrips:[],matchingTripCount:0,hasFollowedTrips:!1,sortValue:S(d(s,["time_sort","class_group_sequence","schedule_sequence","ring_number"]))};return I.statusBucket=z(M,I),I}function he(e,t){const s=J(e),l=d(s,["ring_number","ringNumber","ring_no","ring"]),r=d(s,["ringName","ring_name","ring_nickname"])||(l?`Ring ${l}`:null),a=d(s,["class_groupxclasses_id","class_group_id","group_id"]),i=d(s,["class_id","entryxclasses_uuid"]),g=d(s,["class_number","classNumber"]),o=d(s,["class_name","className"]),p=d(s,["group_display","group_name","group_label"])||o||"unknown group",w=X(s)||p||"unknown class",v=W(s),n=d(s,["horseName","horse_name","horse","barnName","sched_display","teamName"]),_=d(s,["riderName","rider_name","rider","groomName"]),f=d(s,["trip_id","tripId","entryxclasses_uuid","entry_id"]),h=d(s,["latestGO","latest_go","estimatedGO","estimated_go_time","rs_go_time"]),m=d(s,["status","latestStatus","latest_status"]),T={id:`trip:${f||`${v}:${a||i||g||t}:${b(n||_||t)}`}`,source:e,index:t,showId:d(s,["show_id","sid","app_show_id","app_show_idv2"]),showDate:d(s,["show_date","dt","schedule_show_datev2","scheduled_date"]),ringNumber:l,ringName:r,ringLabel:r||(l?`Ring ${l}`:"unknown ring"),ringKey:v,groupId:d(s,["class_group_id","group_id"]),groupLabel:p,groupKey:b(d(s,["class_group_id","group_id"])||`${v}:${p}`),classGroupId:a,classId:i,classNumber:g,className:o,classLabel:w,classKey:b(i||a||`${v}:${w}`),classTextKey:b($([g,o,p]).join(" ")),horse:n,horseKey:b(n),rider:_,riderKey:b(_),entryNumber:d(s,["entryNumber","entry_number","backNumber","back_number"]),latestGO:h,oog:d(s,["runningOOG","lastOOG","oog","rs_order_of_go"]),placing:d(s,["latestPlacing","lastPlace","placing","place"]),score:d(s,["lastScore","score","score1"]),latestStatus:m,completedTrips:S(d(s,["completed_trips","completedTrips"])),remainingTrips:S(d(s,["remaining_trips","remainingTrips"])),secondsTill:S(d(s,["secondsTill","rs_min_till_go"])),isFollowed:ce(d(s,["is_target","followed","is_followed","active"]))||!!(n||_||f),matchingScheduleRow:null};return T.statusBucket=z(m,T),T}function fe(e,t,s){t&&(e.has(t)||e.set(t,[]),e.get(t).push(s))}function me(e,t){const s=new Map,l=new Map,r=new Map,a=new Map,i=new Map;for(const n of e)s.has(n.ringKey)||s.set(n.ringKey,{id:`ring:${n.ringKey}`,key:n.ringKey,label:n.ringLabel,ringNumber:n.ringNumber,scheduleRows:[],tripRows:[]}),s.get(n.ringKey).scheduleRows.push(n),l.has(n.groupKey)||l.set(n.groupKey,{id:`group:${n.groupKey}`,key:n.groupKey,label:n.groupLabel,ringLabel:n.ringLabel,scheduleRows:[],tripRows:[]}),l.get(n.groupKey).scheduleRows.push(n),r.has(n.classKey)||r.set(n.classKey,{id:`class:${n.classKey}`,key:n.classKey,label:n.classLabel,ringLabel:n.ringLabel,scheduleRows:[],tripRows:[],totalTrips:0}),r.get(n.classKey).scheduleRows.push(n),r.get(n.classKey).totalTrips+=n.totalTrips||0;for(const n of t)s.has(n.ringKey)&&s.get(n.ringKey).tripRows.push(n),l.has(n.groupKey)&&l.get(n.groupKey).tripRows.push(n),r.has(n.classKey)&&r.get(n.classKey).tripRows.push(n),n.horseKey&&!a.has(n.horseKey)&&a.set(n.horseKey,{id:`horse:${n.horseKey}`,key:n.horseKey,label:n.horse,tripRows:[]}),n.horseKey&&a.get(n.horseKey).tripRows.push(n),n.riderKey&&!i.has(n.riderKey)&&i.set(n.riderKey,{id:`rider:${n.riderKey}`,key:n.riderKey,label:n.rider,tripRows:[]}),n.riderKey&&i.get(n.riderKey).tripRows.push(n);const g=[...s.values()].sort(C),o=[...l.values()].sort(C),p=[...r.values()].sort(C),w=[...a.values()].sort(C),v=[...i.values()].sort(C);for(const n of g)n.matchingTripCount=n.tripRows.length,n.followedTripCount=n.tripRows.filter(_=>_.isFollowed).length;for(const n of[...o,...p])n.matchingTripCount=n.tripRows.length,n.followedTripCount=n.tripRows.filter(_=>_.isFollowed).length;return{rings:g,groups:o,classes:p,horses:w,riders:v}}function ye(e,t,s,l,r){const a=[],i=t.filter(o=>o.isFollowed),g=o=>{if(!o)return"unknown source";const p=String(o).split(";").filter(Boolean);return p.length>1?`${p.length} staged trip files`:p[0].split(/[\\/]/).filter(Boolean).slice(-2).join("/")||p[0]};a.push({id:"thread:data-refresh",type:"data-refresh",title:"Data refreshed",text:`Schedule ${g(r.scheduleSource)}; trips ${g(r.tripsSource)}.`,statusBucket:"unknown",relatedRows:[]});for(const o of s.filter(p=>p.scheduleRows.length).slice(0,8))a.push({id:`thread:ring:${o.key}`,type:"ring-summary",title:`${o.label} has ${L(o.scheduleRows.length,"schedule row")}`,text:`${L(o.followedTripCount,"followed trip")} found in this ring.`,statusBucket:o.tripRows.some(p=>p.statusBucket==="live")?"live":"upcoming",relatedRows:[...o.scheduleRows.slice(0,8),...o.tripRows.slice(0,8)]});for(const o of i.slice(0,10)){const p=k(o.horse);a.push({id:`thread:trip:${o.id}`,type:o.statusBucket==="completed"?"completed":"followed-trip",title:`${p} appears in ${k(o.classLabel)}`,text:`${k(o.ringLabel)}${o.latestGO?` at ${o.latestGO}`:""}.`,statusBucket:o.statusBucket,relatedRows:$([o.matchingScheduleRow,o])})}for(const o of l.filter(p=>p.totalTrips||p.followedTripCount).slice(0,8))a.push({id:`thread:class:${o.key}`,type:"class-summary",title:`${o.label} has ${L(o.totalTrips||o.matchingTripCount,"trip")}`,text:`${L(o.followedTripCount,"followed overlay")} tied to this class.`,statusBucket:o.followedTripCount?"upcoming":"unknown",relatedRows:[...o.scheduleRows.slice(0,4),...o.tripRows.slice(0,6)]});for(const o of e.filter(p=>p.matchingTripCount===0).slice(0,6))a.push({id:`thread:missing:${o.id}`,type:"missing-data",title:`${o.classLabel} has no trip overlay`,text:`${o.ringLabel} is present in the schedule map without followed trip detail.`,statusBucket:"unknown",relatedRows:[o]});return a}function be(e,t,s,l,r,a,i,g,o){const p=t.filter(f=>f.isFollowed),w=p.filter(f=>f.statusBucket==="upcoming"||f.statusBucket==="live"),v=p.filter(f=>f.statusBucket==="completed"),n=[...w].sort((f,h)=>{const m=f.secondsTill??Number.MAX_SAFE_INTEGER,T=h.secondsTill??Number.MAX_SAFE_INTEGER;return m-T})[0]||p[0]||null,_=[...s].sort((f,h)=>h.followedTripCount-f.followedTripCount||h.scheduleRows.length-f.scheduleRows.length)[0]||null;return{totalScheduleRows:e.length,totalTripRows:t.length,ringCount:s.length,groupCount:l.length,classCount:r.length,followedHorseCount:a.length,followedRiderCount:i.length,liveCount:e.filter(f=>f.statusBucket==="live").length,upcomingCount:e.filter(f=>f.statusBucket==="upcoming").length,completedCount:e.filter(f=>f.statusBucket==="completed").length,followedUpcomingCount:w.length,followedCompletedCount:v.length,lastGeneratedAt:o.lastGeneratedAt||null,lastFetchedAt:o.lastFetchedAt||null,nextFollowedTrip:n,mostActiveRing:_,navCounts:{summary:p.length,lite:w.length,full:e.length||l.length,threads:g.length}}}function Y(e=[],t=[],s={}){const l=e.map(ge),r=t.map(he),a=new Map;for(const h of l)for(const m of de(h))fe(a,m,h);for(const h of r){const m=pe(h).map(T=>a.get(T)?.[0]).find(Boolean)||null;h.matchingScheduleRow=m,m&&(h.ringKey=m.ringKey||h.ringKey,h.groupKey=m.groupKey||h.groupKey,h.classKey=m.classKey||h.classKey,m.matchingTrips.push(h))}for(const h of l)h.matchingTrips=ue(h.matchingTrips,m=>m.id),h.matchingTripCount=h.matchingTrips.length,h.hasFollowedTrips=h.matchingTrips.some(m=>m.isFollowed);const i=l.sort((h,m)=>C({...h,sortValue:h.ringNumber},{...m,sortValue:m.ringNumber})||C(h,m)),g=r.sort((h,m)=>(h.secondsTill??Number.MAX_SAFE_INTEGER)-(m.secondsTill??Number.MAX_SAFE_INTEGER)),{rings:o,groups:p,classes:w,horses:v,riders:n}=me(i,g),_=ye(i,g,o,w,s),f=be(i,g,o,p,w,v,n,_,s);return{scheduleRows:i,tripRows:g,rings:o,groups:p,classes:w,horses:v,riders:n,threads:_,summaryStats:f}}function we(e){return $(e).map(t=>`<span class="row-tag">${c(t)}</span>`).join("")}function y(e,t){return t==null||String(t).trim()===""?"":`
    <div class="schedule-detail-row">
      <span>${c(e)}</span>
      <strong>${c(t)}</strong>
    </div>
  `}function q(e){if(!e)return"";const t=String(e.id||"").startsWith("trip:"),s=k(t?e.horse:e.classLabel||e.label),l=$([e.ringLabel,e.classLabel&&t?e.classLabel:e.groupLabel,e.latestGO]).join(" | ");return`
    <div class="row schedule-related-row">
      <span class="row-title">${c(s)}</span>
      <span class="row-tag">${c(l||e.statusBucket||"related")}</span>
    </div>
  `}function _e(e){const t=e.payload||{};return e.type==="aggregate-detail"?t.label||"Aggregate":e.type==="ring-detail"?t.label||t.ringLabel||"Ring":e.type==="group-detail"?t.label||t.groupLabel||"Group":e.type==="class-detail"?t.classLabel||t.label||"Class":e.type==="trip-detail"?t.horse||"Trip":e.type==="horse-detail"?t.label||"Horse":e.type==="rider-detail"?t.label||"Rider":e.type==="thread-detail"?t.title||"Thread":"Detail"}function ve(e){const t=e.payload||{};return e.type==="aggregate-detail"?t.hint||`${t.rows?.length||0} related rows`:e.type==="thread-detail"?t.text||t.type||"":$([t.ringLabel||t.label,t.groupLabel,t.classLabel,t.rider]).join(" | ")}function $e(e){const t=e.payload||{};return e.type==="aggregate-detail"?[y("Value",t.value),y("Rows",t.rows?.length??e.relatedRows?.length),y("Source",e.source)].join(""):e.type==="trip-detail"?[y("Horse",t.horse),y("Rider",t.rider),y("Entry",t.entryNumber),y("Ring",t.ringLabel),y("Class",t.classLabel),y("GO",t.latestGO),y("OOG",t.oog),y("Placing",t.placing),y("Score",t.score),y("Status",t.statusBucket)].join(""):e.type==="thread-detail"?[y("Type",t.type),y("Status",t.statusBucket),y("Related",e.relatedRows?.length||0)].join(""):[y("Ring",t.ringLabel||t.label),y("Group",t.groupLabel),y("Class",t.classLabel),y("Time",t.startDisplay||t.estimatedStart),y("Status",t.statusBucket),y("Total trips",t.totalTrips),y("Followed trips",t.matchingTripCount||t.followedTripCount),y("Completed",t.completedTrips),y("Remaining",t.remainingTrips)].join("")}function Re(e){if(!e)return"";const t=e.payload||{},s=e.relatedRows||t.rows||[];return`
    <div class="schedule-flyup-layer" data-action="close-detail">
      <section class="schedule-flyup" role="dialog" aria-modal="true" aria-labelledby="schedule-flyup-title" data-stop-close>
        <div class="schedule-flyup__handle" aria-hidden="true"></div>
        <div class="schedule-flyup__head">
          <div>
            <h2 id="schedule-flyup-title">${c(_e(e))}</h2>
            <p>${c(ve(e))}</p>
          </div>
          <button class="header-back schedule-flyup__close" type="button" data-action="close-detail">Close</button>
        </div>
        <div class="schedule-card__chips">
          ${we([e.type,t.statusBucket,t.type,e.source])}
        </div>
        <div class="schedule-detail-rows">
          ${$e(e)}
        </div>
        <div class="section-title">Related</div>
        <div class="schedule-related-list">
          ${s.length?s.slice(0,12).map(q).join(""):q({label:"No related rows",statusBucket:"empty"})}
        </div>
      </section>
    </div>
  `}const Se=[{screen:"start",label:"Start"},{screen:"summary",label:"Summary"},{screen:"lite",label:"Lite"},{screen:"full",label:"Full"},{screen:"threads",label:"Threads"}];function Te(e,t={}){return`<div class="nav-strip">${Se.map(l=>{const r=e===l.screen,a=l.screen==="start"?"":` ${t[l.screen]??0}`;return`
      <button class="nav-btn ${r?"is-active":""}" type="button" data-screen="${c(l.screen)}" aria-pressed="${r?"true":"false"}">
        ${c(l.label)}${c(a)}
      </button>
    `}).join("")}</div>`}function N(e,t="empty"){return`
    <div class="schedule-card schedule-card--${c(t)}" data-card-role="${c(t)}-card">
      <div class="schedule-card__title">${c(e)}</div>
    </div>
  `}function ke(){return N("Loading schedule data...","loading")}function Ce(e){return N(e||"Could not refresh data. Showing last known data if available.","error")}function B(e){return $(e).map(t=>`<span class="row-tag">${c(t)}</span>`).join("")}function K(e,t,s="card"){return`data-detail-type="${c(e)}" data-detail-id="${c(t)}" data-detail-source="${c(s)}"`}function Le({id:e,label:t,value:s,hint:l,rows:r=[]}){return`
    <button class="schedule-card schedule-card--metric" type="button" data-card-role="summary-card" ${K("aggregate-detail",e,"summary-aggregate")}>
      <span class="schedule-card__kicker">${c(t)}</span>
      <strong class="schedule-card__metric">${c(s)}</strong>
      <span class="schedule-card__hint">${c(l||L(r.length,"row"))}</span>
    </button>
  `}function Q({title:e,subtitle:t,chips:s=[],detailType:l,detailId:r,source:a,role:i="summary-card"}){return`
    <button class="schedule-card schedule-card--action" type="button" data-card-role="${c(i)}" ${K(l,r,a)}>
      <span class="schedule-card__title">${c(e)}</span>
      <span class="schedule-card__subtitle">${c(t)}</span>
      <span class="schedule-card__chips">${B(s)}</span>
    </button>
  `}function Z(e,t="ring-card"){const s=e.scheduleRows.filter(l=>l.statusBucket==="live").length;return`
    <button class="schedule-card schedule-card--ring" type="button" data-card-role="ring-card" ${K("ring-detail",e.id,t)}>
      <span class="schedule-card__kicker">${c(e.label)}</span>
      <span class="schedule-card__title">${c(L(e.scheduleRows.length,"schedule row"))}</span>
      <span class="schedule-card__subtitle">${c(L(e.followedTripCount,"followed trip"))}</span>
      <span class="schedule-card__chips">${B([s?`${s} live`:null,e.followedTripCount?"overlay":"map only"])}</span>
    </button>
  `}function ee(e,t="schedule-card"){return`
    <button class="schedule-card schedule-card--schedule ${e.hasFollowedTrips?"is-active":""}" type="button" data-card-role="class-card" ${K("class-detail",e.id,t)}>
      <span class="schedule-card__kicker">${c(e.ringLabel)}</span>
      <span class="schedule-card__title">${c(k(e.classLabel))}</span>
      <span class="schedule-card__subtitle">${c($([e.startDisplay||e.estimatedStart,e.groupLabel]).join(" | ")||"time unknown")}</span>
      <span class="schedule-card__chips">
        ${B([e.statusBucket,e.totalTrips!==null?`${e.totalTrips} trips`:null,e.matchingTripCount?`${e.matchingTripCount} followed`:"no overlay",e.remainingTrips!==null?`${e.remainingTrips} remaining`:null])}
      </span>
    </button>
  `}function te(e,t="trip-card"){return`
    <button class="schedule-card schedule-card--trip" type="button" data-card-role="trip-card" ${K("trip-detail",e.id,t)}>
      <span class="schedule-card__kicker">${c(k(e.ringLabel))}</span>
      <span class="schedule-card__title">${c(k(e.horse))}</span>
      <span class="schedule-card__subtitle">${c($([e.rider,e.classLabel]).join(" | ")||"trip detail")}</span>
      <span class="schedule-card__chips">
        ${B([e.statusBucket,e.latestGO?`GO ${e.latestGO}`:null,e.oog?`OOG ${e.oog}`:null,e.placing?`Place ${e.placing}`:null,e.score?`Score ${e.score}`:null])}
      </span>
    </button>
  `}function se(e,t="thread-card"){return`
    <button class="schedule-card schedule-card--thread" type="button" data-card-role="thread-card" ${K("thread-detail",e.id,t)}>
      <span class="schedule-card__kicker">${c(e.type)}</span>
      <span class="schedule-card__title">${c(e.title)}</span>
      <span class="schedule-card__subtitle">${c(e.text)}</span>
      <span class="schedule-card__chips">${B([e.statusBucket,e.relatedRows?.length?`${e.relatedRows.length} related`:null])}</span>
    </button>
  `}function A(e,t,s,l){const r=l===t;return`
    <button class="chip ${r?"is-active":""}" type="button" data-toggle="${c(e)}" data-value="${c(t)}" aria-pressed="${r?"true":"false"}">
      ${c(s)}
    </button>
  `}function j(e){return`
    <div class="schedule-toggle-row" aria-label="Global filters">
      <div class="chip-strip schedule-chip-strip">
        ${A("scope","active","ACTIVE",e.global.scope)}
        ${A("scope","full","FULL",e.global.scope)}
      </div>
      <div class="chip-strip schedule-chip-strip">
        ${A("status","live","LIVE",e.global.status)}
        ${A("status","all","ALL",e.global.status)}
      </div>
    </div>
  `}function O(e,t,s){return`
    <input
      class="schedule-search"
      type="search"
      value="${c(t||"")}"
      placeholder="${c(s)}"
      data-search-screen="${c(e)}"
      autocomplete="off"
    />
  `}function x(e,t,s){return`<div class="chip-strip schedule-peak-filters">${s.map(r=>`
    <button class="chip ${t===r.value?"is-active":""}" type="button" data-filter-screen="${c(e)}" data-filter-value="${c(r.value)}" aria-pressed="${t===r.value?"true":"false"}">
      ${c(r.label)}
    </button>
  `).join("")}</div>`}function Ke(e,t){const s=t.filters.global,l=t.filters.full,r=b(t.search.full);return e.filter(a=>s.scope==="full"||a.statusBucket!=="completed"||a.hasFollowedTrips).filter(a=>s.status==="all"||a.statusBucket==="live").filter(a=>l==="all"?!0:l.startsWith("ring:")?a.ringKey===l.slice(5):l.startsWith("group:")?a.groupKey===l.slice(6):!0).filter(a=>r?b([a.ringLabel,a.groupLabel,a.classLabel,a.classNumber].join(" ")).includes(r):!0)}function Ne(e){const t=new Map;for(const s of e)t.has(s.ringKey)||t.set(s.ringKey,{label:s.ringLabel,rows:[]}),t.get(s.ringKey).rows.push(s);return[...t.values()]}function Be(e){const t=Ke(e.derived.scheduleRows,e),s=[{value:"all",label:"All"},...e.derived.rings.slice(0,10).map(r=>({value:`ring:${r.key}`,label:r.label}))],l=Ne(t);return`
    <div class="list-column schedule-screen schedule-screen--full">
      ${j(e.filters)}
      ${O("full",e.search.full,"Search ring, group, class")}
      ${x("full",e.filters.full,s)}
      ${l.length?l.map(r=>`
        <section class="schedule-group-block">
          <div class="section-title">${c(r.label)}</div>
          ${r.rows.map(a=>ee(a,"full")).join("")}
        </section>
      `).join(""):N("No schedule rows found.")}
    </div>
  `}function Ae(e,t){const s=t.filters.global,l=t.filters.lite,r=b(t.search.lite);return e.filter(a=>a.isFollowed).filter(a=>s.scope==="full"||a.statusBucket!=="completed").filter(a=>s.status==="all"||a.statusBucket==="live").filter(a=>l==="all"||a.statusBucket===l).filter(a=>r?b([a.horse,a.rider,a.classLabel,a.ringLabel].join(" ")).includes(r):!0)}function Fe(e){const t=Ae(e.derived.tripRows,e),s=[{value:"all",label:"All"},{value:"upcoming",label:"Upcoming"},{value:"live",label:"Live"},{value:"completed",label:"Completed"},{value:"unknown",label:"Unknown"}];return`
    <div class="list-column schedule-screen schedule-screen--lite">
      ${j(e.filters)}
      ${O("lite",e.search.lite,"Search horse, rider, class, ring")}
      ${x("lite",e.filters.lite,s)}
      <div class="schedule-card-stack">
        ${t.length?t.map(l=>te(l,"lite")).join(""):N("No followed trips found for the current filters.")}
      </div>
    </div>
  `}function Ie({meta:e,derived:t}){const s=t?.summaryStats,l=t?.scheduleRows?.[0]?.showLabel||t?.scheduleRows?.[0]?.showDate||"schedule pending",r=s?.totalScheduleRows?"Ready":"Waiting";return`
    <div class="list-column schedule-screen schedule-screen--start">
      <button class="row row--tap row--active" type="button" data-action="start-session">
        <span class="row-title">Start Session</span>
        <span class="row-tag row-tag--boolean row-tag--positive">${c(r)}</span>
      </button>
      <div class="row">
        <span class="row-title">${c(l)}</span>
        <span class="row-tag">${c(s?.totalScheduleRows?`${s.totalScheduleRows} rows`:"0 rows")}</span>
      </div>
      <div class="row">
        <span class="row-title">Last updated</span>
        <span class="row-tag">${c(e.lastFetchedAt?new Date(e.lastFetchedAt).toLocaleTimeString([],{hour:"numeric",minute:"2-digit"}):"unknown")}</span>
      </div>
    </div>
  `}function Ee(e){const t=e.derived.scheduleRows[0]||{},s=e.derived.summaryStats,l=e.meta.lastFetchedAt?new Date(e.meta.lastFetchedAt).toLocaleTimeString([],{hour:"numeric",minute:"2-digit"}):"unknown";return`
    <div class="schedule-readiness">
      <span>${c(t.showLabel||t.showDate||"Daily schedule")}</span>
      <strong>${c(l)}</strong>
      <span>${c(s.totalScheduleRows?"data ready":"empty")}</span>
    </div>
  `}function Ge(e){const t=e.derived.summaryStats,s=t.nextFollowedTrip;return s?te(s,"summary-now"):t.mostActiveRing?Z(t.mostActiveRing,"summary-now"):Q({title:t.totalScheduleRows?"Schedule loaded":"No schedule rows found",subtitle:t.totalScheduleRows?`${t.totalScheduleRows} schedule rows are ready.`:"The staged schedule source returned no rows.",chips:$([t.totalTripRows?`${t.totalTripRows} active trips`:null,t.ringCount?`${t.ringCount} rings`:null]),detailType:"aggregate-detail",detailId:"agg:scheduleRows",source:"summary-now"})}function je(e){const t=e.summaryStats;return`<div class="schedule-aggregate-grid">${[{id:"agg:scheduleRows",label:"Schedule rows",value:t.totalScheduleRows,hint:"full map",rows:e.scheduleRows},{id:"agg:tripRows",label:"Active trips",value:t.totalTripRows,hint:"overlay",rows:e.tripRows},{id:"agg:rings",label:"Rings",value:t.ringCount,hint:"rings today",rows:e.rings},{id:"agg:live",label:"Live",value:t.liveCount,hint:"current rows",rows:e.scheduleRows.filter(l=>l.statusBucket==="live")},{id:"agg:upcoming",label:"Upcoming",value:t.upcomingCount,hint:"not complete",rows:e.scheduleRows.filter(l=>l.statusBucket==="upcoming")},{id:"agg:completed",label:"Completed",value:t.completedCount,hint:"done rows",rows:e.scheduleRows.filter(l=>l.statusBucket==="completed")}].map(Le).join("")}</div>`}function Oe(e){const t=e.derived,s=[...t.rings].sort((i,g)=>g.followedTripCount-i.followedTripCount||g.scheduleRows.length-i.scheduleRows.length).slice(0,3),l=t.scheduleRows.filter(i=>i.hasFollowedTrips).slice(0,3),r=[...s.map(i=>Z(i,"summary-ring")),...l.map(i=>ee(i,"summary-class"))].slice(0,5),a=t.threads.slice(0,3);return`
    <div class="list-column schedule-screen schedule-screen--summary">
      ${e.error?Ce(e.error):""}
      ${Ee(e)}
      <div class="section-title">What matters now</div>
      ${Ge(e)}
      ${je(t)}
      <div class="section-title">Relevant rings and classes</div>
      ${r.length?r.join(""):Q({title:"Schedule loaded",subtitle:`${k(t.scheduleRows[0]?.showLabel)} has ${t.scheduleRows.length} rows.`,chips:["map ready"],detailType:"aggregate-detail",detailId:"agg:scheduleRows",source:"summary-empty-relevance"})}
      <div class="section-title">Recent threads</div>
      ${a.map(i=>se(i,"summary-thread")).join("")}
    </div>
  `}const xe=[{value:"all",label:"All"},{value:"followed-trip",label:"Followed"},{value:"ring-summary",label:"Rings"},{value:"class-summary",label:"Classes"},{value:"data-refresh",label:"Refresh"},{value:"missing-data",label:"Missing"},{value:"completed",label:"Completed"},{value:"upcoming",label:"Upcoming"}];function Me(e,t){const s=b(t.search.threads),l=t.filters.threads,r=t.filters.global;return e.filter(a=>l==="all"||a.type===l||a.statusBucket===l).filter(a=>r.status==="all"||a.statusBucket==="live").filter(a=>r.scope==="full"||a.type!=="missing-data").filter(a=>{if(!s)return!0;const i=(a.relatedRows||[]).map(g=>[g.horse,g.rider,g.ringLabel,g.classLabel,g.statusBucket].join(" ")).join(" ");return b(`${a.title} ${a.text} ${a.type} ${i}`).includes(s)})}function De(e){const t=Me(e.derived.threads,e);return`
    <div class="list-column schedule-screen schedule-screen--threads">
      ${j(e.filters)}
      ${O("threads",e.search.threads,"Search horse, rider, ring, class, status")}
      ${x("threads",e.filters.threads,xe)}
      <div class="schedule-card-stack">
        ${t.length?t.map(s=>se(s,"threads")).join(""):N("No thread items found for the current filters.")}
      </div>
    </div>
  `}const le=["start","summary","lite","full","threads"],re="ringstatus.schedule.prototype.state.v1",u={screen:"start",loaded:{schedule:!1,trips:!1},loading:!0,error:null,raw:{schedule:[],trips:[]},meta:{scheduleSource:null,tripsSource:null,lastFetchedAt:null,lastGeneratedAt:null,usedCache:!1},derived:null,filters:{global:{scope:"active",status:"all"},lite:"all",full:"all",threads:"all"},search:{lite:"",full:"",threads:""},detail:null};function Ue(){try{const e=JSON.parse(localStorage.getItem(re)||"{}");le.includes(e.screen)&&(u.screen=e.screen),u.filters={...u.filters,...e.filters||{},global:{...u.filters.global,...e.filters?.global||{}}},u.search={...u.search,...e.search||{}}}catch(e){console.warn("schedule_state_hydrate_failed",e)}}function F(){try{localStorage.setItem(re,JSON.stringify({screen:u.screen,filters:u.filters,search:u.search}))}catch(e){console.warn("schedule_state_persist_failed",e)}}function E(e){le.includes(e)&&(u.screen=e,u.detail=null,F())}function Pe(e,t){["scope","status"].includes(e)&&(u.filters.global[e]=t,F())}function Ve(e,t){["lite","full","threads"].includes(e)&&(u.filters[e]=t||"all",F())}function qe(e,t){["lite","full","threads"].includes(e)&&(u.search[e]=t||"",F())}function G(e){u.detail=e}const He={start:"RingStatus",summary:"Summary",lite:"Lite",full:"Full",threads:"Threads"};function ze(){if(u.loading||!u.derived)return`<div class="list-column">${ke()}</div>`;switch(u.screen){case"summary":return Oe(u);case"lite":return Fe(u);case"full":return Be(u);case"threads":return De(u);case"start":default:return Ie(u)}}function Je(e){const t=u.derived,s=t.summaryStats;return{"agg:scheduleRows":{label:"Schedule rows",value:s.totalScheduleRows,hint:"Full schedule scaffold",rows:t.scheduleRows},"agg:tripRows":{label:"Active trips",value:s.totalTripRows,hint:"Followed trip overlay",rows:t.tripRows},"agg:rings":{label:"Rings",value:s.ringCount,hint:"Rings in the map",rows:t.rings},"agg:live":{label:"Live",value:s.liveCount,hint:"Live schedule rows",rows:t.scheduleRows.filter(r=>r.statusBucket==="live")},"agg:upcoming":{label:"Upcoming",value:s.upcomingCount,hint:"Upcoming schedule rows",rows:t.scheduleRows.filter(r=>r.statusBucket==="upcoming")},"agg:completed":{label:"Completed",value:s.completedCount,hint:"Completed schedule rows",rows:t.scheduleRows.filter(r=>r.statusBucket==="completed")}}[e]||{label:"Aggregate",value:0,hint:"No matching rows",rows:[]}}function We(e,t,s){const l=u.derived;if(!l)return null;let r=null,a=[];return e==="aggregate-detail"?(r=Je(t),a=r.rows||[]):e==="ring-detail"?(r=l.rings.find(i=>i.id===t||i.key===t),a=r?[...r.scheduleRows,...r.tripRows]:[]):e==="group-detail"?(r=l.groups.find(i=>i.id===t||i.key===t),a=r?[...r.scheduleRows,...r.tripRows]:[]):e==="class-detail"?(r=l.scheduleRows.find(i=>i.id===t)||l.classes.find(i=>i.id===t||i.key===t),a=r?.scheduleRows?[...r.scheduleRows,...r.tripRows]:[r,...r?.matchingTrips||[]].filter(Boolean)):e==="trip-detail"?(r=l.tripRows.find(i=>i.id===t),a=[r?.matchingScheduleRow,r].filter(Boolean)):e==="horse-detail"?(r=l.horses.find(i=>i.id===t||i.key===t),a=r?.tripRows||[]):e==="rider-detail"?(r=l.riders.find(i=>i.id===t||i.key===t),a=r?.tripRows||[]):e==="thread-detail"&&(r=l.threads.find(i=>i.id===t),a=r?.relatedRows||[]),r?{type:e,id:t,source:s,payload:r,relatedRows:a,openedAt:new Date().toISOString()}:null}function R(){const e=document.getElementById("rs-app-main"),t=document.getElementById("rs-app-nav"),s=document.getElementById("rs-flyup-root"),l=document.getElementById("rs-header-title"),r=document.getElementById("rs-header-back"),a=document.getElementById("rs-header-action"),i=document.activeElement?.dataset?.searchScreen,g=document.activeElement?.selectionStart??null;if(!(!e||!t||!s||!l||!r||!a)&&(l.textContent=He[u.screen]||"RingStatus",r.classList.toggle("is-invisible",u.screen==="start"),a.classList.add("is-invisible"),e.innerHTML=ze(),t.innerHTML=Te(u.screen,u.derived?.summaryStats?.navCounts||{}),s.innerHTML=Re(u.detail),i)){const o=document.querySelector(`[data-search-screen="${i}"]`);o&&(o.focus(),g!==null&&o.setSelectionRange(g,g))}}function Xe(){document.addEventListener("click",e=>{const t=e.target,s=t.closest(".schedule-flyup-layer"),l=t.closest('[data-action="close-detail"]');if(l){if(s&&t.closest("[data-stop-close]")&&l===s)return;G(null),R();return}if(t.closest('[data-action="start-session"]')){E("summary"),R();return}if(t.closest("#rs-header-back")){E("start"),R();return}const i=t.closest("[data-screen]");if(i){E(i.dataset.screen),R();return}const g=t.closest("[data-toggle]");if(g){Pe(g.dataset.toggle,g.dataset.value),R();return}const o=t.closest("[data-filter-screen]");if(o){Ve(o.dataset.filterScreen,o.dataset.filterValue),R();return}const p=t.closest("[data-detail-type]");if(p){const w=We(p.dataset.detailType,p.dataset.detailId,p.dataset.detailSource);w&&(G(w),R())}}),document.addEventListener("input",e=>{const t=e.target.closest("[data-search-screen]");t&&(qe(t.dataset.searchScreen,t.value),R())}),document.addEventListener("keydown",e=>{e.key==="Escape"&&u.detail&&(G(null),R())})}async function Ye(){Ue(),Xe(),R();const e=await oe();u.raw.schedule=e.schedule||[],u.raw.trips=e.trips||[],u.loaded.schedule=u.raw.schedule.length>0,u.loaded.trips=u.raw.trips.length>0,u.meta={...u.meta,...e.meta||{}},u.error=e.error||null,u.derived=Y(u.raw.schedule,u.raw.trips,u.meta),u.loading=!1,R()}Ye().catch(e=>{console.error("schedule_app_boot_failed",e),u.loading=!1,u.error="Could not refresh data. Showing last known data if available.",u.derived=Y([],[],u.meta),R()});
