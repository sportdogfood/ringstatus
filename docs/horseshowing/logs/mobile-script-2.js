
  const POLL_MS = 6 * 60 * 1000;
  const PUBLISH_INTERVAL_MS = 8 * 60 * 1000;
  const EMPTY_RETRY_MS = 10 * 1000;
  const TIMEZONE = "America/New_York";
  const PDF_WORKER_BASE = "https://ringstatus-pdf.gombcg.workers.dev/";
  const WEC_PRINT_URL = "https://ringstatus.com/wec-print";
  const WEC_EDIT_URL = window.RS_WEC_EDIT_URL || "https://ringstatus.com/test/wec-schedule/edit";

  let activeHorse = "__all__";
  let activeStatus = "__all__";

  let activeIO = null;
  let lastRaw = null;
  let lastLastMod = null;
  let currentDataUrl = null;
  let isRefreshing = false;
  let publishedAtMs = null;
  let currentShowNo = "";
  let currentFocusDay = "";
  let hideMode = false;
  let emptyRetryTimer = null;
  const hideSelection = new Map();

  const fmtTimeET = (ms)=>{
    return new Intl.DateTimeFormat("en-US", {
      timeZone: TIMEZONE,
      hour: "numeric",
      minute: "2-digit"
    }).format(new Date(ms));
  };

  const esc = (s)=>String(s ?? "").replace(/[&<>"']/g, c => ({
    "&":"&amp;",
    "<":"&lt;",
    ">":"&gt;",
    '"':"&quot;",
    "'":"&#39;"
  }[c]));

  const toInt = (v)=>{
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  function isoDate(value){
    const raw = String(value ?? "").trim();
    const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
    return m ? `${m[1]}-${m[2]}-${m[3]}` : "";
  }

  function addDaysIso(value, days){
    const iso = isoDate(value);
    if(!iso) return "";
    const d = new Date(`${iso}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().slice(0,10);
  }

  function resolveDataUrl(){
    const base = window.RS_WEC_SCHEDULE_URL || "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?action=wec-mobile-live";
    const focusDay = new URLSearchParams(location.search).get("focus_day");
    if(!focusDay) return base;
    const u = new URL(base, location.href);
    u.searchParams.set("focus_day", focusDay);
    return u.toString();
  }

  function buildPrintPdfUrl(){
    const source = new URL(window.RS_WEC_PRINT_URL || WEC_PRINT_URL, location.href);
    const focusDay = new URLSearchParams(location.search).get("focus_day") || currentFocusDay || isoDate(document.getElementById("subtitle")?.textContent);
    if(focusDay) source.searchParams.set("focus_day", focusDay);
    source.searchParams.set("pdf", "1");
    const filenameDay = focusDay || "schedule";
    const pdf = new URL(PDF_WORKER_BASE);
    pdf.searchParams.set("url", source.toString());
    pdf.searchParams.set("filename", `wec-${filenameDay}-schedule.pdf`);
    pdf.searchParams.set("waitForSelector", 'html[data-rs-pdf-ready="1"]');
    return pdf.toString();
  }

  function syncPrintPdfLink(){
    const link = document.getElementById("printPdfLink");
    if(link) link.href = buildPrintPdfUrl();
  }

  function setHideMode(nextMode){
    hideMode = !!nextMode;
    if(!hideMode) hideSelection.clear();
    document.body.classList.toggle("is-hide-mode", hideMode);
    document.querySelectorAll(".hide-check").forEach(input => {
      input.checked = hideSelection.has(input.dataset.classNo || "");
      input.closest(".c")?.classList.toggle("is-hide-selected", input.checked);
    });
  }

  function updateHideSelection(input){
    const classNo = String(input?.dataset?.classNo || "").trim();
    if(!classNo) return;
    const label = String(input.dataset.classLabel || "").trim();
    if(input.checked) hideSelection.set(classNo, { class_no: classNo, class_label: label });
    else hideSelection.delete(classNo);
    input.closest(".c")?.classList.toggle("is-hide-selected", input.checked);
  }

  async function saveHideSelection(){
    if(!currentShowNo || hideSelection.size === 0) return;
    const save = document.getElementById("hideSaveBtn");
    if(save) save.disabled = true;
    try{
      await postWecEdit({
        action:"hide-classes",
        show_no:currentShowNo,
        focus_day:currentFocusDay,
        classes:Array.from(hideSelection.values()),
        source:"wec-mobile"
      });
      setHideMode(false);
      currentDataUrl = null;
      publishedAtMs = null;
      await initialLoad();
    } finally {
      if(save) save.disabled = false;
    }
  }

  function normRollup(s){
    const raw = String(s ?? "").trim();
    if(!raw) return "";
    const parts = raw.split(",").map(x=>x.trim()).filter(Boolean);
    const out = [];
    const seen = new Set();

    for(const p of parts){
      const k = p.toLowerCase();
      if(seen.has(k)) continue;
      seen.add(k);
      out.push(p);
    }

    return out.join(", ");
  }

  function extractHorseName(groupDisplayPart){
    const s = String(groupDisplayPart ?? "").trim();
    if(!s) return "";
    const idx = s.indexOf(" (");
    return (idx > 0 ? s.slice(0, idx) : s).trim();
  }

  function normalizeHorseToken(horse){
    if(horse && typeof horse === "object"){
      const label = String(horse.label || horse.display || horse.horse || "").trim();
      const rawHorse = String(horse.horse || extractHorseName(label) || label).trim();
      const display = String(horse.display || extractHorseName(label) || rawHorse).trim();
      const missing = horse.barn_name_missing === true || horse.barn_name_missing === "1" || horse.barn_name_missing === "true";
      return {
        label,
        horse: rawHorse,
        display,
        editable: !!(missing && rawHorse && display && rawHorse.toLowerCase() === display.toLowerCase())
      };
    }
    const label = String(horse || "").trim();
    return {
      label,
      horse: extractHorseName(label),
      display: extractHorseName(label) || label,
      editable: false
    };
  }

  function horseTokenHtml(horse){
    const token = normalizeHorseToken(horse);
    if(!token.label) return "";
    const edit = token.editable ? `
      <button class="horse-edit" type="button" data-horse="${esc(token.horse)}" aria-label="Edit barn name for ${esc(token.horse)}">edit</button>
    ` : "";
    return `<span class="horse-token">${esc(token.label)}${edit}</span>`;
  }

  function dedupeGroupDisplayByHorse(s){
    const raw = String(s ?? "").trim();
    if(!raw) return "";

    const parts = raw.split(",").map(x=>x.trim()).filter(Boolean);
    const out = [];
    const seen = new Set();

    for(const p of parts){
      const horse = extractHorseName(p);
      const key = (horse || p).trim().toLowerCase();
      if(!key || seen.has(key)) continue;
      seen.add(key);
      out.push(p);
    }

    return out.join(", ");
  }

  function trainerRollupsForItems(items){
    const byTrainer = new Map();
    for(const row of items){
      const raw = row && row.trainer_rollups;
      const rollups = Array.isArray(raw) ? raw : [];
      for(const item of rollups){
        const trainer = String(item.trainer_display || item.trainer || "").trim();
        const horses = Array.isArray(item.horses) ? item.horses : [];
        if(!trainer || !horses.length) continue;
        const bucket = byTrainer.get(trainer) || [];
        for(const horse of horses){
          const token = normalizeHorseToken(horse);
          if(token.label) bucket.push(horse && typeof horse === "object" ? horse : token.label);
        }
        byTrainer.set(trainer, bucket);
      }
    }
    return Array.from(byTrainer.entries()).map(([trainer, horses]) => ({
      trainer,
      horses: Array.from(new Set(horses))
    })).filter(item => item.horses.length);
  }

  function trainerRollupHtml(rollups, fallback){
    if(rollups && rollups.length){
      return rollups.map(item => `
        <div class="gdisplay-line">
          <span class="gdisplay-badge">${esc(item.trainer)}</span>
          <span class="gdisplay-text">${(item.horses || []).map(horseTokenHtml).filter(Boolean).join(", ")}</span>
        </div>
      `).join("");
    }
    return fallback ? `<div class="gdisplay-line"><span class="gdisplay-text">${esc(fallback)}</span></div>` : "";
  }

  function normalizeStatusToken(s){
    const v = String(s ?? "").trim().toLowerCase();
    if(v === "upcoming") return "upcoming";
    if(v === "underway") return "underway";
    if(v === "completed") return "completed";
    return "";
  }

  function computeGroupStatus(items){
    let hasUnderway = false;
    let hasUpcoming = false;
    let hasCompleted = false;
    let sawAnyStatus = false;
    let sawNonCompleted = false;

    for(const c of items){
      const s = String(c.status2 ?? "").trim();
      if(!s) continue;
      sawAnyStatus = true;
      if(s === "Underway") hasUnderway = true;
      else if(s === "Upcoming") hasUpcoming = true;
      else if(s === "Completed") hasCompleted = true;
      else sawNonCompleted = true;

      if(s !== "Completed") sawNonCompleted = true;
    }

    if(hasUnderway) return "Underway";
    if(hasUpcoming) return "Upcoming";
    if(sawAnyStatus && hasCompleted && !sawNonCompleted) return "Completed";
    return "";
  }

  async function fetchTextMeta(url){
    const res = await fetch(url, { cache:"no-store" });
    const txt = await res.text();
    if(!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}: ${txt.slice(0,200)}`);
    const lastMod = res.headers.get("last-modified");
    return { txt, lastMod };
  }

  function parseJsonText(txt, urlLabel){
    try{
      return normalizeScheduleRows(JSON.parse(txt));
    }catch(e){
      throw new Error(`Bad JSON in ${urlLabel}: ${String(txt).slice(0,200)}`);
    }
  }

  function rollupGroupDisplay(rollups){
    const out = [];
    for(const rollup of rollups || []){
      for(const horse of rollup.horses || []){
        const token = normalizeHorseToken(horse);
        if(token.label) out.push(token.label);
      }
    }
    return Array.from(new Set(out)).join(", ");
  }

  function normalizeScheduleRows(payload){
    if(Array.isArray(payload)) return payload;
    if(!payload || !Array.isArray(payload.rings)) return [];

    const rows = [];
    for(const ring of payload.rings){
      for(const c of ring.classes || []){
        const classNumber = String(c.class_number ?? "").trim();
        let className = String(c.class_name || c.class_label || "").trim();
        if(classNumber && className.startsWith(`${classNumber} - `)){
          className = className.slice(`${classNumber} - `.length).trim();
        }
        const rollups = Array.isArray(c.rollups) ? c.rollups : [];
        const groupDisplay = rollupGroupDisplay(rollups);
        rows.push({
          show_id: payload.show_no,
          show_days_report_title: payload.show_name,
          show_days_display_date: payload.show_focus_date,
          show_day_key: payload.show_focus_date,
          ring_number: ring.ring_no,
          ring_name: ring.ring_display,
          class_group_id: String(c.class_no || ""),
          class_group_sequence: c.class_start_time || c.class_time || "",
          group_group_name: className,
          class_no: c.class_no,
          class_number: classNumber,
          class_name: className,
          start_display: c.class_time,
          class_start_time: c.class_start_time,
          entry_count: c.entry_count,
          n_gone: c.n_gone,
          n_to_go: c.n_to_go,
          elapsed_seconds: c.elapsed_seconds,
          current_entry_no: c.current_entry_no,
          current_horse: c.current_horse,
          live_source: c.live_source,
          group_display: groupDisplay,
          sched_display: groupDisplay,
          "8778_sched_display": groupDisplay,
          trainer_rollups: rollups,
          diff_class: c.diff_class || ""
        });
      }
    }
    return rows;
  }

  function groupBy(arr, keyFn){
    const m = new Map();
    for(const it of arr){
      const k = keyFn(it);
      if(!m.has(k)) m.set(k, []);
      m.get(k).push(it);
    }
    return m;
  }

  function setHeader(rows){
    const first = rows[0] || {};

    const reportTitle =
      first.show_days_report_title ??
      first.show_report_title ??
      (first.show_id ? `Show ${first.show_id}` : "Schedule");

    const displayDate =
      first.show_days_display_date ??
      first.show_display_date ??
      first.show_date ??
      first.showDate ??
      "";

    const focusDay = isoDate(first.show_day_key ?? first.showDayKey ?? displayDate);
    const showEndDate = isoDate(first.show_end_date ?? first.showEndDate);
    currentShowNo = String(first.show_id || first.show_no || "").trim();
    currentFocusDay = focusDay;

    document.getElementById("title").textContent = reportTitle;

    document.getElementById("subtitle").textContent = displayDate ? String(displayDate) : "";
    syncDateEdit(focusDay, showEndDate);
  }

  function syncDateEdit(focusDay, showEndDate){
    const edit = document.getElementById("dateEditToggle");
    const panel = document.getElementById("dateEditPanel");
    const next = document.getElementById("dateNextBtn");
    if(!edit || !panel || !next) return;

    const nextDay = addDaysIso(focusDay, 1);
    const canShow = !!(focusDay && nextDay && nextDay > focusDay && (!showEndDate || nextDay <= showEndDate));

    edit.classList.toggle("is-visible", canShow);
    if(!canShow){
      panel.classList.remove("is-visible");
      next.textContent = "";
      return;
    }

    next.textContent = nextDay;
    next.dataset.focusDay = nextDay;
  }

  async function postWecEdit(payload){
    const res = await fetch(WEC_EDIT_URL, {
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body:JSON.stringify(payload)
    });
    const txt = await res.text();
    let data = null;
    try{ data = JSON.parse(txt); }catch{}
    if(!res.ok || data?.ok === false){
      throw new Error(data?.error || txt || `Edit failed ${res.status}`);
    }
    return data || {};
  }

  function bindDateEdit(){
    const edit = document.getElementById("dateEditToggle");
    const panel = document.getElementById("dateEditPanel");
    const next = document.getElementById("dateNextBtn");

    if(edit && edit.dataset.rsBound !== "1"){
      edit.dataset.rsBound = "1";
      edit.addEventListener("click", () => {
        panel?.classList.toggle("is-visible");
      });
    }

    if(next && next.dataset.rsBound !== "1"){
      next.dataset.rsBound = "1";
      next.addEventListener("click", async () => {
        const focusDay = next.dataset.focusDay;
        if(!focusDay) return;
        if(currentShowNo){
          await postWecEdit({
            action:"set-focus-day",
            show_no:currentShowNo,
            focus_day:focusDay,
            source:"wec-mobile"
          });
        }
        const u = new URL(location.href);
        u.searchParams.set("focus_day", focusDay);
        history.replaceState(null, "", u.toString());
        currentDataUrl = null;
        publishedAtMs = null;
        await initialLoad();
      });
    }
  }

  function bindHorseEdits(){
    document.addEventListener("click", async (event) => {
      const btn = event.target && event.target.closest ? event.target.closest(".horse-edit") : null;
      if(!btn) return;
      const horse = btn.dataset.horse || "";
      if(!horse || !currentShowNo) return;
      const barnName = window.prompt(`Barn name for ${horse}`, "");
      if(!barnName || !barnName.trim()) return;
      btn.disabled = true;
      try{
        await postWecEdit({
          action:"set-barn-name",
          show_no:currentShowNo,
          focus_day:currentFocusDay,
          horse,
          barn_name:barnName.trim(),
          source:"wec-mobile"
        });
        currentDataUrl = null;
        publishedAtMs = null;
        await initialLoad();
      } finally {
        btn.disabled = false;
      }
    }, { passive:false });
  }

  function bindHideMode(){
    const start = document.getElementById("hideModeBtn");
    const save = document.getElementById("hideSaveBtn");
    const cancel = document.getElementById("hideCancelBtn");

    if(start && start.dataset.rsBound !== "1"){
      start.dataset.rsBound = "1";
      start.addEventListener("click", () => setHideMode(true));
    }

    if(cancel && cancel.dataset.rsBound !== "1"){
      cancel.dataset.rsBound = "1";
      cancel.addEventListener("click", () => setHideMode(false));
    }

    if(save && save.dataset.rsBound !== "1"){
      save.dataset.rsBound = "1";
      save.addEventListener("click", () => saveHideSelection().catch(err => {
        window.alert(String(err && err.message ? err.message : err));
      }));
    }

    document.addEventListener("change", event => {
      const input = event.target && event.target.closest ? event.target.closest(".hide-check") : null;
      if(!input) return;
      updateHideSelection(input);
    });
  }

  function rowDisplayTime(row){
    const sd1 = String(row.start_display ?? row.startDisplay ?? "").trim();
    const sd2 = String(row.start_display2 ?? row.startDisplay2 ?? "").trim();
    return sd2 && (!sd1 || sd2 !== sd1) ? sd2 : (sd1 || sd2);
  }

  function isCheckDisplay(value){
    return String(value || "").trim().toLowerCase() === "check time";
  }

  function roundedTenMinuteTime(value){
    const raw = String(value || "").trim();
    if(!raw || isCheckDisplay(raw)) return { key: raw.toLowerCase(), display: raw };
    const m = raw.match(/^(\d{1,2})(?::?(\d{2}))?\s*([ap]m?)?$/i);
    if(!m) return { key: raw.toLowerCase(), display: raw };

    let hour = Number(m[1]);
    const minute = Number(m[2] || 0);
    const meridiemRaw = String(m[3] || "").toUpperCase();
    const meridiem = meridiemRaw.startsWith("P") ? "PM" : meridiemRaw.startsWith("A") ? "AM" : "";
    if(!Number.isFinite(hour) || !Number.isFinite(minute)) return { key: raw.toLowerCase(), display: raw };

    let total = hour * 60 + minute;
    if(meridiem === "PM" && hour < 12) total += 12 * 60;
    if(meridiem === "AM" && hour === 12) total -= 12 * 60;

    total = Math.round(total / 10) * 10;
    total = ((total % (24 * 60)) + (24 * 60)) % (24 * 60);

    let outHour24 = Math.floor(total / 60);
    const outMinute = total % 60;
    const outMeridiem = outHour24 >= 12 ? "PM" : "AM";
    let outHour = outHour24 % 12;
    if(outHour === 0) outHour = 12;
    const outDisplay = `${outHour}:${String(outMinute).padStart(2, "0")} ${outMeridiem}`;
    return { key: `${outHour24}:${String(outMinute).padStart(2, "0")}`, display: outDisplay };
  }

  function classRenderKey(row){
    const ringNo = String(row.ring_no ?? row.ringNo ?? row.ring_number ?? row.ringNumber ?? "").trim();
    const roundedTime = roundedTenMinuteTime(rowDisplayTime(row)).key;
    const name15 = normalizedDisplayClassText(row);
    return `display:${ringNo}|${roundedTime}|${name15}`;
  }

  function mergeClassRows(items){
    const merged = new Map();
    for(const row of items || []){
      const key = classRenderKey(row);
      if(!merged.has(key)){
        merged.set(key, Object.assign({}, row));
        continue;
      }
      const existing = merged.get(key);
      for(const field of ["start_display", "startDisplay", "start_display2", "startDisplay2", "class_name", "className", "name", "class_number", "classNumber"]){
        if(!String(existing[field] ?? "").trim() && String(row[field] ?? "").trim()){
          existing[field] = row[field];
        }
      }
    }
    return Array.from(merged.values());
  }

  function classDisplayLabel(row){
    const rawName = String(row.class_name ?? row.className ?? row.name ?? "").trim();
    const leadingLabel = rawName.match(/^([A-Za-z0-9]+)\)\s*(.+)$/);
    if(leadingLabel) return `${leadingLabel[1]} - ${leadingLabel[2]}`.trim();
    return rawName;
  }

  function normalizedDisplayClassText(row){
    return String(classDisplayLabel(row))
      .replace(/^[A-Za-z0-9]+\s*-\s*/, "")
      .replace(/\([^)]*\)/g, " ")
      .replace(/['’]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase()
      .slice(0, 15);
  }

  function buildRings(rows){
    const rings = Array.from(groupBy(rows, r => String(r.ring_number ?? r.ringNumber ?? "9999")).entries())
      .map(([k, items]) => ({ ring_number: toInt(k) ?? 9999, items }))
      .sort((a,b)=>a.ring_number - b.ring_number);

    const ringEls = [];

    for(let i=0;i<rings.length;i++){
      const ring = rings[i];
      const items0 = ring.items[0] || {};
      const ringTitle = items0.ring_name ?? items0.ringName ?? items0.ring_nickname ?? items0.ringNickname ?? "Ring";

      const ringId = (Number.isFinite(ring.ring_number) && ring.ring_number !== 9999)
        ? `ring-${ring.ring_number}`
        : `ring-x${i+1}`;

      const groups = Array.from(groupBy(ring.items, r => classRenderKey(r)).entries())
        .map(([gid, items]) => {
          const seq = toInt(items[0]?.class_group_sequence ?? items[0]?.classGroupSequence ?? 9999) ?? 9999;
          const gname = items[0]?.group_group_name ?? items[0]?.group_name ?? items[0]?.groupName ?? "Group";
          const gdRaw = normRollup(items.map(x => x.group_display).filter(Boolean).join(", "));
          const gd = dedupeGroupDisplayByHorse(gdRaw);
          const trainerRollups = trainerRollupsForItems(items);
          const gstatus = computeGroupStatus(items);
          const checkRank = items.every(x => isCheckDisplay(rowDisplayTime(x))) ? 1 : 0;
          return { gid, seq, gname, gdisplay: gd, trainerRollups, gstatus, checkRank, items };
        })
        .sort((a,b)=>(a.checkRank - b.checkRank) || (a.seq - b.seq));

      const sec = document.createElement("section");
      sec.className = "ring";
      sec.id = ringId;
      sec.dataset.ringTitle = String(ringTitle);

      sec.innerHTML = `<div class="ring-head">${esc(ringTitle)}</div>`;
      let lastRingTimeBucket = null;
      const seenNonTeamTimeClassKeys = new Set();

      for(const g of groups){
        const hasTeam = !!g.gdisplay || (g.trainerRollups && g.trainerRollups.length);
        const classes = mergeClassRows(g.items).sort((a,b)=>{
          const ac = isCheckDisplay(rowDisplayTime(a)) ? 1 : 0;
          const bc = isCheckDisplay(rowDisplayTime(b)) ? 1 : 0;
          if(ac !== bc) return ac - bc;
          const an = toInt(a.class_number ?? a.classNumber ?? 999999) ?? 999999;
          const bn = toInt(b.class_number ?? b.classNumber ?? 999999) ?? 999999;
          return an - bn;
        });

        let cl = "";
        for(const c of classes){
          const sd1 = String(c.start_display ?? c.startDisplay ?? "").trim();
          const sd2 = String(c.start_display2 ?? c.startDisplay2 ?? "").trim();
          const usingTime2 = !!(sd2 && (!sd1 || sd2 !== sd1));
          const rawDisplayTime = rowDisplayTime(c);
          const roundedTime = roundedTenMinuteTime(rawDisplayTime);
          const displayTime = roundedTime.display;
          const isCheckTime = isCheckDisplay(displayTime);

          const label = classDisplayLabel(c);
          const classNo = String(c.class_no ?? c.classNo ?? "").trim();
          const duplicateKey = `${roundedTime.key}|${normalizedDisplayClassText(c)}`;
          if(duplicateKey.trim() && seenNonTeamTimeClassKeys.has(duplicateKey)) continue;
          if(duplicateKey.trim()) seenNonTeamTimeClassKeys.add(duplicateKey);

          const showTime = (displayTime && (hasTeam || roundedTime.key !== lastRingTimeBucket));
          if(showTime) lastRingTimeBucket = roundedTime.key;

          const timeHtml = showTime
            ? (isCheckTime
              ? `<span class="check-time">check</span>`
              : esc(displayTime))
            : "";
          const diffClass = String(c.diff_class ?? c.diffClass ?? "").split(/\s+/).filter(x => /^diff-[a-z0-9_-]+$/i.test(x)).join(" ");

          cl += `
            <div class="c${usingTime2 ? " time2" : ""}${isCheckTime ? " check-time-row" : ""}${diffClass ? ` ${diffClass}` : ""}" data-class-no="${esc(classNo)}" data-class-label="${esc(label)}">
              <div class="t">${timeHtml}</div>
              <div class="n">${esc(label)}</div>
              <div class="hide-cell"><input class="hide-check" type="checkbox" data-class-no="${esc(classNo)}" data-class-label="${esc(label)}" aria-label="Hide class ${esc(label)}"></div>
            </div>
          `;
        }

        const horses = [];
        if(g.trainerRollups && g.trainerRollups.length){
          for(const rollup of g.trainerRollups){
            for(const horse of rollup.horses || []){
              const hn = normalizeHorseToken(horse).display;
              if(hn) horses.push(hn);
            }
          }
        } else if(g.gdisplay){
          const parts = String(g.gdisplay).split(",").map(x=>x.trim()).filter(Boolean);
          for(const p of parts){
            const hn = extractHorseName(p);
            if(hn) horses.push(hn);
          }
        }

        const horsesNorm = Array.from(new Set(horses.map(h => h.toLowerCase()))).join("|");
        const gDiv = document.createElement("div");

        gDiv.className = `group${hasTeam ? " has-team" : ""}`;
        if(hasTeam){
          if(horsesNorm) gDiv.dataset.horses = horsesNorm;
        }

        gDiv.innerHTML = `
          <div class="group-head">
            ${hasTeam ? `<div class="gdisplay">${trainerRollupHtml(g.trainerRollups, g.gdisplay)}</div>` : ``}
            <div class="gname">${esc(g.gname)}</div>
          </div>
          <div class="class-list">${cl || `<div class="muted">No classes scheduled.</div>`}</div>
        `;

        sec.appendChild(gDiv);
      }

      ringEls.push(sec);
    }

    return ringEls;
  }

  function setActiveRingButton(id){
    const buttons = Array.from(document.querySelectorAll("#ringNav .nav-btn"));
    for(const b of buttons){
      b.classList.toggle("active", b.dataset.target === id);
    }
  }

  function buildBottomNav(ringEls){
    const nav = document.getElementById("ringNav");
    nav.innerHTML = "";

    for(const sec of ringEls){
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "nav-btn";
      btn.textContent = sec.dataset.ringTitle || "Ring";
      btn.dataset.target = sec.id;

      btn.addEventListener("click", ()=>{
        const target = document.getElementById(sec.id);
        if(!target) return;

        setActiveRingButton(sec.id);
        target.scrollIntoView({ behavior:"smooth", block:"start" });
      });

      nav.appendChild(btn);
    }
  }

  function destroyActiveRingObserver(){
    if(activeIO){
      try{ activeIO.disconnect(); }catch(e){}
      activeIO = null;
    }
  }

  function setupActiveRingObserver(){
    destroyActiveRingObserver();

    const sections = Array.from(document.querySelectorAll("section.ring"))
      .filter(sec => sec.style.display !== "none");

    activeIO = new IntersectionObserver((entries)=>{
      const vis = entries
        .filter(e => e.isIntersecting)
        .sort((a,b) => b.intersectionRatio - a.intersectionRatio);

      if(!vis.length) return;
      setActiveRingButton(vis[0].target.id);
    }, {
      root:null,
      rootMargin:`-${(56 + 74 + 10)}px 0px -80px 0px`,
      threshold:[0.2, 0.35, 0.5, 0.65]
    });

    sections.forEach(sec => activeIO.observe(sec));
  }

  function collectFilterCandidatesFromDOM(){
    const horseSet = new Set();

    const groups = Array.from(document.querySelectorAll(".group.has-team"));
    for(const g of groups){
      const horses = String(g.dataset.horses ?? "").trim();
      if(horses){
        horses.split("|").map(x=>x.trim()).filter(Boolean).forEach(h => horseSet.add(h));
      }
    }

    const horsesList = Array.from(horseSet)
      .map(h => ({ key:h, label:h }))
      .sort((a,b) => a.label.localeCompare(b.label));

    return { horsesList };
  }

  function prettyHorseLabel(h){
    const s = String(h || "").trim();
    if(!s) return "";
    return s.charAt(0).toUpperCase() + s.slice(1);
  }

  function makeFilterButton({ text, value, active, onClick }){
    const b = document.createElement("button");
    b.type = "button";
    b.className = "nav-btn" + (active ? " active" : "");
    b.textContent = text;
    b.dataset.value = value;
    b.addEventListener("click", onClick);
    return b;
  }

  function updateFilterUI(){
    for(const b of Array.from(document.querySelectorAll("#horseFilters .nav-btn"))){
      b.classList.toggle("active", b.dataset.value === activeHorse);
    }
  }

  function applyFilters(){
    const groups = Array.from(document.querySelectorAll(".group"));

    const needHorse = activeHorse !== "__all__";

    const horseKey = String(activeHorse).toLowerCase();

    for(const g of groups){
      let ok = true;

      if(needHorse){
        const horses = String(g.dataset.horses ?? "");
        ok = ok && horses.split("|").map(x => x.trim()).includes(horseKey);
      }

      g.style.display = ok ? "" : "none";
    }

    const ringSections = Array.from(document.querySelectorAll("section.ring"));
    const ringNavBtns = Array.from(document.querySelectorAll("#ringNav .nav-btn"));

    const navById = new Map();
    for(const b of ringNavBtns){
      if(b.dataset.target) navById.set(b.dataset.target, b);
    }

    for(const sec of ringSections){
      const groupsInRing = Array.from(sec.querySelectorAll(".group"));
      const anyVisible = groupsInRing.some(g => g.style.display !== "none");

      sec.style.display = anyVisible ? "" : "none";

      const btn = navById.get(sec.id);
      if(btn) btn.style.display = anyVisible ? "" : "none";
    }

    setupActiveRingObserver();

    const firstVisibleRing = Array.from(document.querySelectorAll("section.ring"))
      .find(sec => sec.style.display !== "none");

    if(firstVisibleRing){
      setActiveRingButton(firstVisibleRing.id);
    }
  }

  function renderFilters(){
    const { horsesList } = collectFilterCandidatesFromDOM();

    const horseWrap = document.getElementById("horseFilters");

    horseWrap.innerHTML = "";

    for(const h of horsesList){
      horseWrap.appendChild(makeFilterButton({
        text: prettyHorseLabel(h.label),
        value: h.key,
        active: activeHorse === h.key,
        onClick: ()=>{
          activeHorse = (activeHorse === h.key) ? "__all__" : h.key;
          updateFilterUI();
          applyFilters();
        }
      }));
    }

    const horseExists = activeHorse === "__all__" || horsesList.some(x => x.key === activeHorse);
    if(!horseExists) activeHorse = "__all__";

    updateFilterUI();
    applyFilters();
  }

  function renderRows(rows){
    if(!Array.isArray(rows) || rows.length === 0){
      if(!document.querySelector("section.ring")){
        document.getElementById("rings").innerHTML = `<div class="muted">Updating schedule...</div>`;
      }
      scheduleEmptyRetry();
      return false;
    }

    if(emptyRetryTimer){
      clearTimeout(emptyRetryTimer);
      emptyRetryTimer = null;
    }

    setHeader(rows);

    const ringEls = buildRings(rows);

    const container = document.getElementById("rings");
    container.innerHTML = "";
    for(const el of ringEls) container.appendChild(el);

    buildBottomNav(ringEls);
    renderFilters();
    setupActiveRingObserver();

    const firstVisibleRing = Array.from(document.querySelectorAll("section.ring"))
      .find(sec => sec.style.display !== "none");

    if(firstVisibleRing){
      setActiveRingButton(firstVisibleRing.id);
    }
    return true;
  }

  function scheduleEmptyRetry(){
    if(emptyRetryTimer) return;
    emptyRetryTimer = setTimeout(() => {
      emptyRetryTimer = null;
      pollForUpdates(true);
    }, EMPTY_RETRY_MS);
  }

  function updatePublishedFromHeader(lastMod){
    if(!lastMod) return;
    const ms = Date.parse(lastMod);
    if(Number.isFinite(ms)) publishedAtMs = ms;
  }

  async function initialLoad(){
    currentDataUrl = resolveDataUrl();

    const { txt, lastMod } = await fetchTextMeta(currentDataUrl);
    updatePublishedFromHeader(lastMod);

    const rows = parseJsonText(txt, currentDataUrl);
    if(renderRows(rows)){
      lastRaw = txt;
      lastLastMod = lastMod || null;
    }
    syncPrintPdfLink();
  }

  async function pollForUpdates(force = false){
    if(isRefreshing) return;
    if(!currentDataUrl) return;

    isRefreshing = true;
    try{
      const { txt, lastMod } = await fetchTextMeta(currentDataUrl);

      const lmChanged = !!(lastMod && lastLastMod && lastMod !== lastLastMod);
      const bodyChanged = (lastRaw !== null && txt !== lastRaw);

      if(!force && !lmChanged && !bodyChanged){
        return;
      }

      const rows = parseJsonText(txt, currentDataUrl);
      const scrollY = window.scrollY;
      if(!renderRows(rows)){
        return;
      }

      lastRaw = txt;
      lastLastMod = lastMod || lastLastMod;
      updatePublishedFromHeader(lastMod);
      syncPrintPdfLink();

      window.scrollTo(0, scrollY);
    }catch(e){
      // keep existing UI
    }finally{
      isRefreshing = false;
    }
  }

  (async function main(){
    bindDateEdit();
    bindHorseEdits();
    bindHideMode();
    await initialLoad();
    setInterval(pollForUpdates, POLL_MS);
  })().catch(err=>{
    document.getElementById("rings").innerHTML =
      `<div class="err">${esc(String(err && err.stack ? err.stack : err))}</div>`;
  });

