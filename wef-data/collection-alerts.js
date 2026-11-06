"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");

function changes(before, after) {
  const lines = [];
  for (const [key, message] of Object.entries(after)) {
    if (!(key in before)) lines.push(message);
  }
  for (const key of Object.keys(before)) {
    if (!(key in after)) lines.push("Recovered: " + key);
  }
  return lines;
}

function check(runtime, report, attempt, logging) {
  const file = path.join(runtime, "collection-alerts.json");
  const state = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file,"utf8")) : {open:{}};
  const prior = JSON.stringify(state);
  for (const day of report.counts?.days || []) {
    const prefix = report["show-key"] + "|" + day.day + "|";
    const current = new Set();
    for (const e of (report["item-errors"] || []).filter(e => e.day === day.day)) {
      const field = /^classes ([0-9|:-]+) source-json$/.exec(e.key);
      const schedule = e.key === "Day inventory collection";
      if (!field && !schedule) continue;
      const key = prefix + (schedule ? "/schedule" : field[1]);
      current.add(key);
      const code = /HTTP (\d{3})/.exec(e.error)?.[1];
      const reason = code ? "HTTP " + code : /timeout|timed out/i.test(e.error) ? "timeout" : "invalid or unavailable schedule";
      state.open[key] = schedule
        ? day.day + " /schedule failed (" + reason + "). " + day.counts.saved + " records saved."
        : day.day + " class " + field[1] + ": Airtable rejected source-json (422).";
    }
    for (const key of Object.keys(state.open)) {
      if (!key.startsWith(prefix) || current.has(key)) continue;
      const item = key.slice(prefix.length);
      const recovered = item === "/schedule"
        ? day.counts.saved > 0 && day.counts["stage-seconds"]?.["prepare-rings-groups-classes"] !== undefined
        : day["confirmed-classes"]?.includes(item);
      if (recovered) delete state.open[key];
    }
  }
  for (const channel of ["desktop","records"]) {
    const delivery = state[channel] ||= {sent:{}};
    if (delivery.pending) {
      const receiptFile = path.join(runtime,"collection-notice-"+channel+".json");
      const receipt = fs.existsSync(receiptFile) ? JSON.parse(fs.readFileSync(receiptFile,"utf8").replace(/^\uFEFF/,"")) : {};
      if (receipt.attempt === delivery.pending.attempt && receipt.ok) delivery.sent = delivery.pending.target;
      else if (delivery.pending.pid) {
        try { process.kill(delivery.pending.pid,0); continue; } catch (e) { if(e.code!=="ESRCH") continue; }
      }
      delete delivery.pending;
    }
    const lines = changes(delivery.sent,state.open);
    if (!lines.length) continue;
    const child = spawn("powershell.exe",["-NoProfile","-NonInteractive","-File",
      path.join(__dirname,"collection-notice.ps1"),attempt,channel],{
      windowsHide:true,detached:false,stdio:["pipe","ignore","ignore"],
      env:Object.fromEntries(Object.entries(process.env).filter(([k])=> !/TOKEN|COOKIE|API_KEY|PASSWORD|SECRET/i.test(k)))
    });
    child.on("error",error=>logging.recordError(path.join(runtime,"logging","wef-data"),attempt,
      {category:"Notification",message:error.message},"Alert"));
    child.stdin.on("error",()=>{});
    child.stdin.end(lines.join("\n"));
    delivery.pending={attempt,pid:child.pid,target:{...state.open}};
    child.unref();
  }
  if (JSON.stringify(state)!==prior) logging.atomic(file,state);
}
module.exports = {check};
