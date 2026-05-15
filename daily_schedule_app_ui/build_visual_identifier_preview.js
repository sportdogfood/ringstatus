#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const LOCAL_ROOT = __dirname;
const DEFAULT_CONTRACT = path.join(LOCAL_ROOT, "visual_identifier_contract.json");
const DEFAULT_OUT = path.join(LOCAL_ROOT, "render", "visual_identifier_preview.html");
const DEFAULT_MODEL = path.join(LOCAL_ROOT, "render", "visual_identifier_preview_model.json");

function isBlank(value) {
  return value === undefined || value === null || String(value).trim() === "";
}

function normalizeKey(value) {
  return String(value || "").trim().toLowerCase();
}

function htmlEscape(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function timeSort(value) {
  const text = String(value || "");
  const match = text.match(/^(\d{1,2}):(\d{2})([AP])?/i);
  if (!match) return 999999;
  let hour = Number(match[1]);
  const minute = Number(match[2]);
  const suffix = String(match[3] || "").toUpperCase();
  if (suffix === "P" && hour < 12) hour += 12;
  if (suffix === "A" && hour === 12) hour = 0;
  return hour * 60 + minute;
}

function timePeriod(value) {
  const match = String(value || "").trim().match(/[AP]$/i);
  if (!match) return "";
  return match[0].toUpperCase() === "A" ? "am" : "pm";
}

function isFirstUpRollup(rollup) {
  if (rollup?.is_first_up === true) return true;
  const order = String(rollup?.order || rollup?.oog || "").trim();
  return /^1\s*\//.test(order);
}

function rowHasFirstUp(row) {
  if (row?.is_first_up === true) return true;
  return (row?.rollups || []).some(isFirstUpRollup);
}

function ringNumberFromRow(row) {
  return String(row?.ring_number || row?.ring || "").replace(/^\D+/, "").trim();
}

function ringAbbreviationFromRow(row, abbreviations) {
  const ringLabel = String(row?.ring || "").trim();
  if (ringLabel && abbreviations?.[ringLabel]) return abbreviations[ringLabel];
  const ringNumber = ringNumberFromRow(row);
  if (ringNumber) {
    const ringName = `Ring ${ringNumber}`;
    if (abbreviations?.[ringName]) return abbreviations[ringName];
    return `R${ringNumber}`;
  }
  return "";
}

function buildStatusByIncoming(statusTerms) {
  const map = new Map();
  for (const term of statusTerms || []) {
    map.set(normalizeKey(term.label), term);
    for (const incoming of term.incoming_terms || []) {
      map.set(normalizeKey(incoming), term);
    }
  }
  return map;
}

function buildSequenceTypeByIncoming(tokenGroups) {
  const map = new Map();
  const sequenceGroup = (tokenGroups || []).find((group) => group.id === "schedule_sequence_type");
  for (const token of sequenceGroup?.tokens || []) {
    const normalized = { id: token.id, label: token.label, shade: token.shade || "" };
    map.set(normalizeKey(token.label), normalized);
    for (const incoming of token.incoming_terms || []) {
      map.set(normalizeKey(incoming), normalized);
    }
  }
  return map;
}

function normalizeStatusTerm(value, statusByIncoming) {
  if (isBlank(value)) return null;
  return statusByIncoming.get(normalizeKey(value)) || null;
}

function normalizeSequenceType(value, sequenceTypeByIncoming) {
  if (isBlank(value)) return null;
  return sequenceTypeByIncoming.get(normalizeKey(value)) || { id: null, label: value, shade: "" };
}

function buildPreviewModel(contract) {
  const statusTerms = [...(contract.status_terms || [])].sort((a, b) => Number(a.priority || 99) - Number(b.priority || 99));
  const statusByIncoming = buildStatusByIncoming(statusTerms);
  const tokenGroups = contract.token_groups || [];
  const sequenceTypeByIncoming = buildSequenceTypeByIncoming(tokenGroups);
  const ringAbbreviations = contract.ring_abbreviations || {};
  const sampleRows = (contract.sample_rows || []).map((row) => {
    const status = normalizeStatusTerm(row.status, statusByIncoming);
    const sequenceType = normalizeSequenceType(row.schedule_sequence_type, sequenceTypeByIncoming);
    return {
      ...row,
      ring_number: ringNumberFromRow(row),
      ring_abbrev: ringAbbreviationFromRow(row, ringAbbreviations),
      schedule_sequence_type: sequenceType?.label || "",
      schedule_sequence_type_id: sequenceType?.id || null,
      schedule_sequence_type_shade: sequenceType?.shade || "",
      status: status?.label || row.status,
      status_id: status?.id || null,
      rollups: (row.rollups || []).map((rollup) => {
        const rollupStatus = normalizeStatusTerm(rollup.status, statusByIncoming);
        return {
          ...rollup,
          status: rollupStatus?.label || rollup.status,
          status_id: rollupStatus?.id || null,
        };
      }),
    };
  });

  return {
    meta: {
      generated_at: new Date().toISOString(),
      contract_version: contract?.meta?.version || null,
      purpose: contract?.meta?.purpose || null,
    },
    ringAbbreviations,
    statusTerms,
    statusByIncoming,
    tokenGroups,
    sampleRows,
    timeRows: [...sampleRows].sort((a, b) => timeSort(a.time) - timeSort(b.time) || Number(a.ring_number || 0) - Number(b.ring_number || 0)),
    ringWalk: sampleRows.find((row) => !isBlank(row.ring_walk))?.ring_walk || null,
  };
}

function tokenClass(token) {
  return [
    "token",
    `token--${token.treatment || "tag"}`,
    `shade--${token.shade || "slate"}`,
  ].join(" ");
}

function statusClass(statusId) {
  return ["state", statusId ? `state--${statusId}` : "state--unknown"].join(" ");
}

function modifierClass(prefix, value) {
  const key = normalizeKey(value).replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
  return key ? `${prefix}--${key}` : "";
}

function renderCellValue(value) {
  return isBlank(value) ? '<span class="cell-empty" aria-hidden="true"></span>' : htmlEscape(value);
}

function renderTimeValue(value) {
  return `<span class="time-clock" aria-hidden="true">
      <svg viewBox="0 0 16 16" focusable="false">
        <circle cx="8" cy="8" r="5.75"></circle>
        <path d="M8 4.75v3.4l2.25 1.35"></path>
      </svg>
    </span><span class="time-text">${renderCellValue(value)}</span>`;
}

function renderStatusTerm(term) {
  return `
    <div class="status-map-row">
      <div class="${statusClass(term.id)}">${htmlEscape(term.label)}</div>
      <div class="status-meaning">${htmlEscape(term.meaning || "")}</div>
      <div class="status-incoming">${htmlEscape((term.incoming_terms || []).join(" / "))}</div>
      <div class="status-treatment">${htmlEscape(term.treatment || "")}</div>
    </div>`;
}

function renderToken(token) {
  const sample = token.sample || (token.incoming_terms || []).join(" / ");
  return `
    <div class="token-swatch">
      <span class="${tokenClass(token)}">${htmlEscape(token.label)}</span>
      <span class="token-sample">${htmlEscape(sample)}</span>
    </div>`;
}

function renderTokenGroup(group) {
  return `
    <section class="identifier-section">
      <div class="section-head">
        <h2>${htmlEscape(group.title || group.id)}</h2>
        <span>${(group.tokens || []).length} tokens</span>
      </div>
      <div class="token-grid">
        ${(group.tokens || []).map(renderToken).join("")}
      </div>
    </section>`;
}

function renderRollup(rollup) {
  const horse = rollup.horse || rollup.name;
  const order = rollup.order || rollup.oog;
  return `
    <span class="rollup-row ${rollup.status_id ? `rollup-row--${htmlEscape(rollup.status_id)}` : ""}" data-horse="${htmlEscape(horse || "")}" data-rider="${htmlEscape(rollup.rider || "")}" data-rollup-status="${htmlEscape(rollup.status || "")}" data-rollup-status-id="${htmlEscape(rollup.status_id || "")}">
      <span class="rollup-cell rollup-cell--horse">${renderCellValue(horse)}</span>
      <span class="rollup-cell rollup-cell--time">${renderCellValue(rollup.time)}</span>
      <span class="rollup-cell rollup-cell--order">${renderCellValue(order)}</span>
    </span>`;
}

function rowBandClass(row) {
  return ["schedule-band", row.status_id ? `schedule-band--${row.status_id}` : "schedule-band--unknown"].join(" ");
}

function rowDataAttrs(row) {
  const rollups = row.rollups || [];
  const horses = rollups.map((rollup) => rollup.horse || rollup.name).filter((value) => !isBlank(value)).join("|");
  const riders = rollups.map((rollup) => rollup.rider).filter((value) => !isBlank(value)).join("|");
  return [
    `data-ring="${htmlEscape(row.ring_abbrev || row.ring_number || "")}"`,
    `data-group="${htmlEscape(row.group || "")}"`,
    `data-status="${htmlEscape(row.status || "")}"`,
    `data-status-id="${htmlEscape(row.status_id || "")}"`,
    `data-class-type="${htmlEscape(row.class_type || "")}"`,
    `data-sequence-type="${htmlEscape(row.schedule_sequence_type || "")}"`,
    `data-class-number="${htmlEscape(row.class_number || "")}"`,
    `data-period="${htmlEscape(timePeriod(row.time))}"`,
    `data-first-up="${rowHasFirstUp(row) ? "true" : "false"}"`,
    `data-has-rollups="${rollups.length ? "true" : "false"}"`,
    `data-horses="${htmlEscape(horses)}"`,
    `data-riders="${htmlEscape(riders)}"`,
  ].join(" ");
}

function renderSampleRow(row) {
  const timeClass = modifierClass("time-status", row.status_id);
  const sequenceClass = modifierClass("sequence-shade", row.schedule_sequence_type_shade);
  return `
    <article class="class-card ${rowBandClass(row)}" ${rowDataAttrs(row)}>
      <div class="schedule-line class-line">
        <div class="time-col c-time ${htmlEscape(timeClass)}">${renderTimeValue(row.time)}</div>
        <div class="ring-num-col"><span class="ring-token">${renderCellValue(row.ring_abbrev || row.ring_number)}</span></div>
        <div class="class-num-col"><span class="class-num-token">${renderCellValue(row.class_number)}</span></div>
        <div class="class-name-col c-name ${htmlEscape(sequenceClass)}">${renderCellValue(row.class_name)}</div>
        <div class="class-type-col"><span class="cell-token c-type">${renderCellValue(row.class_type)}</span></div>
      </div>
      ${(row.rollups || []).length ? `<div class="rollup-line">${row.rollups.map(renderRollup).join("")}</div>` : ""}
    </article>`;
}

function renderTimeRow(row) {
  const timeClass = modifierClass("time-status", row.status_id);
  const sequenceClass = modifierClass("sequence-shade", row.schedule_sequence_type_shade);
  return `
    <article class="schedule-line time-line ${rowBandClass(row)}" ${rowDataAttrs(row)}>
      <div class="time-col c-time ${htmlEscape(timeClass)}">${renderTimeValue(row.time)}</div>
      <div class="ring-num-col"><span class="ring-token">${renderCellValue(row.ring_abbrev || row.ring_number)}</span></div>
      <div class="class-num-col"><span class="class-num-token">${renderCellValue(row.class_number)}</span></div>
      <div class="class-name-col c-name ${htmlEscape(sequenceClass)}">${renderCellValue(row.class_name)}</div>
      <div class="class-type-col"><span class="cell-token c-type">${renderCellValue(row.class_type)}</span></div>
      ${(row.rollups || []).length ? `<div class="time-rollup-cell">${row.rollups.map(renderRollup).join("")}</div>` : ""}
    </article>`;
}

function uniqueValues(values) {
  return [...new Set(values.filter((value) => !isBlank(value)).map((value) => String(value).trim()))];
}

function renderRailButton({ label, value, rail, active = false }) {
  return `<button class="rail-button${active ? " is-active" : ""}" type="button" data-${rail}="${htmlEscape(value)}" aria-pressed="${active ? "true" : "false"}">${htmlEscape(label)}</button>`;
}

function ringStatusValues(model) {
  return ["NOW", "NEXT", "DONE"].filter((status) => model.sampleRows.some((row) => row.status === status));
}

function renderRingStatusControls(model) {
  const statusButtons = ringStatusValues(model)
    .map((status) => renderRailButton({ label: status, value: status, rail: "status-filter" }))
    .join("");
  return `<div class="ring-status-controls" aria-label="Status filters and indicator legend">${statusButtons}</div>`;
}

function renderRails(model) {
  const ringValues = uniqueValues(model.sampleRows.map((row) => row.ring_abbrev || row.ring_number));
  const horseValues = uniqueValues(model.sampleRows.flatMap((row) => (row.rollups || []).map((rollup) => rollup.horse || rollup.name)));
  const ringButtons = ringValues.map((ring) => renderRailButton({ label: ring, value: ring, rail: "ring-anchor" })).join("");
  const horseButtons = horseValues.map((horse) => renderRailButton({ label: horse, value: horse, rail: "horse-filter" })).join("");
  const quickFilters = [
    { label: "1UP", value: "first_up" },
    { label: "AM", value: "am" },
    { label: "PM", value: "pm" },
  ].map((filter) => renderRailButton({ label: filter.label, value: filter.value, rail: "quick-filter" })).join("");

  return `
    <section class="rail-stack" aria-label="Schedule navigation and horse filters">
      <div class="filter-actions" aria-label="Filter actions">
        <div class="quick-filter-group" aria-label="Quick filters">${quickFilters}</div>
        <button class="rollup-switch" type="button" data-rollup-only-toggle aria-pressed="false">
          <span class="rollup-switch__label">Trips</span>
          <span class="rollup-switch__track" aria-hidden="true">
            <span class="rollup-switch__thumb"></span>
          </span>
          <span class="rollup-switch__state" data-switch-state>OFF</span>
        </button>
      </div>
      <div class="rail-row" data-rail-row="rings" aria-label="Ring anchors">${ringButtons}</div>
      <div class="rail-row" data-rail-row="horses" aria-label="Horse filters">${horseButtons}</div>
    </section>`;
}

function renderVisualIdentifierHtml(model) {
  const statusRows = model.statusTerms.map(renderStatusTerm).join("");
  const tokenGroups = model.tokenGroups.map(renderTokenGroup).join("");
  const ringRows = model.sampleRows.map(renderSampleRow).join("");
  const timeRows = model.timeRows.map(renderTimeRow).join("");
  const rails = renderRails(model);
  const ringStatusControls = renderRingStatusControls(model);

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="Cache-Control" content="no-store">
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Expires" content="0">
  <title>RingStatus Visual Identifier Preview</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #090b11;
      --panel: #10141f;
      --panel-2: #151a29;
      --line: rgba(255, 255, 255, .08);
      --text: #e8ecf5;
      --muted: #9aa3b4;
      --faint: #687085;
      --green: #49d17d;
      --green-bg: rgba(73, 209, 125, .16);
      --blue: #8fb8ff;
      --blue-bg: rgba(73, 118, 255, .16);
      --violet: #c386ff;
      --violet-bg: rgba(156, 89, 255, .18);
      --teal: #58dac7;
      --teal-bg: rgba(88, 218, 199, .16);
      --amber: #f2c15c;
      --amber-bg: rgba(242, 193, 92, .16);
      --red: #ff8d9a;
      --red-bg: rgba(255, 141, 154, .18);
      --token-radius: 6px;
      --schedule-cols: 7.4ch 4.5ch 4ch minmax(0, 1fr) 4ch;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-size: 12px;
      line-height: 1.35;
    }
    .page {
      width: min(980px, 100%);
      margin: 0 auto;
      padding: 16px 10px 34px;
    }
    .topbar {
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 14px;
      padding: 8px 2px 16px;
      border-bottom: 1px solid var(--line);
    }
    h1, h2 {
      margin: 0;
      font-size: 14px;
      letter-spacing: 0;
      font-weight: 650;
    }
    h1 { font-size: 16px; }
    .subtle {
      color: var(--muted);
      margin-top: 4px;
      font-size: 12px;
    }
    .scope-token {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: var(--token-radius);
      color: var(--muted);
      font-weight: 650;
      padding: 7px 10px;
      white-space: nowrap;
    }
    .preview-grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(300px, .85fr);
      gap: 12px;
      margin-top: 12px;
      align-items: start;
    }
    .rail-stack {
      display: grid;
      gap: 6px;
      margin-top: 10px;
    }
    .filter-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 6px;
      min-width: 0;
      overflow-x: auto;
      padding-bottom: 2px;
      scrollbar-width: thin;
    }
    .quick-filter-group {
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      gap: 5px;
    }
    .quick-filter-group .rail-button {
      min-height: 25px;
      padding: 4px 8px;
      font-size: 10px;
      border-color: rgba(143, 184, 255, .2);
    }
    .quick-filter-group .rail-button.is-active {
      color: var(--blue);
      border-color: rgba(143, 184, 255, .42);
      background: rgba(73, 118, 255, .16);
    }
    .rollup-switch {
      flex: 0 0 auto;
      min-height: 25px;
      display: inline-flex;
      align-items: center;
      gap: 7px;
      border-radius: var(--token-radius);
      border: 1px solid rgba(154, 163, 180, .18);
      background: rgba(154, 163, 180, .08);
      color: var(--muted);
      padding: 4px 8px;
      font: inherit;
      font-size: 10px;
      font-weight: 750;
      letter-spacing: 0;
      cursor: pointer;
      white-space: nowrap;
    }
    .rollup-switch__track {
      width: 30px;
      height: 15px;
      display: inline-flex;
      align-items: center;
      padding: 2px;
      border-radius: var(--token-radius);
      border: 1px solid rgba(154, 163, 180, .18);
      background: rgba(9, 11, 17, .45);
    }
    .rollup-switch__thumb {
      width: 9px;
      height: 9px;
      border-radius: calc(var(--token-radius) - 2px);
      background: #8d96a8;
      transform: translateX(0);
      transition: transform .14s ease, background .14s ease;
    }
    .rollup-switch__state {
      min-width: 22px;
      text-align: right;
      color: var(--faint);
    }
    .rollup-switch.is-active {
      color: #b6f0c8;
      border-color: rgba(73, 209, 125, .34);
      background: rgba(73, 209, 125, .1);
    }
    .rollup-switch.is-active .rollup-switch__thumb {
      transform: translateX(15px);
      background: var(--green);
    }
    .rollup-switch.is-active .rollup-switch__state {
      color: var(--green);
    }
    .rail-row {
      display: flex;
      gap: 6px;
      overflow-x: auto;
      padding: 2px 0 4px;
      scrollbar-width: thin;
    }
    .rail-button {
      flex: 0 0 auto;
      min-height: 25px;
      border-radius: var(--token-radius);
      border: 1px solid rgba(154, 163, 180, .18);
      background: rgba(154, 163, 180, .08);
      color: var(--muted);
      padding: 4px 8px;
      font: inherit;
      font-size: 11px;
      font-weight: 700;
      white-space: nowrap;
      cursor: pointer;
    }
    .rail-button.is-active {
      color: var(--text);
      border-color: rgba(143, 184, 255, .34);
      background: rgba(143, 184, 255, .13);
    }
    .ring-status-controls .rail-button[data-status-filter="NOW"] {
      border-color: rgba(73, 209, 125, .34);
      color: var(--green);
    }
    .ring-status-controls .rail-button[data-status-filter="NOW"].is-active {
      background: rgba(73, 209, 125, .14);
      border-color: rgba(73, 209, 125, .48);
    }
    .ring-status-controls .rail-button[data-status-filter="NEXT"] {
      border-color: rgba(143, 184, 255, .34);
      color: var(--blue);
    }
    .ring-status-controls .rail-button[data-status-filter="NEXT"].is-active {
      background: rgba(73, 118, 255, .16);
      border-color: rgba(143, 184, 255, .5);
    }
    .ring-status-controls .rail-button[data-status-filter="DONE"] {
      border-color: rgba(154, 163, 180, .2);
      color: #8d96a8;
    }
    .ring-status-controls .rail-button[data-status-filter="DONE"].is-active {
      background: rgba(154, 163, 180, .14);
      border-color: rgba(154, 163, 180, .34);
    }
    .rail-row[data-rail-row="horses"] .rail-button.is-active {
      border-color: rgba(73, 209, 125, .34);
      color: #b6f0c8;
    }
    .panel, .identifier-section, .ring-card, .time-card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    .panel-head, .section-head, .ring-line {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      min-height: 38px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
    }
    .panel-head span, .section-head span {
      color: var(--faint);
      font-size: 11px;
      font-weight: 620;
      text-transform: uppercase;
    }
    .status-map-row {
      display: grid;
      grid-template-columns: 58px minmax(120px, .8fr) minmax(180px, 1.2fr) 110px;
      gap: 10px;
      align-items: center;
      min-height: 42px;
      padding: 8px 12px;
      border-bottom: 1px solid var(--line);
    }
    .status-map-row:last-child { border-bottom: 0; }
    .status-meaning {
      font-weight: 620;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .status-incoming, .status-treatment, .token-sample {
      color: var(--muted);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .identifier-stack {
      display: grid;
      gap: 12px;
    }
    .token-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .token-swatch {
      min-width: 0;
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 10px;
      border-bottom: 1px solid var(--line);
      border-right: 1px solid var(--line);
    }
    .token-swatch:nth-child(2n) { border-right: 0; }
    .token {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 21px;
      min-width: 30px;
      border-radius: var(--token-radius);
      padding: 3px 7px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .token--column, .token--mono_text, .token--tiny_text, .token--eyebrow {
      min-width: auto;
      padding: 3px 7px;
      border-radius: var(--token-radius);
    }
    .state {
      justify-self: start;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 44px;
      min-height: 22px;
      padding: 3px 8px;
      border-radius: var(--token-radius);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0;
      white-space: nowrap;
    }
    .state--now {
      background: var(--green-bg);
      color: var(--green);
      border: 1px solid rgba(73, 209, 125, .3);
    }
    .state--next {
      background: transparent;
      color: var(--blue);
      border: 1px solid rgba(143, 184, 255, .42);
    }
    .state--following {
      background: rgba(143, 184, 255, .07);
      color: #b6c8ee;
      border: 1px solid rgba(143, 184, 255, .18);
    }
    .state--upcoming {
      background: rgba(154, 163, 180, .12);
      color: var(--muted);
      border: 1px solid rgba(154, 163, 180, .18);
    }
    .state--completed {
      background: rgba(154, 163, 180, .08);
      color: #767f90;
      border: 1px solid rgba(154, 163, 180, .12);
    }
    .shade--green { color: var(--green); background: var(--green-bg); border: 1px solid rgba(73, 209, 125, .24); }
    .shade--blue { color: var(--blue); background: var(--blue-bg); border: 1px solid rgba(143, 184, 255, .24); }
    .shade--blue_muted { color: #b6c8ee; background: rgba(143, 184, 255, .08); border: 1px solid rgba(143, 184, 255, .18); }
    .shade--slate { color: #d4d9e6; background: rgba(154, 163, 180, .12); border: 1px solid rgba(154, 163, 180, .16); }
    .shade--muted { color: #8d96a8; background: rgba(154, 163, 180, .08); border: 1px solid rgba(154, 163, 180, .12); }
    .shade--violet { color: var(--violet); background: var(--violet-bg); border: 1px solid rgba(195, 134, 255, .22); }
    .shade--teal { color: var(--teal); background: var(--teal-bg); border: 1px solid rgba(88, 218, 199, .22); }
    .shade--amber { color: var(--amber); background: var(--amber-bg); border: 1px solid rgba(242, 193, 92, .22); }
    .shade--red { color: var(--red); background: var(--red-bg); border: 1px solid rgba(255, 141, 154, .25); }
    .shade--text { color: var(--text); background: transparent; border-color: transparent; }
    .ring-card { position: sticky; top: 10px; }
    .right-stack {
      display: grid;
      gap: 12px;
      align-self: start;
      position: sticky;
      top: 10px;
    }
    .right-stack .ring-card {
      position: static;
    }
    .ring-title {
      font-size: 14px;
      font-weight: 620;
      flex: 0 0 auto;
    }
    .ring-eyebrow {
      display: flex;
      align-items: center;
      gap: 8px;
      justify-content: flex-end;
      flex: 1 1 auto;
      min-width: 0;
      margin-left: auto;
    }
    .ring-status-controls {
      display: flex;
      justify-content: flex-end;
      gap: 5px;
      overflow-x: auto;
      min-width: 0;
      scrollbar-width: thin;
    }
    .ring-status-controls .rail-button {
      min-height: 23px;
      padding: 3px 7px;
      font-size: 10px;
    }
    .ring-walk {
      min-width: 62px;
      min-height: 23px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 4px 8px;
      color: var(--muted);
      background: rgba(154, 163, 180, .1);
      border: 1px solid rgba(154, 163, 180, .14);
      font-size: 11px;
      font-weight: 650;
      white-space: nowrap;
    }
    .ring-walk:empty {
      display: none;
    }
    .cell-empty {
      display: inline-block;
      width: 1ch;
      height: 1em;
    }
    .class-card {
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(255, 255, 255, .01), rgba(255, 255, 255, 0));
    }
    .class-card:last-child { border-bottom: 0; }
    .ring-card .schedule-band:nth-of-type(even),
    .time-card .schedule-band:nth-of-type(even) {
      background: rgba(255, 255, 255, .012);
    }
    .schedule-band {
      position: relative;
      isolation: isolate;
    }
    .schedule-band::before {
      content: "";
      position: absolute;
      inset: 3px;
      border-radius: var(--token-radius);
      border: 1px solid transparent;
      pointer-events: none;
      z-index: 0;
    }
    .schedule-band > * {
      position: relative;
      z-index: 1;
    }
    .schedule-band--now::before {
      border-color: rgba(73, 209, 125, .34);
      background: transparent;
    }
    .schedule-band--next::before {
      border-color: rgba(143, 184, 255, .18);
      background: transparent;
    }
    .schedule-band--completed::before {
      border-color: rgba(154, 163, 180, .18);
      background: transparent;
    }
    .schedule-band.is-filter-hidden,
    .rollup-row.is-filter-hidden {
      display: none;
    }
    .schedule-line {
      display: grid;
      grid-template-columns: var(--schedule-cols);
      gap: 3px;
      align-items: center;
      min-height: 38px;
      padding: 7px 8px;
    }
    .time-line {
      border-bottom: 1px solid var(--line);
    }
    .time-line:last-child { border-bottom: 0; }
    .time-col,
    .ring-num-col,
    .class-num-col,
    .class-type-col {
      min-width: 0;
      width: 100%;
      overflow: hidden;
      white-space: nowrap;
    }
    .time-col {
      display: inline-flex;
      align-items: center;
      gap: 3px;
      text-align: left;
      overflow: hidden;
    }
    .time-clock {
      width: 9px;
      height: 9px;
      flex: 0 0 9px;
      opacity: .72;
      color: currentColor;
      overflow: visible;
    }
    .time-clock svg {
      display: block;
      width: 9px;
      height: 9px;
      min-width: 9px;
      fill: none;
      stroke: currentColor;
      stroke-width: 1.6;
      stroke-linecap: round;
      stroke-linejoin: round;
      overflow: visible;
    }
    .time-text {
      min-width: max-content;
      flex: 0 0 auto;
      overflow: visible;
      text-overflow: clip;
    }
    .c-time, .c-num, .c-type, .trip-metric {
      font-size: 10px;
      font-weight: 650;
      color: var(--muted);
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .ring-num-col {
      display: inline-flex;
      justify-content: center;
      align-items: center;
    }
    .c-num {
      color: var(--text);
      text-align: right;
    }
    .c-name {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 11px;
      font-weight: 560;
      padding-left: 3px;
    }
    .class-type-col {
      display: inline-flex;
      justify-content: center;
      align-items: center;
    }
    .cell-token,
    .trip-metric {
      width: 100%;
      min-width: 0;
      min-height: 22px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 6px;
      padding: 2px 4px;
      font-size: 9px;
      font-weight: 700;
      line-height: 1;
      overflow: hidden;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .cell-token,
    .class-num-token,
    .trip-metric,
    .ring-token {
      border-radius: var(--token-radius);
    }
    .class-num-token {
      width: 100%;
      min-width: 0;
      min-height: 22px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 2px 4px;
      color: var(--text);
      background: rgba(255, 255, 255, .06);
      border: 1px solid rgba(255, 255, 255, .12);
      font-size: 10px;
      font-weight: 750;
      line-height: 1;
      overflow: hidden;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .ring-token {
      width: 100%;
      min-width: 0;
      min-height: 22px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 2px 4px;
      color: var(--muted);
      background: rgba(154, 163, 180, .08);
      border: 1px solid rgba(154, 163, 180, .12);
      font-size: 10px;
      font-weight: 700;
      line-height: 1;
      overflow: hidden;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .c-type {
      color: var(--muted);
      background: rgba(154, 163, 180, .08);
      border: 1px solid rgba(154, 163, 180, .12);
    }
    .time-status--now { color: var(--green); }
    .time-status--next { color: var(--blue); }
    .time-status--following { color: #b6c8ee; }
    .time-status--upcoming { color: var(--muted); }
    .time-status--completed { color: #767f90; }
    .sequence-shade--teal { color: var(--teal); }
    .sequence-shade--violet { color: var(--violet); }
    .sequence-shade--green { color: var(--green); }
    .sequence-shade--blue { color: var(--blue); }
    .sequence-shade--amber { color: var(--amber); }
    .sequence-shade--muted { color: var(--muted); }
    .rollup-line {
      display: flex;
      justify-content: flex-end;
      gap: 6px;
      overflow-x: auto;
      padding: 0 8px 8px 8px;
      scrollbar-width: thin;
    }
    .time-rollup-cell {
      grid-column: 1 / -1;
      display: flex;
      justify-content: flex-end;
      gap: 6px;
      overflow-x: auto;
      padding-top: 2px;
      scrollbar-width: thin;
    }
    .rollup-row {
      display: inline-grid;
      grid-template-columns: minmax(0, max-content) minmax(6ch, 6ch) minmax(5ch, 5ch);
      column-gap: 4px;
      align-items: center;
      min-height: 24px;
      border-radius: var(--token-radius);
      border: 1px solid rgba(154, 163, 180, .16);
      background: rgba(154, 163, 180, .1);
      color: var(--text);
      padding: 3px 6px;
      font-size: 10px;
      font-weight: 650;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .rollup-cell {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: var(--muted);
    }
    .rollup-cell--horse {
      max-width: 8ch;
      color: #b6c8ee;
    }
    .rollup-cell--time,
    .rollup-cell--order {
      text-align: center;
      width: 100%;
      min-width: 0;
      overflow: visible;
      text-overflow: clip;
    }
    .rollup-cell--time,
    .rollup-cell--order {
      border-left: 1px solid rgba(182, 200, 238, .4);
      padding-left: 6px;
    }
    .rollup-row--now { border-color: rgba(73, 209, 125, .24); }
    .rollup-row--next { border-color: rgba(143, 184, 255, .26); }
    .rollup-row--completed { color: #8d96a8; }
    @media (max-width: 780px) {
      .page { padding-inline: 8px; }
      .preview-grid { grid-template-columns: 1fr; }
      .right-stack { position: static; }
      .status-map-row {
        grid-template-columns: 54px minmax(0, 1fr);
      }
      .status-incoming, .status-treatment { grid-column: 2; }
      .token-grid { grid-template-columns: 1fr; }
      .token-swatch { border-right: 0; }
    }
    @media (max-width: 430px) {
      .rollup-line { padding-left: 8px; }
    }
  </style>
</head>
<body>
  <main class="page">
    <header class="topbar">
      <div>
        <h1>Visual Identifier Preview</h1>
        <div class="subtle">Generated ${htmlEscape(model.meta.generated_at)} from contract ${htmlEscape(model.meta.contract_version)}</div>
      </div>
      <div class="scope-token">No icons. No schedule wiring.</div>
    </header>

    ${rails}

    <div class="preview-grid">
      <div class="right-stack">
        <section class="ring-card">
          <div class="ring-line">
            <div class="ring-title">Ring 6</div>
            <div class="ring-eyebrow">
              ${ringStatusControls}
              <div class="ring-walk">${model.ringWalk ? `WALK ${htmlEscape(model.ringWalk)}` : ""}</div>
            </div>
          </div>
          ${ringRows}
        </section>

        <section class="time-card">
          <div class="ring-line">
            <div class="ring-title">Time</div>
            <div class="ring-eyebrow">
              ${ringStatusControls}
              <div class="ring-walk"></div>
            </div>
          </div>
          ${timeRows}
        </section>
      </div>

      <div class="identifier-stack">
        <section class="panel">
          <div class="panel-head">
            <h2>Status Language</h2>
            <span>normalize first</span>
          </div>
          ${statusRows}
        </section>
        ${tokenGroups}
      </div>
    </div>
  </main>
  <script>
    (() => {
      const setActive = (buttons, activeButton = null) => {
        buttons.forEach((button) => {
          const active = button === activeButton;
          button.classList.toggle("is-active", active);
          button.setAttribute("aria-pressed", active ? "true" : "false");
        });
      };
      const setActiveByValue = (buttons, attribute, activeValue) => {
        buttons.forEach((button) => {
          const active = Boolean(activeValue) && button.dataset[attribute] === activeValue;
          button.classList.toggle("is-active", active);
          button.setAttribute("aria-pressed", active ? "true" : "false");
        });
      };

      const ringButtons = [...document.querySelectorAll("[data-ring-anchor]")];
      let activeRing = "";
      ringButtons.forEach((button) => {
        button.addEventListener("click", () => {
          const ring = button.dataset.ringAnchor;
          const nextRing = activeRing === ring ? "" : ring;
          activeRing = nextRing;
          setActive(ringButtons, nextRing ? button : null);
          const target = nextRing
            ? document.querySelector(\`.schedule-band[data-ring="\${CSS.escape(nextRing)}"]\`)
            : document.querySelector(".right-stack");
          target?.scrollIntoView({ behavior: "smooth", block: "start" });
        });
      });

      const horseButtons = [...document.querySelectorAll("[data-horse-filter]")];
      const statusButtons = [...document.querySelectorAll("[data-status-filter]")];
      const quickFilterButtons = [...document.querySelectorAll("[data-quick-filter]")];
      const rollupOnlyToggle = document.querySelector("[data-rollup-only-toggle]");
      const rollupOnlyState = document.querySelector("[data-switch-state]");
      let activeHorse = "";
      let activeStatus = "";
      let activeQuickFilter = "";
      let activeRollupOnly = false;

      const applyFilters = () => {
        document.body.dataset.horseFilter = activeHorse;
        document.body.dataset.statusFilter = activeStatus;
        document.body.dataset.quickFilter = activeQuickFilter;
        document.body.dataset.rollupOnly = activeRollupOnly ? "true" : "";
        document.querySelectorAll(".schedule-band").forEach((band) => {
          const horses = (band.dataset.horses || "").split("|").filter(Boolean);
          const horseMatches = !activeHorse || horses.includes(activeHorse);
          const statusMatches = !activeStatus || band.dataset.status === activeStatus;
          const quickMatches = !activeQuickFilter
            || (activeQuickFilter === "first_up" && band.dataset.firstUp === "true")
            || (activeQuickFilter === "am" && band.dataset.period === "am")
            || (activeQuickFilter === "pm" && band.dataset.period === "pm");
          const rollupOnlyMatches = !activeRollupOnly || band.dataset.hasRollups === "true";
          const bandMatches = horseMatches && statusMatches && quickMatches && rollupOnlyMatches;
          band.classList.toggle("is-filter-hidden", !bandMatches);
          band.querySelectorAll(".rollup-row").forEach((rollup) => {
            const rollupMatches = !activeHorse || rollup.dataset.horse === activeHorse;
            rollup.classList.toggle("is-filter-hidden", !rollupMatches);
          });
        });
      };

      horseButtons.forEach((button) => {
        button.addEventListener("click", () => {
          const horse = button.dataset.horseFilter;
          activeHorse = activeHorse === horse ? "" : horse;
          setActive(horseButtons, activeHorse ? button : null);
          applyFilters();
        });
      });

      statusButtons.forEach((button) => {
        button.addEventListener("click", () => {
          const status = button.dataset.statusFilter;
          activeStatus = activeStatus === status ? "" : status;
          setActiveByValue(statusButtons, "statusFilter", activeStatus);
          applyFilters();
        });
      });

      quickFilterButtons.forEach((button) => {
        button.addEventListener("click", () => {
          const quickFilter = button.dataset.quickFilter;
          activeQuickFilter = activeQuickFilter === quickFilter ? "" : quickFilter;
          setActive(quickFilterButtons, activeQuickFilter ? button : null);
          applyFilters();
        });
      });

      rollupOnlyToggle?.addEventListener("click", () => {
        activeRollupOnly = !activeRollupOnly;
        rollupOnlyToggle.classList.toggle("is-active", activeRollupOnly);
        rollupOnlyToggle.setAttribute("aria-pressed", activeRollupOnly ? "true" : "false");
        if (rollupOnlyState) rollupOnlyState.textContent = activeRollupOnly ? "ON" : "OFF";
        applyFilters();
      });
    })();
  </script>
</body>
</html>`;
}

function parseArgs(argv) {
  const args = { contract: DEFAULT_CONTRACT, out: DEFAULT_OUT, model: DEFAULT_MODEL };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--contract") args.contract = path.resolve(argv[++i]);
    else if (arg === "--out") args.out = path.resolve(argv[++i]);
    else if (arg === "--model") args.model = path.resolve(argv[++i]);
    else if (arg === "--help") args.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log("Usage: node daily_schedule_app_source/build_visual_identifier_preview.js");
    return;
  }
  const contract = JSON.parse(fs.readFileSync(args.contract, "utf8"));
  const model = buildPreviewModel(contract);
  const html = renderVisualIdentifierHtml(model);
  const serializableModel = {
    ...model,
    statusByIncoming: Object.fromEntries([...model.statusByIncoming.entries()].map(([key, value]) => [key, value.id])),
  };
  fs.mkdirSync(path.dirname(args.out), { recursive: true });
  fs.writeFileSync(args.out, html, "utf8");
  fs.writeFileSync(args.model, JSON.stringify(serializableModel, null, 2) + "\n", "utf8");
  console.log(JSON.stringify({
    ok: true,
    out: args.out,
    model: args.model,
    status_terms: model.statusTerms.length,
    token_groups: model.tokenGroups.length,
    sample_rows: model.sampleRows.length,
  }, null, 2));
}

if (require.main === module) {
  try {
    main();
  } catch (err) {
    console.error(err && err.stack ? err.stack : String(err));
    process.exitCode = 1;
  }
}

module.exports = {
  buildPreviewModel,
  normalizeStatusTerm,
  renderVisualIdentifierHtml,
  renderRails,
};
