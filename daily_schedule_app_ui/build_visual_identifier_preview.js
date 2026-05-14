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

function normalizeStatusTerm(value, statusByIncoming) {
  if (isBlank(value)) return null;
  return statusByIncoming.get(normalizeKey(value)) || null;
}

function buildPreviewModel(contract) {
  const statusTerms = [...(contract.status_terms || [])].sort((a, b) => Number(a.priority || 99) - Number(b.priority || 99));
  const statusByIncoming = buildStatusByIncoming(statusTerms);
  const tokenGroups = contract.token_groups || [];
  const ringAbbreviations = contract.ring_abbreviations || {};
  const sampleRows = (contract.sample_rows || []).map((row) => {
    const status = normalizeStatusTerm(row.status, statusByIncoming);
    return {
      ...row,
      ring_number: ringNumberFromRow(row),
      ring_abbrev: ringAbbreviationFromRow(row, ringAbbreviations),
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

function renderCellValue(value) {
  return isBlank(value) ? '<span class="cell-empty" aria-hidden="true"></span>' : htmlEscape(value);
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
  return `
    <span class="epill ${rollup.status_id ? `epill--${htmlEscape(rollup.status_id)}` : ""}">
      <span class="epill__name">${renderCellValue(rollup.name)}</span>
      <span class="epill__sep" aria-hidden="true"></span>
      <span class="epill__time">${renderCellValue(rollup.time)}</span>
      <span class="epill__sep" aria-hidden="true"></span>
      <span class="epill__oog">${renderCellValue(rollup.oog)}</span>
      <span class="epill__sep" aria-hidden="true"></span>
      <span class="epill__metric">In: ${renderCellValue(rollup.in)}</span>
      <span class="epill__sep" aria-hidden="true"></span>
      <span class="epill__metric">Walk: ${renderCellValue(rollup.walk)}</span>
    </span>`;
}

function renderSampleRow(row) {
  return `
    <article class="class-card">
      <div class="class-line">
        <div class="time-col c-time"><span class="time-mark time-mark--${htmlEscape(row.status_id || "unknown")}" aria-hidden="true"></span><span>${renderCellValue(row.time)}</span></div>
        <div class="class-num-col c-num">${renderCellValue(row.class_number)}</div>
        <div class="class-name-col c-name">${renderCellValue(row.class_name)}</div>
        <div class="class-type-col"><span class="cell-token c-type">${renderCellValue(row.class_type)}</span></div>
        <div class="status-col"><span class="cell-token ${statusClass(row.status_id)}">${renderCellValue(row.status)}</span></div>
        <div class="trips-col"><span class="trip-metric">${renderCellValue(row.metric)}</span></div>
      </div>
      ${(row.rollups || []).length ? `<div class="rollup-line">${row.rollups.map(renderRollup).join("")}</div>` : ""}
    </article>`;
}

function renderTimeRow(row) {
  return `
    <article class="time-line">
      <div class="time-col c-time"><span class="time-mark time-mark--${htmlEscape(row.status_id || "unknown")}" aria-hidden="true"></span><span>${renderCellValue(row.time)}</span></div>
      <div class="ring-num-col"><span class="ring-token">${renderCellValue(row.ring_abbrev || row.ring_number)}</span></div>
      <div class="class-num-col c-num">${renderCellValue(row.class_number)}</div>
      <div class="class-name-col c-name">${renderCellValue(row.class_name)}</div>
      <div class="class-type-col"><span class="cell-token c-type">${renderCellValue(row.class_type)}</span></div>
      <div class="status-col"><span class="cell-token ${statusClass(row.status_id)}">${renderCellValue(row.status)}</span></div>
      <div class="trips-col"><span class="trip-metric">${renderCellValue(row.metric)}</span></div>
    </article>`;
}

function renderVisualIdentifierHtml(model) {
  const statusRows = model.statusTerms.map(renderStatusTerm).join("");
  const tokenGroups = model.tokenGroups.map(renderTokenGroup).join("");
  const ringRows = model.sampleRows.map(renderSampleRow).join("");
  const timeRows = model.timeRows.map(renderTimeRow).join("");

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-size: 13px;
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
      font-size: 15px;
      letter-spacing: 0;
      font-weight: 650;
    }
    h1 { font-size: 17px; }
    .subtle {
      color: var(--muted);
      margin-top: 4px;
      font-size: 12px;
    }
    .scope-token {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 999px;
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
      border-radius: 999px;
      padding: 3px 7px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .token--column, .token--mono_text, .token--tiny_text {
      min-width: auto;
      padding: 0;
      border-radius: 0;
      background: transparent;
      border: 0;
      font-variant-numeric: tabular-nums;
    }
    .token--eyebrow {
      min-width: auto;
      padding: 0;
      border-radius: 0;
      background: transparent;
      border: 0;
      color: var(--muted);
      font-size: 10px;
    }
    .state {
      justify-self: start;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 44px;
      min-height: 22px;
      padding: 3px 8px;
      border-radius: 999px;
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
      font-size: 15px;
      font-weight: 620;
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
      visibility: hidden;
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
    .class-line {
      display: grid;
      grid-template-columns: 6ch 3ch minmax(0, 1fr) 4ch 5ch 6ch;
      gap: 3px;
      align-items: center;
      min-height: 38px;
      padding: 7px 8px;
    }
    .time-line {
      display: grid;
      grid-template-columns: 6ch 4.5ch 3ch minmax(0, 1fr) 4ch 5ch 6ch;
      gap: 3px;
      align-items: center;
      min-height: 38px;
      padding: 7px 8px;
      border-bottom: 1px solid var(--line);
    }
    .time-line:last-child { border-bottom: 0; }
    .time-col,
    .ring-num-col,
    .class-num-col,
    .class-type-col,
    .status-col,
    .trips-col {
      min-width: 0;
      width: 100%;
      overflow: hidden;
      white-space: nowrap;
    }
    .time-col {
      display: inline-flex;
      align-items: center;
      gap: 3px;
    }
    .time-mark {
      width: 4px;
      height: 4px;
      border-radius: 999px;
      background: var(--faint);
      opacity: .8;
      flex: 0 0 auto;
    }
    .time-mark--now {
      width: 5px;
      height: 5px;
      background: var(--green);
      box-shadow: 0 0 0 2px rgba(73, 209, 125, .11);
      opacity: 1;
    }
    .time-mark--next {
      background: var(--blue);
      opacity: .95;
    }
    .time-mark--following {
      background: #b6c8ee;
      opacity: .75;
    }
    .time-mark--upcoming {
      background: var(--muted);
      opacity: .55;
    }
    .time-mark--completed {
      background: #697185;
      opacity: .38;
    }
    .c-time, .c-num, .c-type, .trip-metric {
      font-size: 11px;
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
      font-size: 12px;
      font-weight: 680;
    }
    .class-type-col,
    .status-col,
    .trips-col {
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
      font-size: 10px;
      font-weight: 700;
      line-height: 1;
      overflow: hidden;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .cell-token,
    .trip-metric,
    .ring-token {
      border-radius: var(--token-radius);
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
    .trips-col {
      text-align: right;
    }
    .trip-metric {
      color: var(--amber);
      border: 1px solid rgba(242, 193, 92, .22);
      background: rgba(242, 193, 92, .1);
    }
    .rollup-line {
      display: flex;
      justify-content: flex-end;
      gap: 6px;
      overflow-x: auto;
      padding: 0 8px 8px 8px;
      scrollbar-width: thin;
    }
    .epill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 24px;
      border-radius: 999px;
      border: 1px solid rgba(154, 163, 180, .16);
      background: rgba(154, 163, 180, .1);
      color: var(--text);
      padding: 4px 8px;
      font-size: 11px;
      font-weight: 650;
      white-space: nowrap;
    }
    .epill__sep {
      width: 2px;
      height: 2px;
      border-radius: 999px;
      background: rgba(154, 163, 180, .48);
      flex: 0 0 auto;
    }
    .epill__oog, .epill__time, .epill__metric {
      color: var(--muted);
      font-variant-numeric: tabular-nums;
    }
    .epill__name {
      min-width: 44px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .epill__time,
    .epill__oog,
    .epill__metric {
      min-width: 38px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .epill__metric { min-width: 52px; }
    .epill--now { border-color: rgba(73, 209, 125, .24); }
    .epill--next { border-color: rgba(143, 184, 255, .26); }
    .epill--completed { color: #8d96a8; }
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
      .class-line {
        grid-template-columns: 6ch 3ch minmax(0, 1fr) 4ch 5ch 6ch;
        gap: 3px;
      }
      .time-line {
        grid-template-columns: 6ch 4.5ch 3ch minmax(0, 1fr) 4ch 5ch 6ch;
        gap: 3px;
      }
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

    <div class="preview-grid">
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

      <div class="right-stack">
        <section class="ring-card">
          <div class="ring-line">
            <div class="ring-title">Ring 6</div>
            <div class="ring-walk">${model.ringWalk ? `WALK ${htmlEscape(model.ringWalk)}` : ""}</div>
          </div>
          ${ringRows}
        </section>

        <section class="time-card">
          <div class="ring-line">
            <div class="ring-title">Time</div>
            <div class="ring-walk"></div>
          </div>
          ${timeRows}
        </section>
      </div>
    </div>
  </main>
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
};
