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

function isDoneRow(row) {
  return row?.status_id === "completed" || row?.status_id === "done" || row?.status === "DONE";
}

function renderCellValue(value) {
  return isBlank(value) ? '<span class="cell-empty" aria-hidden="true"></span>' : htmlEscape(value);
}

function renderAlignedTimeText(value) {
  if (isBlank(value)) return renderCellValue(value);
  return htmlEscape(String(value).trim());
}

function renderAlignedOrderText(value) {
  if (isBlank(value)) return renderCellValue(value);
  return htmlEscape(String(value).trim());
}

function renderTimeValue(value) {
  return `<span class="time-clock" aria-hidden="true">
      <svg viewBox="0 0 16 16" focusable="false">
        <circle cx="8" cy="8" r="5.75"></circle>
        <path d="M8 4.75v3.4l2.25 1.35"></path>
      </svg>
    </span><span class="time-value">${renderAlignedTimeText(value)}</span>`;
}

function renderOrderOfGoValue(rollup) {
  if (!isBlank(rollup?.order_of_go)) return rollup.order_of_go;
  const order = rollup?.order || rollup?.oog;
  return isBlank(order) ? "" : String(order).split("/")[0].trim();
}

function renderRingNameValue(row) {
  const ringName = row.ring_name || row.ring;
  const parts = [ringName];
  if (!isBlank(row.ring_late)) parts.push(`{${row.ring_late}}`);
  if (!isBlank(row.ring_takes)) parts.push(`{${row.ring_takes}}`);
  return parts.filter((value) => !isBlank(value)).join(" ");
}

function renderModalLabelRow() {
  return `
              <div class="modal-label-row" aria-hidden="true">
                <div class="modal-label-cell modal-label-time">time</div>
                <div class="modal-label-cell modal-label-number">number</div>
                <div class="modal-label-cell modal-label-name">name</div>
                <div class="modal-label-cell modal-label-order">order</div>
                <div class="modal-label-cell modal-label-starts">starts-or-ends</div>
                <div class="modal-label-cell modal-label-leave">leave</div>
              </div>`;
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
      <span class="rollup-cell rollup-cell--time">${renderAlignedTimeText(rollup.time)}</span>
      <span class="rollup-cell rollup-cell--order">${renderAlignedOrderText(order)}</span>
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
  const sequenceClass = isDoneRow(row) ? "" : modifierClass("sequence-shade", row.schedule_sequence_type_shade);
  const classTypeClass = modifierClass("class-type-shade", row.class_type);
  return `
    <article class="class-card ${rowBandClass(row)}" ${rowDataAttrs(row)}>
      <div class="schedule-line class-line">
        <div class="time-col c-time ${htmlEscape(timeClass)}">${renderTimeValue(row.time)}</div>
        <div class="ring-num-col"><span class="slot-token ring-token">${renderCellValue(row.ring_abbrev || row.ring_number)}</span></div>
        <div class="class-num-col"><span class="slot-token class-num-token">${renderCellValue(row.class_number)}</span></div>
        <div class="class-name-col c-name ${htmlEscape(sequenceClass)}">${renderCellValue(row.class_name)}</div>
        <div class="class-type-col"><span class="slot-token cell-token c-type ${htmlEscape(classTypeClass)}">${renderCellValue(row.class_type)}</span></div>
      </div>
      ${(row.rollups || []).length ? `<div class="rollup-line">${row.rollups.map(renderRollup).join("")}</div>` : ""}
    </article>`;
}

function renderClassLine(row, overrides = {}) {
  const timeClass = modifierClass("time-status", row.status_id);
  const sequenceClass = isDoneRow(row) ? "" : modifierClass("sequence-shade", row.schedule_sequence_type_shade);
  const classTypeClass = modifierClass("class-type-shade", row.class_type);
  const keyClass = overrides.keyClass || "class-num-token";
  const finalCell = overrides.finalAsClassType
    ? `<span class="slot-token cell-token c-type ${htmlEscape(classTypeClass)}">${renderCellValue(overrides.finalValue ?? row.class_type)}</span>`
    : `<span class="modal-metric">${renderCellValue(overrides.finalValue)}</span>`;
  return `
      <div class="schedule-line class-line modal-class-line">
        <div class="time-col c-time ${htmlEscape(timeClass)}">${renderTimeValue(overrides.time ?? row.time)}</div>
        <div class="modal-key-col"><span class="slot-token ${htmlEscape(keyClass)}">${renderCellValue(overrides.keyValue)}</span></div>
        <div class="class-name-col c-name ${overrides.nameSpanMetrics ? "modal-name-span" : ""} ${htmlEscape(sequenceClass)}">${renderCellValue(overrides.className ?? row.class_name)}</div>
        ${overrides.nameSpanMetrics ? "" : `<div class="modal-metric-col"><span class="modal-metric">${renderCellValue(overrides.metricOne)}</span></div>
        <div class="modal-metric-col"><span class="modal-metric">${renderCellValue(overrides.metricTwo)}</span></div>`}
        <div class="class-type-col">${finalCell}</div>
      </div>`;
}

function renderTimeRow(row) {
  const timeClass = modifierClass("time-status", row.status_id);
  const sequenceClass = isDoneRow(row) ? "" : modifierClass("sequence-shade", row.schedule_sequence_type_shade);
  const classTypeClass = modifierClass("class-type-shade", row.class_type);
  return `
    <article class="schedule-line time-line ${rowBandClass(row)}" ${rowDataAttrs(row)}>
      <div class="time-col c-time ${htmlEscape(timeClass)}">${renderTimeValue(row.time)}</div>
      <div class="ring-num-col"><span class="slot-token ring-token">${renderCellValue(row.ring_abbrev || row.ring_number)}</span></div>
      <div class="class-num-col"><span class="slot-token class-num-token">${renderCellValue(row.class_number)}</span></div>
      <div class="class-name-col c-name ${htmlEscape(sequenceClass)}">${renderCellValue(row.class_name)}</div>
      <div class="class-type-col"><span class="slot-token cell-token c-type ${htmlEscape(classTypeClass)}">${renderCellValue(row.class_type)}</span></div>
      ${(row.rollups || []).length ? `<div class="time-rollup-cell">${row.rollups.map(renderRollup).join("")}</div>` : ""}
    </article>`;
}

function renderClassOverviewModal(row) {
  const tripLines = (row.rollups || []).slice(0, 2).map((rollup) => `
              ${renderClassLine(row, {
                time: rollup.time,
                keyValue: rollup.entry_number,
                className: [rollup.horse || rollup.name, rollup.rider].filter((value) => !isBlank(value)).join(" + "),
                metricOne: renderOrderOfGoValue(rollup),
                metricTwo: rollup.starts_in || rollup.in,
                finalValue: rollup.leave_in || rollup.walk,
              })}`).join("");
  return `
    <section class="modal-preview" aria-label="Class overview modal visual">
      <div class="overview-modal">
        <div class="modal-head">
          <div class="${statusClass(row.status_id)}">${renderCellValue(row.status)}</div>
          <div class="modal-title">Class Overview</div>
          <button class="modal-action modal-action--icon modal-action--quiet" type="button" aria-label="Close">
            <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
              <path d="M4.25 4.25l7.5 7.5M11.75 4.25l-7.5 7.5"></path>
            </svg>
          </button>
        </div>
        <div class="modal-body">
          <div class="modal-output-list" aria-label="Expanded class overview outputs">
            <div class="modal-output-section">
              <div class="modal-output-label">RING</div>
              ${renderModalLabelRow()}
              ${renderClassLine(row, {
                keyValue: row.ring_number,
                keyClass: "ring-token",
                className: renderRingNameValue(row),
                metricOne: row.trips,
                metricTwo: row.gone,
                finalValue: row.left,
              })}
            </div>

            <div class="modal-output-section">
              <div class="modal-output-label">GROUP</div>
              ${renderModalLabelRow()}
              ${renderClassLine(row, {
                keyValue: row.class_number,
                className: row.class_name,
                nameSpanMetrics: true,
                finalValue: row.class_type,
                finalAsClassType: true,
              })}
            </div>

            <div class="modal-output-section">
              <div class="modal-output-label">TRIPS</div>
              ${renderModalLabelRow()}
              ${tripLines}
            </div>
          </div>
        </div>
        <div class="modal-actions" aria-label="Class overview actions">
          <button class="rail-button" type="button">Save to Thread</button>
          <button class="rail-button" type="button">Share</button>
        </div>
      </div>
    </section>`;
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
          <span class="rollup-switch__track" aria-hidden="true">
            <span class="rollup-switch__thumb"></span>
          </span>
          <span class="rollup-switch__label">Team</span>
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
  const classOverviewModal = renderClassOverviewModal(model.sampleRows[0]);
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
      --blue-muted: #b6c8ee;
      --blue-bg: rgba(73, 118, 255, .16);
      --ring: #a8c7ff;
      --ring-bg: rgba(92, 142, 255, .11);
      --violet: #c386ff;
      --violet-bg: rgba(156, 89, 255, .18);
      --teal: #58dac7;
      --teal-bg: rgba(88, 218, 199, .16);
      --amber: #f2c15c;
      --amber-bg: rgba(242, 193, 92, .16);
      --red: #ff8d9a;
      --red-bg: rgba(255, 141, 154, .18);
      --token-radius: 6px;
      --schedule-cols: minmax(8ch, 8ch) 4.5ch 4ch minmax(0, 1fr) 4ch;
      --modal-overview-cols: minmax(8ch, 8ch) 6ch minmax(0, 1fr) 5ch 6ch 6ch;
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
    .rollup-switch.is-active {
      color: #b6f0c8;
      border-color: rgba(73, 209, 125, .34);
      background: rgba(73, 209, 125, .1);
    }
    .rollup-switch.is-active .rollup-switch__thumb {
      transform: translateX(15px);
      background: var(--green);
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
      background: var(--blue-bg);
      border-color: rgba(143, 184, 255, .24);
      color: var(--blue);
    }
    .ring-status-controls .rail-button[data-status-filter="NEXT"].is-active {
      background: rgba(73, 118, 255, .18);
      border-color: rgba(143, 184, 255, .42);
    }
    .ring-status-controls .rail-button[data-status-filter="DONE"] {
      background: rgba(154, 163, 180, .12);
      border-color: rgba(154, 163, 180, .16);
      color: #d4d9e6;
    }
    .ring-status-controls .rail-button[data-status-filter="DONE"].is-active {
      background: rgba(154, 163, 180, .14);
      border-color: rgba(154, 163, 180, .3);
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
      color: var(--blue-muted);
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
    .shade--blue_muted { color: var(--blue-muted); background: rgba(143, 184, 255, .08); border: 1px solid rgba(143, 184, 255, .18); }
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
    .modal-preview {
      background: rgba(3, 6, 14, .86);
      border: 1px solid rgba(143, 184, 255, .16);
      border-radius: 8px;
      padding: 8px;
      box-shadow: 0 18px 42px rgba(0, 0, 0, .38);
    }
    .overview-modal {
      overflow: hidden;
      border-radius: 8px;
      border: 1px solid var(--line);
      background: var(--panel);
    }
    .modal-head {
      min-height: 38px;
      display: grid;
      grid-template-columns: minmax(56px, max-content) minmax(0, 1fr) minmax(78px, max-content);
      align-items: center;
      gap: 8px;
      padding: 7px 8px;
      border-bottom: 1px solid var(--line);
    }
    .modal-title {
      min-width: 0;
      text-align: center;
      font-size: 12px;
      font-weight: 620;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .modal-action {
      min-height: 24px;
      border-radius: var(--token-radius);
      border: 1px solid rgba(143, 184, 255, .22);
      background: rgba(73, 118, 255, .12);
      color: var(--blue);
      padding: 3px 7px;
      font: inherit;
      font-size: 10px;
      font-weight: 560;
      white-space: nowrap;
    }
    .modal-action--quiet {
      color: var(--muted);
      background: rgba(154, 163, 180, .08);
      border-color: rgba(154, 163, 180, .16);
    }
    .modal-action--icon {
      width: 24px;
      min-width: 24px;
      padding: 0;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }
    .modal-action--icon svg {
      width: 12px;
      height: 12px;
      fill: none;
      stroke: currentColor;
      stroke-width: 1.8;
      stroke-linecap: round;
    }
    .modal-body .class-card {
      border-bottom: 1px solid var(--line);
    }
    .modal-body .class-card::before {
      inset: 3px;
    }
    .modal-output-list {
      display: grid;
      gap: 6px;
      padding: 7px 10px 8px;
      border-bottom: 1px solid var(--line);
    }
    .modal-label-row {
      display: grid;
      grid-template-columns: var(--modal-overview-cols);
      column-gap: 3px;
      align-items: center;
      color: var(--faint);
      font-size: 12px;
      font-weight: 560;
      line-height: 1;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .modal-label-cell {
      min-width: 0;
      width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      font-size: 8px;
    }
    .modal-label-time,
    .modal-label-number,
    .modal-label-name,
    .modal-label-order,
    .modal-label-starts,
    .modal-label-leave { text-align: center; }
    .modal-output-section {
      display: grid;
      gap: 3px;
    }
    .modal-output-section .schedule-line {
      grid-template-columns: var(--modal-overview-cols);
      min-height: 22px;
      padding: 0;
    }
    .modal-output-label {
      position: absolute;
      width: 1px;
      height: 1px;
      margin: -1px;
      padding: 0;
      overflow: hidden;
      clip: rect(0 0 0 0);
      white-space: nowrap;
      border: 0;
      color: var(--faint);
      font-size: 9.5px;
      font-weight: 560;
      text-transform: uppercase;
    }
    .modal-output-line {
      min-width: 0;
      min-height: 21px;
      display: flex;
      align-items: center;
      gap: 0;
      color: var(--text);
      background: rgba(154, 163, 180, .08);
      border: 1px solid rgba(154, 163, 180, .12);
      border-radius: var(--token-radius);
      font-size: 10px;
      font-weight: 560;
      overflow: hidden;
      white-space: nowrap;
    }
    .modal-output-line--primary {
      color: var(--blue);
      background: rgba(73, 118, 255, .1);
      border-color: rgba(143, 184, 255, .18);
    }
    .modal-output-line span {
      min-width: 0;
      display: inline-flex;
      align-items: center;
      padding: 0 7px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      line-height: 1;
      border-left: 1px solid rgba(182, 200, 238, .22);
    }
    .modal-output-line span:first-child {
      border-left: 0;
    }
    .modal-output-strong {
      color: var(--blue-muted);
    }
    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 6px;
      padding: 7px 8px;
    }
    .modal-actions .rail-button {
      min-height: 24px;
      padding: 3px 7px;
      font-size: 10px;
      font-weight: 560;
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
    .class-card.schedule-band {
      display: grid;
      row-gap: 3px;
      padding: 8px 10px;
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
      column-gap: 3px;
      row-gap: 3px;
      align-items: center;
      min-height: 38px;
      padding: 8px 10px;
    }
    .class-card .schedule-line {
      min-height: 22px;
      padding: 0;
    }
    .time-line {
      border-bottom: 1px solid var(--line);
    }
    .time-line:last-child { border-bottom: 0; }
    .ring-num-col,
    .class-num-col,
    .modal-metric-col,
    .class-type-col {
      min-width: 0;
      width: 100%;
      overflow: hidden;
      white-space: nowrap;
    }
    .time-col {
      min-width: 0;
      width: 100%;
      justify-self: stretch;
      display: grid;
      grid-template-columns: 11px minmax(6ch, 6ch);
      align-items: center;
      justify-content: end;
      column-gap: 3px;
      text-align: right;
      overflow: visible;
      padding: 0;
      min-height: 22px;
      height: 22px;
      white-space: nowrap;
    }
    .time-clock {
      width: 11px;
      height: 11px;
      justify-self: center;
      opacity: .72;
      color: currentColor;
      overflow: visible;
    }
    .time-clock svg {
      display: block;
      width: 11px;
      height: 11px;
      min-width: 11px;
      fill: none;
      stroke: currentColor;
      stroke-width: 1.6;
      stroke-linecap: round;
      stroke-linejoin: round;
      overflow: visible;
    }
    .time-value {
      min-width: 0;
      display: block;
      text-align: right;
      overflow: visible;
      white-space: nowrap;
    }
    .c-time, .c-num, .c-type, .trip-metric {
      font-size: 10px;
      font-weight: 560;
      color: var(--muted);
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .c-time {
      font-family: "Roboto Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      font-weight: 560;
      letter-spacing: 0;
      line-height: 1.35;
      padding: 0;
      background: transparent;
      border: 0;
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
      min-height: 22px;
      height: 22px;
      display: flex;
      align-items: center;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 11px;
      font-weight: 560;
      padding-left: 3px;
    }
    .modal-name-span {
      grid-column: span 3;
    }
    .class-type-col {
      display: inline-flex;
      justify-content: center;
      align-items: center;
    }
    .modal-metric-col {
      display: inline-flex;
      justify-content: center;
      align-items: center;
    }
    .modal-key-col {
      min-width: 0;
      width: 100%;
      display: inline-flex;
      justify-content: center;
      align-items: center;
      overflow: hidden;
      white-space: nowrap;
    }
    .slot-token {
      width: 100%;
      min-width: 0;
      min-height: 20px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 1px 4px;
      border-radius: var(--token-radius);
      font-weight: 560;
      line-height: 1;
      overflow: hidden;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .cell-token,
    .trip-metric,
    .modal-metric {
      width: 100%;
      min-width: 0;
      min-height: 20px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 1px 4px;
      border-radius: var(--token-radius);
      font-size: 9.5px;
      font-weight: 560;
      line-height: 1;
      overflow: hidden;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .modal-metric {
      color: var(--muted);
      background: rgba(154, 163, 180, .06);
      border: 1px solid rgba(154, 163, 180, .1);
    }
    .class-num-token {
      color: var(--text);
      background: rgba(255, 255, 255, .06);
      border: 1px solid rgba(255, 255, 255, .12);
      font-size: 9.5px;
    }
    .ring-token {
      color: var(--ring);
      background: var(--ring-bg);
      border: 1px solid rgba(168, 199, 255, .2);
      font-size: 9.5px;
    }
    .c-type {
      color: var(--muted);
      background: rgba(154, 163, 180, .08);
      border: 1px solid rgba(154, 163, 180, .12);
    }
    .class-type-shade--hun {
      color: var(--teal);
      background: var(--teal-bg);
      border-color: rgba(88, 218, 199, .22);
    }
    .class-type-shade--eq {
      color: var(--violet);
      background: var(--violet-bg);
      border-color: rgba(195, 134, 255, .22);
    }
    .class-type-shade--jmp {
      color: var(--amber);
      background: var(--amber-bg);
      border-color: rgba(242, 193, 92, .22);
    }
    .time-status--now { color: var(--green); }
    .time-status--next { color: var(--blue); }
    .time-status--following { color: var(--blue-muted); }
    .time-status--upcoming { color: var(--muted); }
    .time-status--completed,
    .time-status--done { color: var(--text); }
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
      padding: 0;
      scrollbar-width: thin;
    }
    .time-rollup-cell {
      grid-column: 1 / -1;
      display: flex;
      justify-content: flex-end;
      gap: 6px;
      overflow-x: auto;
      padding-top: 0;
      scrollbar-width: thin;
    }
    .rollup-row {
      --rollup-cell-x: 7px;
      display: inline-grid;
      grid-template-columns: minmax(0, max-content) minmax(calc(6ch + (var(--rollup-cell-x) * 2)), calc(6ch + (var(--rollup-cell-x) * 2))) minmax(calc(5ch + (var(--rollup-cell-x) * 2)), calc(5ch + (var(--rollup-cell-x) * 2)));
      column-gap: 0;
      flex: 0 0 auto;
      align-items: stretch;
      box-sizing: border-box;
      min-height: 20px;
      height: 20px;
      border-radius: var(--token-radius);
      border: 1px solid rgba(154, 163, 180, .16);
      background: rgba(154, 163, 180, .1);
      color: var(--text);
      padding: 0;
      font-size: 9px;
      font-weight: 560;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .rollup-cell {
      min-width: 0;
      min-height: 18px;
      height: 100%;
      display: flex;
      align-items: center;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      line-height: 1;
      color: var(--muted);
      padding: 0 var(--rollup-cell-x);
    }
    .rollup-cell--horse {
      max-width: calc(8ch + (var(--rollup-cell-x) * 2));
      justify-content: flex-start;
      color: var(--blue-muted);
    }
    .rollup-cell--time,
    .rollup-cell--order {
      justify-content: center;
      text-align: center;
      width: 100%;
      min-width: 0;
      overflow: visible;
      text-overflow: clip;
    }
    .rollup-cell--time {
      font-family: "Roboto Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      font-weight: 560;
      color: #c2d2f2;
    }
    .rollup-cell--time,
    .rollup-cell--order {
      border-left: 1px solid rgba(182, 200, 238, .4);
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
    @media (max-width: 390px) {
      .modal-output-section .schedule-line {
        grid-template-columns: minmax(8ch, 8ch) 6ch 5ch 6ch 6ch;
        row-gap: 2px;
      }
      .modal-label-row {
        grid-template-columns: minmax(8ch, 8ch) 6ch 5ch 6ch 6ch;
      }
      .modal-label-name {
        display: none;
      }
      .modal-class-line .class-name-col {
        grid-column: 2 / -1;
        grid-row: 2;
        height: 18px;
        min-height: 18px;
        padding-left: 0;
      }
      .modal-name-span {
        grid-column: 2 / -1;
      }
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

        ${classOverviewModal}
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
