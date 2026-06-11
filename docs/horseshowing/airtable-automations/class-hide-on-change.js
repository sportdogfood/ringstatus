/*
Trigger: class_hide record created or updated
Input variable: recordId = Airtable record ID from class_hide
*/

const { recordId } = input.config();

const CATALYST_ENDPOINT = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

function clean(value) {
  return value == null ? "" : String(value).trim();
}

function boolValue(value) {
  return value === true || value === "1" || String(value).toLowerCase() === "true";
}

function classNoValue(record) {
  return clean(record.getCellValue("class_no")).replace(/\.0$/, "");
}

function hideTextValue(record) {
  return clean(record.getCellValueAsString("hide_text") || record.getCellValueAsString("hide_lib"));
}

function showNoValue(record) {
  return clean(record.getCellValue("show_no")).replace(/\.0$/, "");
}

function ruleKey(showNo, classNo, hideText) {
  if (classNo) return `${showNo}|class_no:${classNo}`;
  return `${showNo}|text:${hideText.toLowerCase()}`;
}

function addRule(rules, classNo, hideText) {
  if (classNo) rules.push(`class_no:${classNo}`);
  if (hideText) rules.push(`text:${hideText}`);
}

const classHideTable = base.getTable("class_hide");
const focusShowTable = base.getTable("focus_show");

const changedRecord = await classHideTable.selectRecordAsync(recordId);
if (!changedRecord) throw new Error(`class_hide record not found: ${recordId}`);

const showNo = showNoValue(changedRecord);
if (!showNo) throw new Error("class_hide.show_no is required");

const classNo = classNoValue(changedRecord);
const hideText = hideTextValue(changedRecord);
if (!classNo && !hideText) throw new Error("class_hide requires class_no or hide_text/hide_lib");
const classHideKey = ruleKey(showNo, classNo, hideText);

if (changedRecord.getCellValueAsString("class_hide_key") !== classHideKey) {
  await classHideTable.updateRecordAsync(changedRecord.id, {
    class_hide_key: classHideKey
  });
}

const focusQuery = await focusShowTable.selectRecordsAsync({
  fields: ["show_no", "focus_day"]
});
const focusRecord = focusQuery.records.find((record) => showNoValue(record) === showNo);
if (!focusRecord) throw new Error(`focus_show record not found for show_no ${showNo}`);

const focusDay = clean(focusRecord.getCellValueAsString("focus_day")).slice(0, 10);
if (!focusDay) throw new Error(`focus_show.focus_day is required for show_no ${showNo}`);

const hideQuery = await classHideTable.selectRecordsAsync({
  fields: ["show_no", "class_no", "hide_text", "hide_lib", "active"]
});

const rules = [];
for (const record of hideQuery.records) {
  if (showNoValue(record) !== showNo) continue;
  if (!boolValue(record.getCellValue("active"))) continue;
  addRule(rules, classNoValue(record), hideTextValue(record));
}

const uniqueRules = [...new Set(rules.filter(Boolean))];
const params = new URLSearchParams({
  action: "set-hide-classes",
  show_no: showNo,
  focus_day: focusDay,
  hide_classes: uniqueRules.join("|")
});

const response = await fetch(`${CATALYST_ENDPOINT}?${params.toString()}`);
const body = await response.text();
if (!response.ok) {
  throw new Error(`Catalyst set-hide-classes failed ${response.status}: ${body}`);
}

console.log(JSON.stringify({
  ok: true,
  show_no: showNo,
  focus_day: focusDay,
  class_hide_key: classHideKey,
  hide_rules: uniqueRules.length
}));
