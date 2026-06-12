let { recordId } = input.config();

let trainersTable = base.getTable("trainers");
let focusTable = base.getTable("focus_show");

let trainerRecord = await trainersTable.selectRecordAsync(recordId);

if (!trainerRecord) {
  throw new Error(`Trainer record not found: ${recordId}`);
}

let trainerRecords = await trainersTable.selectRecordsAsync({
  fields: ["trainer", "trainer_display", "active"]
});

let activeTrainers = [];
let trainerDisplays = {};

for (let record of trainerRecords.records) {
  if (record.getCellValue("active") !== true) continue;

  let trainer = record.getCellValueAsString("trainer").trim();
  let trainerDisplay = record.getCellValueAsString("trainer_display").trim() || trainer;

  if (!trainer) continue;

  activeTrainers.push(trainer);
  trainerDisplays[trainer] = trainerDisplay;
}

let focusRecords = await focusTable.selectRecordsAsync({
  fields: ["show_no", "focus_day", "active"]
});

let focusRecord = focusRecords.records.find(record => {
  return record.getCellValue("active") === true;
});

if (!focusRecord && focusRecords.records.length === 1) {
  focusRecord = focusRecords.records[0];
}

if (!focusRecord) {
  throw new Error("No active focus_show record found");
}

let showNo = focusRecord.getCellValueAsString("show_no").trim();
let focusDay = focusRecord.getCellValueAsString("focus_day").trim();

if (!showNo || !focusDay) {
  throw new Error(`focus_show missing show_no or focus_day: ${focusRecord.id}`);
}

let params = new URLSearchParams({
  action: "set-active-trainers",
  show_no: showNo,
  focus_day: focusDay
});

let url = `https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/?${params.toString()}`;

let response = await fetch(url, {
  method: "POST",
  headers: {
    "Content-Type": "application/json"
  },
  body: JSON.stringify({
    active_trainers: activeTrainers.join("|"),
    trainer_displays: trainerDisplays
  })
});

let body = await response.text();

if (!response.ok) {
  throw new Error(`Catalyst failed ${response.status}: ${body}`);
}

console.log(body);
