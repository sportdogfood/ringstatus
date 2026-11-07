/**
 * Independent Airtable schedule-priority evaluation.
 * No source calls or collector/linker/cleanup integration.
 * Writes only rs-lever, rs-flag and rs-priority in schedules.
 */
const schedules = base.getTable("schedules");
const classes = base.getTable("classes");
const [scheduleQuery, classQuery] = await Promise.all([
  schedules.selectRecordsAsync({fields: ["schedule-key", "kind", "ring", "group", "class", "source-json", "rs-lever", "rs-flag", "rs-priority"]}),
  classes.selectRecordsAsync({fields: ["entry-count"]})
]);
const byClass = new Map(classQuery.records.map(r => [r.id, r]));
const text = value => String(value ?? "").trim();
const tripTag = n => ["", "First trip", "Second trip", "Third trip", "Fourth trip", "Fifth trip"][n] || `Trip ${n}`;
const plans = [];
const groups = new Map();
for (const rec of scheduleQuery.records) {
  if (rec.getCellValue("kind") !== "class") continue;
  let source;
  try { source = JSON.parse(rec.getCellValue("source-json") || "{}"); }
  catch (err) { console.error(`${rec.id}: invalid source-json: ${err.message}`); continue; }
  const name = text(source.class_name).toLowerCase();
  const type = text(source.class_type).toLowerCase();
  const sequence = text(source.schedule_sequencetype).toLowerCase();
  const linkedClass = byClass.get(rec.getCellValue("class")?.[0]?.id);
  const count = linkedClass?.getCellValue("entry-count");
  let flag = null;
  // Category takes precedence over zero entries when both apply.
  if (/\bhandy\b/.test(name)) flag = "Handy";
  else if (/\bwarm[ -]?up\b/.test(name)) flag = "Warmup";
  else if (/\bschooling pony\b/.test(name)) flag = "Schooling pony";
  else if (/\bmulligan\b/.test(name)) flag = "Mulligan";
  else if (/\badd[ _-]back\b/.test(name)) flag = "Add back";
  else if (sequence === "model") flag = "Model";
  else if (/\bclassic\b/.test(name) && ["hunter", "hunters"].includes(type)) flag = "Classic";
  else if (count === 0) flag = "No entries";
  const plan = {rec, fields: {"rs-lever": flag ? "lever2" : "lever1", "rs-flag": flag, "rs-priority": null}};
  if (flag) { plans.push(plan); continue; }
  if (sequence === "under saddle/flat") {
    plan.fields["rs-flag"] = "Flat";
    plan.fields["rs-priority"] = 9;
    plans.push(plan);
    continue;
  }
  const number = source.class_number;
  const ring = rec.getCellValue("ring")?.[0]?.id;
  const group = rec.getCellValue("group")?.[0]?.id;
  const keyParts = text(rec.getCellValue("schedule-key")).split("|");
  if (number === null || number === undefined || text(number) === "" || !Number.isFinite(Number(number)) || !ring || !group || keyParts.length < 5) {
    console.error(`${rec.id}: cannot number trip without numeric class_number and show/day/ring/group; other records continue`);
    continue;
  }
  plan.number = Number(number);
  const key = JSON.stringify([...keyParts.slice(0, 3), ring, group, text(source.class_name)]);
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(plan);
  plans.push(plan);
}
for (const group of groups.values()) {
  group.sort((a, b) => a.number - b.number || a.rec.id.localeCompare(b.rec.id));
  group.forEach((plan, index) => {
    plan.fields["rs-priority"] = index + 1;
    plan.fields["rs-flag"] = tripTag(index + 1);
  });
}
const updates = plans.filter(p => Object.entries(p.fields).some(([field, value]) => (p.rec.getCellValue(field) ?? null) !== value))
  .map(p => ({id: p.rec.id, fields: p.fields}));
let saved = 0;
for (let i = 0; i < updates.length; i += 50) {
  const batch = updates.slice(i, i + 50);
  try { await schedules.updateRecordsAsync(batch); saved += batch.length; }
  catch (err) { console.error(`Schedule priority batch: ${err.message}`); }
}
output.set("evaluated", plans.length);
output.set("updated", saved);
output.set("flags", plans.reduce((counts, plan) => {
  const flag = plan.fields["rs-flag"];
  counts[flag] = (counts[flag] || 0) + 1;
  return counts;
}, {}));

