"use strict";
// Destination schema only. Does not invoke business collection or source endpoints.
const fs = require("node:fs"),
  path = require("node:path"),
  settings = require("./settings"),
  schema = require("./schema");
async function configure() {
  async function api(suffix = "", method = "GET", body) {
    await new Promise((r) => setTimeout(r, 260));
    const res = await fetch(
      `https://api.airtable.com/v0/meta/bases/${settings.BASE_ID}/tables${suffix}`,
      {
        method,
        headers: {
          Authorization: "Bearer " + settings.AIRTABLE_TOKEN,
          "Content-Type": "application/json",
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
        signal: AbortSignal.timeout(30000),
      },
    );
    if (!res.ok)
      throw Error(
        "Live schema " + method + " " + res.status + " " + (await res.text()),
      );
    return res.json();
  }
  let current = await api();
  for (const [name, fields] of Object.entries(schema.tables)) {
    let t = current.tables.find((t) => t.name === name);
    if (!t) {
      t = await api("", "POST", { name, fields: fields.map(schema.field) });
      current.tables.push(t);
    } else
      for (const n of fields) {
        const existing = t.fields.find(f=>f.name===n);
        if (!existing) await api('/'+t.id+'/fields','POST',schema.field(n));
        else if (existing.type!==schema.field(n).type) throw Error("Schema conflict " + name + "." + n);
      }
  }
  current = await api();
  const missing = [];
  for (const [table, fields] of Object.entries(schema.links))
    for (const [name, target] of Object.entries(fields)) {
      const t = current.tables.find((t) => t.name === table),
        dest = current.tables.find((t) => t.name === target);
      if (!dest) {
        missing.push(target);
        continue;
      }
      const old = t.fields.find((f) => f.name === name);
      if (old) {
        if (
          old.type !== "multipleRecordLinks" ||
          old.options.linkedTableId !== dest.id
        )
          throw Error("Link conflict " + table + "." + name);
      } else
        await api("/" + t.id + "/fields", "POST", {
          name,
          type: "multipleRecordLinks",
          options: { linkedTableId: dest.id },
        });
    }
  const after = await api();
  fs.writeFileSync(
    path.join(__dirname, "evidence/schema-configured.json"),
    JSON.stringify(after, null, 2),
  );
  console.log(
    JSON.stringify({
      tables: after.tables.map((t) => ({ name: t.name, id: t.id })),
      missingReferences: [...new Set(missing)],
      businessRecordsWritten: 0,
    }),
  );
}
if (require.main === module)
  configure().catch((e) => {
    console.error(e.message);
    process.exitCode = 1;
  });
module.exports = { configure };
