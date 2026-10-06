#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const SCHEMA_URL = new URL("../config/rs-inputs-schema.json", import.meta.url);
const PROTECTED_BASES = new Set(["appZahVgD156cMAe3", "apptdhhNzduxm5gjn", "app6XS1RvsPNRT6os"]);

export async function loadSchema() { return JSON.parse(await readFile(SCHEMA_URL, "utf8")); }

// This command has no existing-base mode. Only the returned freshly created base
// may receive the required recognition link fields, in this same invocation.
export async function provision({ workspaceId, token, name = "RingStatus User Inputs Trial", apply = false, fetchImpl = fetch, onProgress = () => {} } = {}) {
  if (!/^wsp[A-Za-z0-9]+$/.test(workspaceId || "")) throw new Error("An explicit Airtable workspace ID is required.");
  if (typeof token !== "string" || !token.trim()) throw new Error("An explicit token environment variable is required; never put tokens in CLI arguments.");
  if (!/^RingStatus User Inputs Trial(?:[ -].*)?$/.test(name)) throw new Error("Base name must begin with RingStatus User Inputs Trial.");
  const schema = await loadSchema();
  const plan = { mode: apply ? "apply" : "dry-run", workspaceId, name, tables: schema.tables, linkFields: schema.linkFields, relationships: schema.relationships, limitations: schema.limitations };
  if (!apply) return plan;
  const headers = { Authorization: `Bearer ${token}`, "Content-Type": "application/json" };
  const completedLinks = [];
  let createdBaseId;
  let lastRequestAt = 0;
  async function post(path, payload) {
    // Stay below Airtable's per-base five requests/second limit. Other clients
    // can still cause throttling; partial progress is reported, never replayed.
    const pause = Math.max(0, 225 - (Date.now() - lastRequestAt));
    if (pause) await new Promise(resolve => setTimeout(resolve, pause));
    lastRequestAt = Date.now();
    let response;
    try {
      response = await fetchImpl(`https://api.airtable.com/v0/meta/${path}`, { method: "POST", redirect: "error", headers, body: JSON.stringify(payload) });
    } catch {
      throw new Error("Airtable write outcome unknown; do not rerun creation without inspecting the workspace.");
    }
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(`Airtable returned HTTP ${response.status}; no automatic write retry was attempted.`);
    return result;
  }
  try {
    const base = await post("bases", { workspaceId, name, tables: schema.tables });
    if (!/^app[A-Za-z0-9]+$/.test(base.id || "") || PROTECTED_BASES.has(base.id)) throw new Error("Refusing link-field writes: new base identity is invalid or protected.");
    createdBaseId = base.id;
    onProgress({ stage: "base-created", baseId: createdBaseId });
    const tables = new Map((base.tables || []).map(table => [table.name, table.id]));
    for (const table of schema.tables) if (!/^tbl[A-Za-z0-9]+$/.test(tables.get(table.name) || "")) throw new Error(`Missing created table identity: ${table.name}`);
    for (const link of schema.linkFields) {
      const tableId = tables.get(link.table);
      const linkedTableId = tables.get(link.target);
      await post(`bases/${createdBaseId}/tables/${tableId}/fields`, { name: link.name, type: "multipleRecordLinks", options: { linkedTableId, prefersSingleRecordLink: link.single } });
      completedLinks.push(`${link.table}.${link.name}`);
    }
    return { mode: "apply", baseId: createdBaseId, tables: Object.fromEntries(tables), completedLinks, runtimeEnvironment: { RS_INPUTS_BASE_ID: createdBaseId, RS_INPUTS_WRITE_MODE: "isolated-trial" }, limitations: schema.limitations };
  } catch (error) {
    if (createdBaseId) error.partial = { baseId: createdBaseId, completedLinks, recovery: "Inspect the created base and complete missing fields; rerunning this command creates another base." };
    throw error;
  }
}

export async function main(argv = process.argv.slice(2), env = process.env) {
  const options = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--apply") { options.apply = true; continue; }
    if (key === "--dry-run") { if (options.apply) throw new Error("Choose only one mode."); options.dryRun = true; continue; }
    if (!["--workspace", "--token-env", "--name"].includes(key) || !argv[i + 1] || argv[i + 1].startsWith("--")) throw new Error(`Unsupported or incomplete argument: ${key}`);
    options[key.slice(2)] = argv[++i];
  }
  if (options.apply && options.dryRun) throw new Error("Choose only one mode.");
  if (!/^[A-Z][A-Z0-9_]*$/.test(options["token-env"] || "")) throw new Error("Pass --token-env with the name of the explicitly supplied credential variable.");
  return provision({ workspaceId: options.workspace, token: env[options["token-env"]], name: options.name, apply: !!options.apply, onProgress: value => process.stderr.write(`${JSON.stringify(value)}\n`) });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().then(result => process.stdout.write(`${JSON.stringify(result, null, 2)}\n`)).catch(error => {
    process.stderr.write(`${JSON.stringify({ error: error.message, ...(error.partial ? { partial: error.partial } : {}) })}\n`);
    process.exitCode = 1;
  });
}
