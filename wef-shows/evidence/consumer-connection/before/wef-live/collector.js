"use strict";
const crypto = require("node:crypto"),
  path = require("node:path");
const settings = require("./settings"),
  model = require("./model"),
  sources = require("./source");
const hash = (x) =>
  crypto.createHash("sha256").update(JSON.stringify(x)).digest("hex");
const clean = (row) =>
  Object.fromEntries(Object.entries(row).filter(([k]) => !k.startsWith("_")));
async function runLive({
  api = require("./airtable").airtable(),
  get = sources.get,
  clock = () => new Date().toISOString(),
  archive = (kind, rows, scope) =>
    require("../wef-tenants/archive").archiveFile(
      path.join(settings.RUNTIME, "history"),
      kind,
      rows,
      scope,
    ),
} = {}) {
  const report = {
    run_id: crypto.randomUUID(),
    started: clock(),
    ok: false,
    errors: [],
    warnings: [],
    archives: [],
    counts: { rings: 0, groups: 0, classes: 0, entries: 0 },
  };
  let scope,
    logReady = false;
  async function log(result) {
    await api.upsert("run-logs", "run-id", [
      {
        "run-id": report.run_id,
        started: report.started,
        completed: report.completed || "",
        result,
        "show-id": scope?.show ?? null,
        date: scope?.date || "",
        summary: report.summary || "Independent live collection started",
        errors: report.errors.join("\n"),
        "ring-count": report.counts.rings,
        "group-count": report.counts.groups,
        "class-count": report.counts.classes,
        "entry-count": report.counts.entries,
        "archive-count": report.archives.length,
        archives: report.archives
          .map(
            (a) =>
              `${a.at}: ${a.kind} ${a.outcome}; ${a.records} records; ${a.file}; SHA256 ${a.sha256}`,
          )
          .join("\n"),
      },
    ]);
  }
  async function keep(kind, rows) {
    let a;
    try {
      a = await archive(kind, rows, {
        shows: scope ? [scope.show] : [],
        dates: scope ? [scope.date] : [],
      });
    } catch (e) {
      e.auditFailure = true;
      throw e;
    }
    report.archives.push(a);
    await log("Running");
    return a;
  }
  try {
    await log("Running");
    logReady = true;
    scope = model.scope(
      await api.list("shows", settings.SCHEDULES_BASE),
      clock(),
    );
    report.scope = scope;
    // The existing schedules show is authoritative. Calendar equality is only an upcoming-day guard.
    if (!scope.active) {
      report.ok = true;
      report.summary =
        "Success: source-selected show day is not today; no live collection requested.";
    } else {
      let scheduleRows = null,
        binder = null;
      try {
        scheduleRows = await api.list("wef-schedules.schedules");
      } catch (e) {
        report.warnings.push(
          "Schedule reference unavailable; existing links retained: " +
            e.message,
        );
      }
      try {
        binder = await api.list("wef-entries.entries");
      } catch (e) {
        report.warnings.push(
          "Entry reference unavailable; existing links retained: " + e.message,
        );
      }
      const observed = clock(),
        groupPayload = await get(sources.endpoints.groups),
        widgetPayload = await get(sources.endpoints.widget);
      await keep("source-groups", groupPayload);
      await keep("source-widget", widgetPayload);
      const plan = model.groups(groupPayload, scope, observed);
      model.ringState(widgetPayload, scope, plan);
      const entries = [],
        datasets = [];
      for (const c of plan.classes) {
        if (!c["detail-available"]) continue;
        try {
          const [detail, decoded] = await Promise.all([
            get(sources.endpoints.detail(scope, c["class-id"])),
            get(sources.endpoints.decoded(scope, c["class-id"]), true),
          ]);
          await keep("class-" + c["class-id"], { detail, decoded });
          const normalized = model.classDetail(
            detail,
            decoded,
            scope,
            c,
            clock(),
          );
          Object.assign(c, normalized.fields);
          entries.push(...normalized.entries);
          datasets.push({
            kind: "class",
            id: c["class-id"],
            endpoint: sources.endpoints.detail(scope, c["class-id"]),
            rows: normalized.entries,
          });
        } catch (e) {
          if (e.auditFailure) throw e;
          report.errors.push("Class " + c["class-id"] + ": " + e.message);
        }
      }
      // A failed archive or identity check prevents replacement of that class. No cleanup is inferred.
      const old = {};
      for (const t of [
        "live-rings",
        "live-groups",
        "live-classes",
        "live-entries",
      ]) {
        old[t] = await api.list(t);
        await keep(t + "-before", old[t]);
      }
      datasets.unshift(
        {
          kind: "rings",
          id: scope.show,
          endpoint: sources.endpoints.widget,
          rows: plan.rings,
        },
        {
          kind: "groups",
          id: scope.show,
          endpoint: sources.endpoints.groups,
          rows: plan.groups,
        },
      );
      const collections = [];
      for (const d of datasets) {
        const event = await keep("dataset-" + d.kind + "-" + d.id, d.rows);
        collections.push({
          "collection-id": model.key(scope.show, scope.date, d.kind, d.id),
          "show-id": scope.show,
          date: scope.date,
          "source-kind": d.kind,
          "source-id": String(d.id),
          endpoint: d.endpoint,
          started: report.started,
          completed: clock(),
          status: "complete",
          "row-count": d.rows.length,
          fingerprint: hash(d.rows),
          "archive-file": event.file,
          message: d.rows.length
            ? "Validated source data"
            : "Success: zero rows",
          errors: "",
        });
      }
      const savedCollections = await api.upsert(
        "collections",
        "collection-id",
        collections,
      );
      const collection = (kind, id) => [
        savedCollections.find(
          (r) =>
            r.fields["collection-id"] ===
            model.key(scope.show, scope.date, kind, id),
        ).id,
      ];
      const ringRows = await api.upsert(
        "live-rings",
        "ring-key",
        plan.rings.map((r) => ({
          ...clean(r),
          collection: collection("rings", scope.show),
        })),
      );
      const ringId = (k) => ringRows.find((r) => r.fields["ring-key"] === k).id;
      const groupRows = await api.upsert(
        "live-groups",
        "group-key",
        plan.groups.map((g) => ({
          ...clean(g),
          ring: [ringId(g._ring)],
          collection: collection("groups", scope.show),
        })),
      );
      const groupId = (k) =>
        groupRows.find((r) => r.fields["group-key"] === k).id;
      const schedules = (c) => {
        const matches = (scheduleRows || []).filter(
          (r) =>
            Number(r.fields["show-id"]) === scope.show &&
            r.fields.date === scope.date &&
            Number(r.fields["class-id"]) === c["class-id"],
        );
        if (matches.length !== 1) {
          report.warnings.push(
            "Class " +
              c["class-id"] +
              ": " +
              matches.length +
              " schedule matches",
          );
          return [];
        }
        return [matches[0].id];
      };
      const classRows = await api.upsert(
        "live-classes",
        "class-key",
        plan.classes.map((c) => ({
          ...clean(c),
          ring: [ringId(c._ring)],
          groups: c._groups.map(groupId),
          ...(scheduleRows ? { schedules: schedules(c) } : {}),
          collection: datasets.some(
            (d) => d.kind === "class" && d.id === c["class-id"],
          )
            ? collection("class", c["class-id"])
            : collection("groups", scope.show),
        })),
      );
      const classId = (k) =>
        classRows.find((r) => r.fields["class-key"] === k).id;
      await api.upsert(
        "live-groups",
        "group-key",
        plan.groups.map((g) => ({
          "group-key": g["group-key"],
          classes: g._classes.map(classId),
          schedules: [
            ...new Set(
              g._classes.flatMap(
                (k) =>
                  classRows.find((r) => r.fields["class-key"] === k).fields
                    .schedules || [],
              ),
            ),
          ],
        })),
      );
      await api.upsert(
        "live-rings",
        "ring-key",
        plan.rings.map((r) => ({
          "ring-key": r["ring-key"],
          "current-group": r._current.map(groupId),
          "next-group": r._next.map(groupId),
          "completed-groups": r._completed.map(groupId),
        })),
      );
      await api.upsert(
        "live-entries",
        "live-entry-key",
        entries.map((e) => {
          const matches = (binder || []).filter(
            (r) => r.fields["entry-key"] === e["entry-key"],
          );
          if (matches.length > 1)
            report.warnings.push("Ambiguous binder " + e["entry-key"]);
          return {
            ...clean(e),
            class: [classId(e._class)],
            ...(binder
              ? { entry: matches.length === 1 ? [matches[0].id] : [] }
              : {}),
            collection: collection("class", Number(e._class.split("|")[2])),
          };
        }),
      );
      // Retirement occurs only after all source and identity collections succeeded and archives were verified.
      if (!report.errors.length && api.retire)
        for (const table of [
          "live-entries",
          "live-classes",
          "live-groups",
          "live-rings",
        ]) {
          const prior = old[table].filter(
            (r) => r.fields.date && r.fields.date < scope.date,
          );
          if (prior.length) {
            const receipt = await keep("retired-" + table, prior);
            await api.retire(table, prior, scope.date, receipt);
          }
        }
      report.counts = {
        rings: plan.rings.length,
        groups: plan.groups.length,
        classes: plan.classes.length,
        entries: entries.length,
      };
      report.ok = !report.errors.length;
      report.summary = `Show ${scope.show}; ${scope.date}; ${plan.rings.length} rings, ${plan.groups.length} groups, ${plan.classes.length} classes, ${entries.length} live trips. ${report.warnings.join("; ")}`;
    }
  } catch (e) {
    report.errors.push(e.message);
    report.summary =
      "Collection stopped; failed source data did not authorize cleanup. " +
      e.message;
  }
  report.completed = clock();
  if (logReady)
    try {
      await log(report.ok ? "Success" : "Failed");
      await api.upsert("heartbeat", "heartbeat-key", [
        {
          "heartbeat-key": "wef-live",
          "checked-at": report.completed,
          "show-id": scope?.show ?? null,
          date: scope?.date || "",
          result: report.ok ? "Success" : "Failed",
          summary: report.summary,
          errors: report.errors.join("\n"),
        },
      ]);
    } catch (e) {
      report.ok = false;
      report.errors.push("Completion evidence: " + e.message);
    }
  return report;
}
module.exports = { runLive };
