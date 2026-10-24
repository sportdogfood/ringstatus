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
  getStatus = require('./activity').readStatus,
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
    unresolved: [],
    link_issues: [],
    warnings: [],
    logging_errors: [],
    archives: [],
    counts: { rings: 0, groups: 0, classes: 0, entries: 0 },
  };
  let scope;
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
    return a;
  }
  try {
    scope = model.scope(
      await api.list("shows", settings.SHOWS_BASE),
      clock(),
    );
    report.scope = scope;
    report.show_source = {base: settings.SHOWS_BASE, record: scope.authority, checked_at: scope.checked_at};
    // The canonical wef-shows record is authoritative. Calendar equality is only an upcoming-day guard.
    if (!scope.active) {
      report.ok = true;
      report.summary =
        "Success: source-selected show day is not today; no live collection requested.";
    } else {
      collection: {
      const liveStatus = require('./activity').statusValue(await getStatus(scope));
      await keep('source-live-status', {liveStatus});
      if (!liveStatus) {
        report.ok = true;
        report.skipped = 'live-status-false';
        report.summary = 'Skipped detailed collection: live status is false. Five-minute checks continue.';
        break collection;
      }
      const observed = clock(),
        groupPayload = await get(sources.endpoints.groups);
      await keep('source-groups', groupPayload);
      const plan = model.groups(groupPayload, scope, observed);
      if (require('./activity').allCompleted(plan.groups)) {
        report.ok = true;
        report.skipped = 'all-today-groups-completed';
        report.summary = `Skipped detailed collection: all ${plan.groups.length} groups for show ${scope.show} on ${scope.date} are completed. Five-minute checks continue.`;
        break collection;
      }
      let scheduleRows = null,
        binder = null, sourceSchedules = null, scheduleHealth = null, binderFields = null;
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
        binderFields = await api.fieldNames('wef-entries.entries');
      } catch (e) {
        report.warnings.push(
          "Entry reference unavailable; existing links retained: " + e.message,
        );
      }
      try {
        [sourceSchedules,scheduleHealth] = await Promise.all([
          api.list('schedules',settings.SCHEDULES_BASE), api.list('show-collection',settings.SCHEDULES_BASE),
        ]);
      } catch(e) { report.warnings.push('Participation schedule health unavailable: '+e.message); }
      const widgetPayload = await get(sources.endpoints.widget);
      await keep("source-widget", widgetPayload);
      model.ringState(widgetPayload, scope, plan);
      const entries = [],
        datasets = [];
      for (const c of plan.classes) {
        if (!c["detail-available"]) continue;
        try {
          let normalized;
          for (let attempt=0; attempt<2; attempt++) {
            const responses = await Promise.allSettled([
              get(sources.endpoints.detail(scope, c['class-id'])),
              get(sources.endpoints.decoded(scope, c['class-id']), true),
            ]);
            const [detail, decoded] = responses.map(r=>r.status==='fulfilled'?r.value:null);
            const sourceErrors = responses.filter(r=>r.status==='rejected').map(r=>r.reason?.message || String(r.reason));
            await keep('class-' + c['class-id'], {detail,decoded,...(sourceErrors.length?{sourceErrors}:{})});
            if (sourceErrors.length) throw Error(sourceErrors.join('; '));
            try { normalized=model.classDetail(detail,decoded,scope,c,clock());break; }
            catch(error) {
              if(error.code!=='LIVE_IDENTITY_SNAPSHOT_MISMATCH'||attempt===1)throw error;
              (report.snapshot_refreshes ||= []).push({class_id:c['class-id'],reason:error.message});
            }
          }
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
          report.unresolved.push({class_id:c['class-id'], message:e.message,
            status:e.code === 'LIVE_IDENTITY_SNAPSHOT_MISMATCH' ? 'unresolved' : 'error'});
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
      for (const item of report.unresolved) {
        const evidence = report.archives.filter(a => a.kind === 'class-' + item.class_id).at(-1);
        collections.push({
          'collection-id': model.key(scope.show, scope.date, 'class', item.class_id),
          'show-id': scope.show, date: scope.date, 'source-kind': 'class',
          'source-id': String(item.class_id), endpoint: sources.endpoints.detail(scope,item.class_id),
          started: report.started, completed: clock(), status: item.status,
          'archive-file': evidence.file,
          message: `${item.status === 'unresolved' ? 'Unresolved identity' : 'Class collection error'}; previous observations and links retained. Not a valid empty collection.`,
          errors: item.message,
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
      const schedules = (c, groupKeys = c._groups) => {
        const ids = [];
        for (const groupKey of groupKeys) {
          const prefix = `${groupKey}_`;
          const matches = (scheduleRows || []).filter(r =>
            r.fields.status === 'active' &&
            Number(r.fields['show-id']) === scope.show &&
            r.fields.date === scope.date &&
            Number(r.fields['class-id']) === c['class-id'] &&
            String(r.fields['stage-key'] || '').startsWith(prefix));
          if (matches.length !== 1) {
            report.warnings.push(`Class ${c['class-id']} group ${groupKey}: ${matches.length} schedule matches`);
          } else ids.push(matches[0].id);
        }
        return [...new Set(ids)];
      };
      const classRows = await api.upsert(
        "live-classes",
        "class-key",
        plan.classes.map((c) => report.unresolved.some(u => u.class_id === c['class-id']) &&
          old['live-classes'].some(r => r.fields['class-key'] === c['class-key'])
          ? {'class-key': c['class-key']} : ({
          ...clean(c),
          ring: [ringId(c._ring)],
          groups: c._groups.map(groupId),
          ...(scheduleRows ? { schedules: schedules(c) } : {}),
          collection: report.unresolved.some(u => u.class_id === c['class-id']) || datasets.some(
            (d) => d.kind === "class" && d.id === c["class-id"],
          )
            ? collection("class", c["class-id"])
            : collection("groups", scope.show),
        })),
      );
      const classId = (k) =>
        classRows.find((r) => r.fields["class-key"] === k).id;
      const preservedClassIds = new Set(old['live-classes']
        .filter(r=>report.unresolved.some(u=>u.class_id===r.fields['class-id']))
        .map(r=>r.id));
      await api.upsert(
        "live-groups",
        "group-key",
        plan.groups.map((g) => ({
          "group-key": g["group-key"],
          classes: [...new Set([
            ...g._classes.map(classId).filter(id=>!preservedClassIds.has(id)),
            ...(old['live-groups'].find(r=>r.fields['group-key']===g['group-key'])?.fields.classes || [])
              .filter(id=>preservedClassIds.has(id)),
          ])],
          ...(scheduleRows ? {schedules: [...new Set(g._classes.flatMap(k =>
            schedules(plan.classes.find(c => c['class-key'] === k), [g['group-key']])))]} : {}),
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
          const previous=old['live-entries'].find(r=>r.fields['live-entry-key']===e['live-entry-key'])?.fields || {};
          const resolved=require('./participation').resolve({entry:e,previous,binder,
            localSchedules:scheduleRows,sourceSchedules,health:scheduleHealth,
            arrayFieldPresent:binderFields?.includes('schedule-keys'),
            observationField:binderFields?.includes('last-seen')?'last-seen':null,
            observationStatusField:binderFields?.includes('observation-status')?'observation-status':null,now:clock()});
          if(resolved.issue)report.link_issues.push({entry_key:e['entry-key'],message:resolved.issue});
          return {
            ...clean(e),
            class: [classId(e._class)],
            ...resolved.fields,
            collection: collection("class", Number(e._class.split("|")[2])),
          };
        }),
      );
      const uncertain = old['live-entries'].filter(r=>r.fields.date===scope.date &&
        Number(r.fields['show-id'])===scope.show && report.unresolved.some(u=>u.class_id===Number(String(r.fields['entry-key']).split('|')[3])));
      if(uncertain.length) await api.upsert('live-entries','live-entry-key',uncertain.map(r=>({
        'live-entry-key':r.fields['live-entry-key'],
        'participation-link-status':r.fields.entry?.length?'historical':'unresolved',
        'participation-link-reason':'Live class collection unresolved; existing observation and links retained',
        'participation-checked-at':clock(),
      })));
      // Retirement occurs only after all source and identity collections succeeded and archives were verified.
      if (!report.errors.length && !report.unresolved.length && !report.link_issues.length && api.retire)
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
      if (report.unresolved.length) report.summary += ' Classes requiring attention: ' + report.unresolved.map(u=>`${u.class_id} (${u.status})`).join(', ') + '; existing observations and links retained.';
      if(report.link_issues.length) report.summary += ` ${report.link_issues.length} participation links unresolved/historical; observations continue.`;
      }
    }
  } catch (e) {
    report.errors.push(e.message);
    report.summary =
      "Collection stopped; failed source data did not authorize cleanup. " +
      e.message;
  }
  report.completed = clock();
    try {
      await log(report.ok ? (report.unresolved.length || report.link_issues.length ? "Partial" : report.skipped ? "Skipped" : "Success") : "Failed");
    } catch (e) { report.logging_errors.push('run-logs: ' + e.message); }
    try {
      await api.upsert("heartbeat", "heartbeat-key", [
        {
          "heartbeat-key": "wef-live",
          "checked-at": report.completed,
          "show-id": scope?.show ?? null,
          date: scope?.date || "",
          result: report.ok ? (report.unresolved.length || report.link_issues.length ? "Partial" : report.skipped ? "Skipped" : "Success") : "Failed",
          summary: report.summary,
          errors: report.errors.join("\n"),
        },
      ]);
    } catch (e) {
      report.logging_errors.push('heartbeat: ' + e.message);
    }
  return report;
}
module.exports = { runLive };
