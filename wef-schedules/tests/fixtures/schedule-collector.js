const assert=require("node:assert/strict");
const { buildAirtableFieldMeta } = require("../../lib/scope_fields");
const NOW = "2026-10-15T14:00:00.000Z";
const names = ("schedule_key schedule_short show_id class_id class_group_id class_groupxclasses_id class_number class_name group_name ring_number day_number class_group_sequence class_type schedule_sequencetype estimated_start_time estimated_end_time total_trips completed_trips show_date scheduled_date schedule_show_datev2 schedule_show_display_datev2 schedule_show_display_date_dayv2 app_show_idv2 app_sql_datev2 app_dow_rawv2 shifted_to_next_dayv2 customer_id shows show focus_day heartbeat is_current_scope is_target scope_run_id scope_status inactive archive dropped_at is_gotcha run_tag record_state last_seen_at last_updated_at latest_ingested_at manual_time_override ignore").split(" ");
const meta = buildAirtableFieldMeta(names.map(name => ({name, type: "singleLineText"})));
function payload(day, name = "Class A") {
  return {show_id: 74, show_date: day, data: [{show_id:74, show_date:day, ring_number:2, class_group_id:8, class_groupxclasses_id:9, class_id:10, class_number:11, class_name:name, estimated_start_time:"09:00 AM"}]};
}
function fixture(options = {}) {
  const rows = structuredClone(options.rows || []);
  const calls = [];
  const io = {
    captureScheduleHtml: async () => '<tr class="class_group_row"><td><a href="?cgid=8&amp;ring=2">Class [11]</a></td><td>9:00 AM</td></tr>',
    fetchJson: async url => { calls.push(["source",url]); const raw = new URL(url).searchParams.get("date"); const day = raw === "00/00/00" ? null : raw; return options.fetch ? options.fetch(day, url) : payload(day || "2026-10-15"); },
    fetchClassListJson: async () => { throw Error("Unexpected class fetch"); },
    loadScheduleFallbackPayload: () => ({ok:false}),
    loadScheduleAttachmentPayload: async () => ({ok:false}),
    loadManualScheduleFallbackPayload: () => ({ok:false}),
    applyScheduleHtmlTimeOverlay: rows => ({rows,summary:{}}),
    fetchLiveGroupsRows: async () => [],
    fetchShowRecordId: async () => "recShows",
    fetchExistingRowsForShow: async (sid, opts) => { calls.push(["read",sid,opts]); return structuredClone(rows); },
    fetchHeartbeatViewRows: async () => { throw Error("Collector must not read heartbeat views"); },
    archivePayload: () => ({ok:true}),
    airtableCreateRecords: async (table, records) => {
      calls.push(["create",table,structuredClone(records)]);
      if(options.failWrite) return {okRows:0,failedRows:records};
      for(const r of records) rows.push({id:"rec"+(rows.length+1),fields:structuredClone(r.fields)});
      return {okRows:records.length,failedRows:[]};
    },
    airtablePatchRecords: async (table, records) => {
      assert.equal(table,"watch_schedule");
      calls.push(["patch",table,structuredClone(records)]);
      if(options.failWrite) return {okRows:0,failedRows:records};
      for(const r of records) Object.assign(rows.find(x=>x.id===r.id).fields,r.fields);
      return {okRows:records.length,failedRows:[]};
    },
    fetchTableFieldMeta: async () => meta,
    fetchWatchScheduleScopeStatusChoices: async () => new Set(["current","dropped"]),
    fetchShowTargetRows: async () => [{id:"recShow",fields:{show_id:74,customer_id:15,focus_day:"2026-10-15",end_date:"2026-10-17",heartbeat:true}}],
  };
  return {io,rows,calls};
}

module.exports={fixture,payload,NOW,meta};
