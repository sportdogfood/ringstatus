function pickEpochMsFromJson(j) {
  if (j == null) return null;

  // direct number payload
  if (typeof j === "number") return j;

  // common top-level numeric keys
  const numKeys = ["epoch_ms", "server_epoch_ms", "now_ms", "time_ms", "server_now_ms"];
  for (const k of numKeys) {
    if (typeof j[k] === "number") return j[k];
    if (typeof j[k] === "string" && /^\d+$/.test(j[k])) return Number(j[k]);
  }

  // your payload: time_zone_date_time.date_obj (ISO string)
  const iso =
    j?.time_zone_date_time?.date_obj ||
    j?.time_zone_date_time?.time_obj ||
    j?.date_obj ||
    j?.time_obj;

  if (typeof iso === "string") {
    const ms = Date.parse(iso);
    if (Number.isFinite(ms)) return ms;
  }

  return null;
}
