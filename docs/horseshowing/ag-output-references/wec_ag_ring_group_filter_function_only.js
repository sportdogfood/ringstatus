// Source reference:
// C:\Users\gombc\Downloads\wec_ag_ring_group_flyup_test.backup-20260705-140656.html
//
// Scope:
// Isolated filter behavior only. This is not the special full-width row shape,
// not the base row contract, and not a styling reference.
//
// Important:
// The source file defines rowMatchesColumnFilters(), but its active
// applyFilters() path only applies hidden-row and horse filters.

let filterPanelOpen = false;
let activeHorseFilters = new Set();
let columnFilters = { time: "", ring: "", classText: "", entries: "", status: "" };
let pendingHiddenRows = new Set();
let showHiddenMode = false;
let hiddenRows = readHiddenRows();
let focusHiddenRows = new Set();
let visibleRings = [];
let visibleRows = [];

function horseKey(value) {
  return text(value).trim().toLowerCase();
}

function rowHorseKeys(row) {
  return new Set(
    (Array.isArray(row.horse_items) ? row.horse_items : [])
      .map(item => horseKey(item.name || item.label))
      .filter(Boolean)
  );
}

function rowMatchesHorseFilters(row) {
  if (!activeHorseFilters.size) return true;
  const keys = rowHorseKeys(row);
  for (const selected of activeHorseFilters) {
    if (keys.has(selected)) return true;
  }
  return false;
}

function rowMatchesColumnFilters(row) {
  const checks = [
    [columnFilters.time, row.time || ""],
    [columnFilters.ring, row.ring || ""],
    [columnFilters.classText, classLabel(row)],
    [columnFilters.entries, row.entry_count ?? ""],
    [columnFilters.status, statusReference(row).label]
  ];

  return checks.every(([needle, value]) =>
    !needle || text(value).toLowerCase().includes(text(needle).toLowerCase())
  );
}

function horseFilterOptions() {
  const options = new Map();
  const hiddenSet = effectiveHiddenRows();

  allRows.forEach(row => {
    if (hiddenSet.has(row.row_key) && !shouldIncludeHiddenRows()) return;

    (Array.isArray(row.horse_items) ? row.horse_items : []).forEach(item => {
      const label = text(item.name || item.label).trim();
      const key = horseKey(label);
      if (key && !options.has(key)) options.set(key, label);
    });
  });

  return Array.from(options, ([key, label]) => ({ key, label }))
    .sort((a, b) => a.label.localeCompare(b.label, undefined, { numeric: true }));
}

function currentHiddenSet() {
  return hideMode ? pendingHiddenRows : hiddenRows;
}

function effectiveHiddenRows() {
  return focusMode ? hiddenUnion(currentHiddenSet(), focusHiddenRows) : currentHiddenSet();
}

function shouldIncludeHiddenRows() {
  return hideMode ? true : showHiddenMode;
}

function focusGroupKey(row) {
  const timeKey = text(row.time || row.class_start_time).trim().toLowerCase() || text(row.sort_time).trim();
  return [
    row.ring_visual_key || row.ring_name_prioritized || row.ring || "",
    timeKey,
    text(row.class_name).trim().toLowerCase()
  ].join("|");
}

function focusKeepValue(row) {
  return numberOrNull(row.class_number) ?? numberOrNull(row.class_no) ?? 999999999;
}

function recomputeFocusHiddenRows() {
  const next = new Set();

  if (!focusMode) {
    focusHiddenRows = next;
    return;
  }

  const groups = new Map();

  allRows.forEach(row => {
    const key = focusGroupKey(row);
    if (!text(row.class_name).trim() || !text(row.time || row.class_start_time).trim()) return;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  });

  groups.forEach(rows => {
    if (rows.length < 2) return;

    const sorted = rows.slice().sort((a, b) => {
      const keep = focusKeepValue(a) - focusKeepValue(b);
      if (keep) return keep;
      return compareRows(a, b);
    });

    sorted.slice(1).forEach(row => next.add(row.row_key));
  });

  focusHiddenRows = next;
}

function applyFilters() {
  recomputeFocusHiddenRows();

  const showHidden = shouldIncludeHiddenRows();
  const hiddenSet = effectiveHiddenRows();

  visibleRings = allRings
    .map(ring => ({
      ...ring,
      classes: ring.classes.filter(row =>
        (showHidden || !hiddenSet.has(row.row_key)) &&
        rowMatchesHorseFilters(row)
      )
    }))
    .filter(ring => ring.classes.length);

  visibleRows = flattenRingRows(visibleRings);

  renderSchedule();
  renderBottomControls();
  updateHiddenButton();

  const horseText = activeHorseFilters.size ? " - horses " + activeHorseFilters.size : "";
  const pendingText = hideMode ? " - pending hidden " + pendingHiddenRows.size : "";
  const focusText = focusMode ? " - focus hidden " + focusHiddenRows.size : "";

  setStatus(
    metadataStatusPrefix() +
      " - rows " + visibleRows.length +
      " - hidden " + hiddenRows.size +
      focusText +
      horseText +
      pendingText,
    false
  );
}

function toggleHorseFilter(key) {
  if (!key) return;
  if (activeHorseFilters.has(key)) activeHorseFilters.delete(key);
  else activeHorseFilters.add(key);
  applyFilters();
}

function updateColumnFilter(key, value) {
  if (!(key in columnFilters)) return;
  columnFilters = { ...columnFilters, [key]: text(value).trim() };
  applyFilters();
}

function setFilterPanel(open) {
  filterPanelOpen = open;
  filterPanel.hidden = !open;
  filterBtn.setAttribute("aria-pressed", String(open));
}
