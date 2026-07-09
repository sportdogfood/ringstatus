// Source reference:
// C:\Users\gombc\Downloads\wec_ag_ring_group_flyup_test.backup-20260705-140656.html
//
// Scope:
// Isolated focus behavior only. This is not the special full-width row shape,
// not the base row contract, not the hide function, and not a styling reference.
//
// Behavior shape:
// - focusMode is toggled by the Focus button.
// - focusHiddenRows is derived state, not saved state.
// - Rows are grouped by ring + time + class name.
// - Within each duplicate group, the lowest class_number/class_no is kept.
// - Remaining duplicate rows are added to focusHiddenRows and excluded by the
//   same hidden-row pipeline used by hide mode.

let focusMode = false;
let focusHiddenRows = new Set();

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

function effectiveHiddenRows() {
  return focusMode ? hiddenUnion(currentHiddenSet(), focusHiddenRows) : currentHiddenSet();
}

function applyFocusToRows() {
  recomputeFocusHiddenRows();

  const showHidden = shouldIncludeHiddenRows();
  const hiddenSet = effectiveHiddenRows();

  visibleRings = allRings
    .map(ring => ({
      ...ring,
      classes: ring.classes.filter(row => showHidden || !hiddenSet.has(row.row_key))
    }))
    .filter(ring => ring.classes.length);

  visibleRows = flattenRingRows(visibleRings);
}

function toggleFocusMode() {
  focusMode = !focusMode;
  document.getElementById("focusModeBtn").setAttribute("aria-pressed", String(focusMode));
  applyFilters();
}
