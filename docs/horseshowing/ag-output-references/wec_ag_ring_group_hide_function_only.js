// Source reference:
// C:\Users\gombc\Downloads\wec_ag_ring_group_flyup_test.backup-20260705-140656.html
//
// Scope:
// Isolated hide behavior only. This is not the special full-width row shape,
// not the base row contract, not a filter UI contract, and not a styling reference.
//
// Behavior shape:
// - hiddenRows is the saved localStorage state.
// - pendingHiddenRows is the edit/hide-mode working state.
// - hideMode shows hidden rows while editing so the user can toggle/save.
// - showHiddenMode shows saved hidden rows outside edit mode.
// - focusMode can add derived hidden rows through focusHiddenRows.

const HIDDEN_KEY = "rs-wec-ag-live-hidden";

let hideMode = false;
let focusMode = false;
let pendingHiddenRows = new Set();
let showHiddenMode = false;
let selectedRow = null;
let hiddenRows = readHiddenRows();
let focusHiddenRows = new Set();

function readHiddenRows() {
  try {
    return new Set(JSON.parse(localStorage.getItem(HIDDEN_KEY) || "[]"));
  } catch (_) {
    return new Set();
  }
}

function hiddenUnion(...sets) {
  const out = new Set();
  sets.forEach(set => set && set.forEach(value => out.add(value)));
  return out;
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

function gridRowClass(params) {
  const row = params.data || {};
  if (row.row_type === "ring") return "row-ring-group";

  const hiddenSet = effectiveHiddenRows();

  return [
    row.__zebra ? "row-zebra" : "",
    row.has_rollup ? "row-has-rollup" : "",
    row.status === "underway" ? "row-underway" : "",
    hideMode && hiddenSet.has(row.row_key) ? "row-pending-hidden" : "",
    !hideMode && showHiddenMode && hiddenSet.has(row.row_key) ? "row-saved-hidden" : ""
  ].filter(Boolean).join(" ");
}

function setHideMode(open) {
  hideMode = open;
  document.getElementById("hideModeBtn").setAttribute("aria-pressed", String(open));
  shell.classList.toggle("is-hide-mode", open);
  hidePanel.hidden = !open;

  if (open) {
    setFilterPanel(false);
    recomputeFocusHiddenRows();
    pendingHiddenRows = focusMode ? hiddenUnion(hiddenRows, focusHiddenRows) : new Set(hiddenRows);
  }

  applyFilters();
}

function togglePendingHidden(rowKey, checked) {
  if (!rowKey) return;
  if (checked) pendingHiddenRows.add(rowKey);
  else pendingHiddenRows.delete(rowKey);
  applyFilters();
}

function saveHideMode() {
  hiddenRows = new Set(pendingHiddenRows);
  localStorage.setItem(HIDDEN_KEY, JSON.stringify(Array.from(hiddenRows)));
  setHideMode(false);
}

function updateHiddenButton() {
  const isHidden = selectedRow && hiddenRows.has(selectedRow.row_key);
  hideRowBtn.disabled = !selectedRow;
  hideRowBtn.textContent = isHidden ? "Show" : "Hide";
}

function toggleRowHidden(row) {
  if (!row) return;

  if (hiddenRows.has(row.row_key)) hiddenRows.delete(row.row_key);
  else hiddenRows.add(row.row_key);

  localStorage.setItem(HIDDEN_KEY, JSON.stringify(Array.from(hiddenRows)));
  applyFilters();
}

function setShowHiddenMode(show) {
  showHiddenMode = show;
  showHiddenBtn.setAttribute("aria-pressed", String(show));
  applyFilters();
}

function applyHideToRows() {
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
