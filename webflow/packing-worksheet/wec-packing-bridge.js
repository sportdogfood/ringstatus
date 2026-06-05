(function () {
  const config = window.WEC_PACKING_BRIDGE_CONFIG || {};
  const root = document.querySelector(config.rootSelector || "[data-rs-packing-bridge]");
  if (!root) return;

  const endpointUrl = config.endpointUrl || root.dataset.rsEndpoint || "";
  if (!endpointUrl) {
    root.setAttribute("data-rs-error", "missing-endpoint");
    return;
  }

  fetch(endpointUrl, { cache: "no-store" })
    .then((response) => response.json())
    .then((data) => {
      if (!data || data.ok === false) throw new Error(data?.error || "bad-response");
      bindValues(root, data, defaultValues(data));
      bindHorseRows(root, data);
      root.setAttribute("data-rs-loaded", "true");
    })
    .catch((error) => {
      root.setAttribute("data-rs-error", error.message || "load-failed");
    });

  function defaultValues(data) {
    const counts = data.counts || {};
    const wave = data.wave || {};
    return {
      report_title: wave.reportTitle || "",
      report_subtitle: wave.reportSubtitle || "",
      pack_wave_label: wave.label || "",
      horse_count: counts.horses,
      visible_horse_count: counts.visibleHorses,
      kit_count: counts.kits,
      kit_item_count: counts.kitItems,
      touched_count: counts.packingRows,
      need_count: counts.need,
      packed_count: counts.packed,
      left_count: counts.left,
      not_needed_count: counts.notNeeded
    };
  }

  function bindValues(scope, data, values) {
    scope.querySelectorAll("[data-rs-value]").forEach((el) => {
      const key = el.dataset.rsValue;
      if (!key) return;
      const value = Object.prototype.hasOwnProperty.call(values, key)
        ? values[key]
        : getPath(data, key);
      if (value != null && typeof value !== "object") el.textContent = value;
    });
  }

  function bindHorseRows(scope, data) {
    const list = scope.querySelector('[data-rs-list="horse_rows"]');
    const template = scope.querySelector('[data-rs-template="horse_row"]');
    if (!list || !template) return;

    list.querySelectorAll('[data-rs-row="horse_row"]').forEach((row) => row.remove());
    buildHorseRows(data).forEach((row, index) => {
      const node = template.cloneNode(true);
      node.hidden = false;
      node.removeAttribute("data-rs-template");
      node.setAttribute("data-rs-row", "horse_row");

      bindValues(node, row, {
        row_number: index + 1,
        horse_id: row.horseId,
        horse_name: row.horseName,
        kit_label: row.kitLabel,
        need: row.need,
        packed: row.packed,
        left: row.left
      });

      node.querySelectorAll("[data-rs-open-horse]").forEach((open) => {
        open.dataset.horseId = row.horseId || "";
        if (row.profileUrl) {
          open.href = row.profileUrl;
        } else {
          open.removeAttribute("href");
        }
        open.addEventListener("click", (event) => {
          if (!row.profileUrl) event.preventDefault();
          root.dispatchEvent(new CustomEvent("rs:open-horse", {
            bubbles: true,
            detail: { horseId: row.horseId, row }
          }));
        });
      });

      list.appendChild(node);
    });
  }

  function buildHorseRows(data) {
    const kits = data.kits || [];
    const kitItems = data.kitItems || [];
    const packingRows = data.packingRows || [];
    const itemById = new Map(kitItems.map((item) => [item.id, item]));

    return (data.horses || []).map((horse) => {
      const items = unique(horse.pakKitItemIds || [])
        .map((id) => itemById.get(id))
        .filter((item) => item && item.active !== false && item.status !== "inactive");
      const kit = kits.find((candidate) =>
        (candidate.kitItemIds || []).some((id) => items.some((item) => item.id === id))
      );
      const counts = countItems(horse, kit, items, packingRows);

      return {
        horseId: horse.id,
        horseName: horse.barnName || horse.name || horse.showName || "",
        profileUrl: horse.profileUrl || "",
        kitLabel: kit ? (kit.displayLabel || kit.label || kit.name || "") : "",
        need: counts.need,
        packed: counts.packed,
        left: counts.left
      };
    });
  }

  function countItems(horse, kit, items, packingRows) {
    if (!kit || !items.length) return { need: 0, packed: 0, left: 0 };
    let packed = 0;
    let notNeeded = 0;

    items.forEach((item) => {
      const row = packingRows.find((candidate) =>
        includes(candidate.horseIds, horse.id) &&
        includes(candidate.kitIds, kit.id) &&
        includes(candidate.kitItemIds, item.id)
      );
      if (row?.packState === "packed") packed += 1;
      if (row?.packState === "not_needed" || row?.neededState === "not_needed") notNeeded += 1;
    });

    const need = Math.max(0, items.length - notNeeded);
    return { need, packed, left: Math.max(0, need - packed) };
  }

  function getPath(source, path) {
    return String(path || "")
      .split(".")
      .filter(Boolean)
      .reduce((value, key) => value == null ? undefined : value[key], source);
  }

  function unique(values) {
    return [...new Set(values || [])];
  }

  function includes(values, value) {
    return Array.isArray(values) && values.includes(value);
  }
})();
