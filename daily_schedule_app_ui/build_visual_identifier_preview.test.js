const assert = require("node:assert/strict");
const fs = require("node:fs");
const test = require("node:test");

const {
  buildPreviewModel,
  normalizeStatusTerm,
  renderVisualIdentifierHtml,
  renderRails,
} = require("./build_visual_identifier_preview");

const contract = {
  meta: { version: "test" },
  ring_abbreviations: {
    "Ring 6": "R6",
    Intl: "INTL",
    Grand: "GRA",
    "Hunter 1": "H1",
    Derby: "DER",
  },
  status_terms: [
    {
      id: "now",
      label: "NOW",
      incoming_terms: ["Now", "Underway", "livenow"],
      shade: "green",
      treatment: "filled_badge",
    },
    {
      id: "next",
      label: "NEXT",
      incoming_terms: ["Next"],
      shade: "blue",
      treatment: "outline_badge",
    },
    {
      id: "done",
      label: "DONE",
      incoming_terms: ["Completed", "Done", "completed"],
      shade: "muted",
      treatment: "quiet_badge",
    },
  ],
  token_groups: [
    {
      id: "entity",
      title: "Entities",
      tokens: [
        { id: "ring", label: "RING", sample: "Ring 6", treatment: "eyebrow" },
        { id: "class", label: "CLS", sample: "411", treatment: "column" },
      ],
    },
    {
      id: "class_type",
      title: "Class Types",
      tokens: [
        { id: "equitation", label: "EQ", incoming_terms: ["Equitation"], treatment: "tag" },
      ],
    },
    {
      id: "schedule_sequence_type",
      title: "Schedule Sequence Type",
      tokens: [
        { id: "over_fences", label: "OVF", incoming_terms: ["Over Fences"], treatment: "tag", shade: "teal" },
        { id: "flat", label: "U/S", incoming_terms: ["Under Saddle/Flat"], treatment: "tag", shade: "violet" },
      ],
    },
  ],
  sample_rows: [
    {
      ring: "Ring 6",
      ring_late: "42m late",
      ring_takes: "takes 5m",
      group: "Small Pony Hunter",
      time: "8:40A",
      trips: "45",
      gone: "22",
      left: "23",
      class_metric_one: "1",
      class_metric_two: "15",
      class_number: "411",
      class_name: "Small Pony Hunter U/S",
      class_type: "HUN",
      schedule_sequence_type: "Over Fences",
      status: "NOW",
      rollups: [{ horse: "LongHorseName", rider: "Test Rider", entry_number: "10002", time: "8:55A", order: "5/14", starts_in: "15m", leave_in: "2m", status: "NOW" }],
    },
    {
      ring: "Ring 6",
      group: "ASPCA Maclay",
      time: "10:00A",
      class_number: "570",
      class_name: "ASPCA Maclay",
      class_type: "EQ",
      schedule_sequence_type: "Over Fences",
      status: "NEXT",
      rollups: [],
    },
    {
      ring: "Ring 6",
      group: "Modified Hunter",
      time: "7:45A",
      class_number: "644",
      class_name: "Modified Hunter",
      class_type: "HUN",
      schedule_sequence_type: "Over Fences",
      status: "DONE",
      rollups: [],
    },
    {
      ring: "Intl",
      group: "International Derby",
      time: "11:45A",
      class_number: "901",
      class_name: "International Hunter Derby",
      class_type: "HUN",
      schedule_sequence_type: "Over Fences",
      status: "UPC",
      rollups: [],
    },
    {
      ring: "Grand",
      group: "Grand Prix",
      time: "12:30P",
      class_number: "120",
      class_name: "Grand Prix Table II",
      class_type: "JMP",
      schedule_sequence_type: "Over Fences",
      status: "UPC",
      rollups: [],
    },
    {
      ring: "Hunter 1",
      group: "Green Hunter",
      time: "1:15P",
      class_number: "305",
      class_name: "Green Hunter Stake",
      class_type: "HUN",
      schedule_sequence_type: "Over Fences",
      status: "UPC",
      rollups: [],
    },
    {
      ring: "Derby",
      group: "Derby Field",
      time: "2:00P",
      class_number: "777",
      class_name: "National Derby",
      class_type: "EQ",
      schedule_sequence_type: "Under Saddle/Flat",
      status: "UPC",
      rollups: [],
    },
  ],
};

test("normalizeStatusTerm maps endpoint-specific terms to compact labels", () => {
  const model = buildPreviewModel(contract);

  assert.equal(normalizeStatusTerm("Underway", model.statusByIncoming).label, "NOW");
  assert.equal(normalizeStatusTerm("completed", model.statusByIncoming).label, "DONE");
  assert.equal(normalizeStatusTerm("unknown", model.statusByIncoming), null);
});

test("buildPreviewModel groups status and identifier tokens", () => {
  const model = buildPreviewModel(contract);

  assert.equal(model.statusTerms.length, 3);
  assert.equal(model.tokenGroups.length, 3);
  assert.equal(model.sampleRows[0].status, "NOW");
});

test("renderVisualIdentifierHtml uses text tokens and no icon-only status controls", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /http-equiv="Cache-Control" content="no-store"/);
  assert.match(html, /NOW/);
  assert.match(html, /DONE/);
  assert.match(html, /Ring 6/);
  assert.match(html, /Small Pony Hunter U\/S/);
  assert.match(html, /class="time-clock" aria-hidden="true"[\s\S]*<svg viewBox="0 0 16 16"/);
  assert.doesNotMatch(html, /ring_btn--icon/);
});

test("renderVisualIdentifierHtml keeps the Ring row fixed-column contract", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /time-col/);
  assert.match(html, /ring-num-col/);
  assert.match(html, /class-num-col/);
  assert.match(html, /slot-token ring-token/);
  assert.match(html, /slot-token ring-token">INTL/);
  assert.match(html, /slot-token ring-token">GRA/);
  assert.match(html, /slot-token ring-token">H1/);
  assert.match(html, /slot-token ring-token">DER/);
  assert.match(html, /slot-token class-num-token/);
  assert.match(html, /slot-token cell-token c-type/);
  assert.match(html, /class-num-token/);
  assert.match(html, /class-name-col/);
  assert.match(html, /class-type-col/);
  assert.match(html, /slot-token cell-token c-type class-type-shade--hun/);
  assert.match(html, /slot-token cell-token c-type class-type-shade--eq/);
  assert.match(html, /sequence-shade--teal/);
  const doneRowStart = html.indexOf('data-status="DONE"');
  const doneRow = doneRowStart === -1 ? "" : html.slice(doneRowStart, html.indexOf("</article>", doneRowStart));
  assert.doesNotMatch(doneRow, /class-name-col c-name sequence-shade--/);
  assert.match(html, /\.c-name \{[\s\S]*font-weight: 560;[\s\S]*padding-left: 3px;/);
  assert.match(html, /cell-token/);
  assert.match(html, /\.slot-token \{[\s\S]*width: 100%;[\s\S]*min-width: 0;[\s\S]*min-height: 20px;[\s\S]*padding: 1px 4px;[\s\S]*border-radius: var\(--token-radius\);/);
  assert.match(html, /--ring: #a8c7ff;/);
  assert.match(html, /--ring-bg: rgba\(92, 142, 255, \.11\);/);
  assert.match(html, /\.ring-token \{[\s\S]*color: var\(--ring\);[\s\S]*background: var\(--ring-bg\);[\s\S]*border: 1px solid rgba\(168, 199, 255, \.2\);/);
  assert.doesNotMatch(html, /\.time-col,\n    \.ring-num-col/);
  assert.match(html, /time-status--now/);
  assert.match(html, /time-clock/);
  assert.match(html, /Class Overview/);
  assert.doesNotMatch(html, /Class Detail/);
  assert.match(html, /Save to Thread/);
  assert.match(html, /modal-action--icon[\s\S]*aria-label="Close"[\s\S]*<svg viewBox="0 0 16 16"/);
  assert.match(html, /\.modal-head \{[\s\S]*grid-template-columns: minmax\(56px, max-content\) minmax\(0, 1fr\) 24px;/);
  assert.match(html, /\.modal-action--icon \{[\s\S]*width: 24px;[\s\S]*justify-content: center;[\s\S]*justify-self: end;/);
  assert.match(html, /modal-output-label">RING/);
  assert.match(html, /modal-output-label">GROUP/);
  assert.match(html, /modal-output-label">TRIPS/);
  assert.match(html, /modal-label-row[\s\S]*modal-label-time">Time<\/div>[\s\S]*modal-label-number">No<\/div>[\s\S]*modal-label-name ">Name<\/div>[\s\S]*modal-label-order">Trips<\/div>[\s\S]*modal-label-starts">Gone<\/div>[\s\S]*modal-label-leave">Left<\/div>/);
  assert.match(html, /modal-label-name modal-name-span">Name<\/div>[\s\S]*modal-label-leave">Type<\/div>/);
  assert.match(html, /modal-label-order">Order<\/div>[\s\S]*modal-label-starts">In or Ends<\/div>[\s\S]*modal-label-leave">Leave<\/div>/);
  assert.equal((html.match(/class="modal-label-row"/g) || []).length, 3);
  assert.match(html, /\.modal-output-label \{[\s\S]*position: absolute;[\s\S]*clip: rect\(0 0 0 0\);/);
  assert.match(html, /\.modal-output-section \.schedule-line \{[\s\S]*min-height: 22px;[\s\S]*padding: 0;/);
  assert.match(html, /modal-key-col/);
  assert.match(html, /Ring 6 \{42m late\} \{takes 5m\}/);
  assert.match(html, /LongHorseName \+ Test Rider/);
  assert.match(html, /10002/);
  assert.doesNotMatch(html, /modal-output-label">Last/);
  assert.doesNotMatch(html, /Start: 8:40A/);
  assert.doesNotMatch(html, /Go: 8:55A/);
  assert.doesNotMatch(html, /Last: Score: 80/);
  assert.match(html, /<\/span><span class="time-value">8:40A<\/span>/);
  assert.doesNotMatch(html, /<\/span>&nbsp;8:40A/);
  assert.match(html, /\.time-col \{[\s\S]*width: 100%;[\s\S]*justify-self: stretch;[\s\S]*display: grid;[\s\S]*grid-template-columns: 11px minmax\(6ch, 6ch\);[\s\S]*align-items: center;[\s\S]*justify-content: end;[\s\S]*column-gap: 3px;[\s\S]*text-align: right;[\s\S]*overflow: visible;[\s\S]*padding: 0;[\s\S]*min-height: 22px;[\s\S]*height: 22px;[\s\S]*white-space: nowrap;/);
  assert.match(html, /\.time-clock \{[\s\S]*justify-self: center;[\s\S]*overflow: visible;/);
  assert.match(html, /\.time-clock \{[\s\S]*width: 11px;[\s\S]*height: 11px;/);
  assert.match(html, /\.time-clock svg \{[\s\S]*width: 11px;[\s\S]*height: 11px;[\s\S]*min-width: 11px;[\s\S]*overflow: visible;/);
  assert.match(html, /\.time-value \{[\s\S]*display: block;[\s\S]*text-align: right;[\s\S]*white-space: nowrap;/);
  assert.match(html, /\.c-time \{[\s\S]*font-family: "Roboto Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;[\s\S]*font-size: 12px;[\s\S]*font-weight: 560;[\s\S]*line-height: 1\.35;[\s\S]*padding: 0;[\s\S]*background: transparent;[\s\S]*border: 0;/);
  assert.match(html, /\.class-type-shade--hun \{[\s\S]*color: var\(--teal\);[\s\S]*background: var\(--teal-bg\);/);
  assert.match(html, /\.class-type-shade--eq \{[\s\S]*color: var\(--violet\);[\s\S]*background: var\(--violet-bg\);/);
  assert.match(html, /\.class-type-shade--jmp \{[\s\S]*color: var\(--amber\);[\s\S]*background: var\(--amber-bg\);/);
  assert.match(html, /--blue-muted: #b6c8ee;/);
  assert.match(html, /\.time-status--following \{ color: var\(--blue-muted\); \}/);
  assert.match(html, /\.time-status--completed,\n    \.time-status--done \{ color: var\(--text\); \}/);
  assert.match(html, /\.c-name \{[\s\S]*min-height: 22px;[\s\S]*height: 22px;[\s\S]*display: flex;[\s\S]*align-items: center;/);
  assert.match(html, /--modal-overview-cols: minmax\(8ch, 8ch\) 6ch minmax\(0, 1fr\) 5ch 6ch 6ch;/);
  assert.match(html, /\.modal-key-col \{[\s\S]*width: 100%;/);
  assert.match(html, /\.modal-name-span \{[\s\S]*grid-column: span 3;/);
  assert.match(html, /@media \(max-width: 390px\) \{[\s\S]*\.modal-output-section \.schedule-line \{[\s\S]*grid-template-columns: minmax\(8ch, 8ch\) 6ch 5ch 6ch 6ch;/);
  assert.match(html, /\.modal-label-row \{[\s\S]*font-size: 12px;/);
  assert.match(html, /\.modal-label-cell \{[\s\S]*font-size: 8px;/);
  assert.match(html, /\.modal-label-time,\n    \.modal-label-number,\n    \.modal-label-name,\n    \.modal-label-order,\n    \.modal-label-starts,\n    \.modal-label-leave \{ text-align: left; \}/);
  assert.match(html, /@media \(max-width: 390px\) \{[\s\S]*\.modal-label-name \{[\s\S]*display: none;/);
  assert.match(html, /@media \(max-width: 390px\) \{[\s\S]*\.modal-class-line \.class-name-col \{[\s\S]*grid-column: 2 \/ -1;[\s\S]*grid-row: 2;/);
  assert.doesNotMatch(html, /\.modal-class-line \.c-time,[\s\S]*font-size: 9\.5px;/);
  assert.match(html, /rollup-row/);
  assert.match(html, /time-value/);
  assert.doesNotMatch(html, /time-mark/);
  assert.doesNotMatch(html, /sequence-type-col/);
  assert.doesNotMatch(html, /sequence-token/);
  assert.doesNotMatch(html, /status-col/);
  assert.doesNotMatch(html, /trips-col/);
  assert.doesNotMatch(html, /epill__sep/);
});

test("renderVisualIdentifierHtml moves normalized status tokens to the ring eyebrow controls", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /ring-status-controls/);
  assert.match(html, /data-status-filter="NOW"/);
  assert.match(html, /\.ring-status-controls \.rail-button\[data-status-filter="NEXT"\] \{[\s\S]*background: var\(--blue-bg\);[\s\S]*border-color: rgba\(143, 184, 255, \.24\);[\s\S]*color: var\(--blue\);/);
  assert.match(html, /\.ring-status-controls \.rail-button\[data-status-filter="DONE"\] \{[\s\S]*background: rgba\(154, 163, 180, \.12\);[\s\S]*border-color: rgba\(154, 163, 180, \.16\);[\s\S]*color: #d4d9e6;/);
  assert.doesNotMatch(html, /data-rail-row="status"/);
  assert.doesNotMatch(html, /status-token/);
});

test("renderVisualIdentifierHtml keeps class number width identical in ring and time rows", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const classLineCss = html.includes(".class-line {") ? html.slice(html.indexOf(".class-line {"), html.indexOf("}", html.indexOf(".class-line {")) + 1) : "";
  const timeLineCss = html.slice(html.indexOf(".time-line {"), html.indexOf("}", html.indexOf(".time-line {")) + 1);

  assert.match(html, /--schedule-cols: minmax\(8ch, 8ch\) 4\.5ch 4ch minmax\(0, 1fr\) 4ch;/);
  assert.match(html, /\.schedule-line \{[\s\S]*grid-template-columns: var\(--schedule-cols\);/);
  assert.match(html, /schedule-line class-line/);
  assert.match(html, /schedule-line time-line/);
  assert.doesNotMatch(classLineCss, /grid-template-columns:/);
  assert.doesNotMatch(timeLineCss, /grid-template-columns:/);
});

test("renderVisualIdentifierHtml renders identical rollup rows under ring and time views", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const timeCard = html.slice(html.indexOf('<section class="time-card">'), html.indexOf("</section>", html.indexOf('<section class="time-card">')));

  assert.match(html, /rollup-line/);
  assert.match(html, /time-rollup-cell/);
  assert.match(timeCard, /time-rollup-cell/);
  assert.match(timeCard, /LongHorseName/);
  assert.match(timeCard, /rollup-cell rollup-cell--time">8:55A/);
});

test("renderVisualIdentifierHtml right-aligns trip rollups", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /\.rollup-line \{[\s\S]*justify-content: flex-end;[\s\S]*overflow-x: auto;/);
  assert.match(html, /\.time-rollup-cell \{[\s\S]*justify-content: flex-end;[\s\S]*overflow-x: auto;/);
  assert.doesNotMatch(html, /\.rollup-line:has\(\.rollup-row:nth-child\(2\)\)/);
  assert.doesNotMatch(html, /\.time-rollup-cell:has\(\.rollup-row:nth-child\(2\)\)/);
});

test("renderVisualIdentifierHtml adds band classes and filter-ready row attributes", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /schedule-band schedule-band--now/);
  assert.match(html, /data-ring="R6"/);
  assert.match(html, /data-group="Small Pony Hunter"/);
  assert.match(html, /data-status="NOW"/);
  assert.match(html, /data-status-id="now"/);
  assert.match(html, /data-class-type="HUN"/);
  assert.match(html, /data-sequence-type="OVF"/);
  assert.match(html, /class-name-col c-name sequence-shade--teal/);
  assert.match(html, /data-class-number="411"/);
  assert.match(html, /data-period="am"/);
  assert.match(html, /data-first-up="false"/);
  assert.match(html, /data-has-rollups="true"/);
  assert.match(html, /data-horses="LongHorseName"/);
  assert.match(html, /data-riders="Test Rider"/);
  assert.match(html, /\.schedule-band--now::before/);
  assert.match(html, /\.ring-card \.schedule-band:nth-of-type\(even\),\n    \.time-card \.schedule-band:nth-of-type\(even\) \{/);
  assert.match(html, /background: rgba\(255, 255, 255, \.012\);/);
  assert.match(html, /rgba\(73, 209, 125, \.34\)/);
  assert.match(html, /background: transparent;/);
});

test("renderVisualIdentifierHtml keeps horse and rider filter data on hidden rollup attributes", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /data-horse="LongHorseName"/);
  assert.match(html, /data-rider="Test Rider"/);
  assert.match(html, /data-rollup-status="NOW"/);
  assert.match(html, /data-rollup-status-id="now"/);
  assert.doesNotMatch(html, />Test Rider</);
});

test("renderVisualIdentifierHtml keeps compact rollups in horse time order only", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const orderCssStart = html.indexOf(".rollup-cell--order {");
  const orderCss = orderCssStart === -1 ? "" : html.slice(orderCssStart, html.indexOf("}", orderCssStart) + 1);

  assert.match(html, /rollup-cell rollup-cell--horse">LongHorseName/);
  assert.match(html, /rollup-cell rollup-cell--time">8:55A/);
  assert.match(html, /rollup-cell rollup-cell--order">5\/14/);
  assert.doesNotMatch(html, /&nbsp;8:55A/);
  assert.doesNotMatch(html, /&nbsp;5\/14/);
  assert.match(html, /\.rollup-cell--time \{[\s\S]*font-family: "Roboto Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;[\s\S]*font-weight: 560;/);
  assert.doesNotMatch(orderCss, /font-family: "Roboto Mono"/);
  assert.match(html, /\.class-card\.schedule-band \{[\s\S]*row-gap: 3px;[\s\S]*padding: 8px 10px;/);
  assert.match(html, /\.schedule-line \{[\s\S]*column-gap: 3px;[\s\S]*row-gap: 3px;[\s\S]*padding: 8px 10px;/);
  assert.match(html, /\.class-card \.schedule-line \{[\s\S]*min-height: 22px;[\s\S]*padding: 0;/);
  assert.match(html, /\.rollup-line \{[\s\S]*padding: 0;/);
  assert.match(html, /\.time-rollup-cell \{[\s\S]*padding-top: 0;/);
  assert.match(html, /\.rollup-row \{[\s\S]*--rollup-cell-x: 7px;[\s\S]*flex: 0 0 auto;[\s\S]*align-items: stretch;[\s\S]*height: 20px;[\s\S]*padding: 0;[\s\S]*font-size: 9px;/);
  assert.match(html, /\.rollup-cell \{[\s\S]*min-height: 18px;[\s\S]*height: 100%;[\s\S]*display: flex;[\s\S]*align-items: center;[\s\S]*line-height: 1;[\s\S]*padding: 0 var\(--rollup-cell-x\);/);
  assert.match(html, /\.rollup-cell--time,\n    \.rollup-cell--order \{[\s\S]*border-left: 1px solid rgba\(182, 200, 238, \.4\);/);
  assert.doesNotMatch(html, /padding-left: 7px;/);
  assert.doesNotMatch(html, /rollup-cell--in/);
  assert.doesNotMatch(html, /rollup-cell--walk/);
  assert.doesNotMatch(html, /In: 15m/);
  assert.doesNotMatch(html, /Walk: 2m/);
  assert.doesNotMatch(html, /epill__state/);
  assert.match(html, /rollup-row--now[\s\S]*rollup-cell--horse[\s\S]*rollup-cell--time[\s\S]*rollup-cell--order/);
});

test("renderVisualIdentifierHtml locks rollup table widths and shared radius", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const rollupCss = html.slice(html.indexOf(".rollup-row {"), html.indexOf(".rollup-cell {"));

  assert.match(html, /grid-template-columns: minmax\(0, max-content\) minmax\(calc\(6ch \+ \(var\(--rollup-cell-x\) \* 2\)\), calc\(6ch \+ \(var\(--rollup-cell-x\) \* 2\)\)\) minmax\(calc\(5ch \+ \(var\(--rollup-cell-x\) \* 2\)\), calc\(5ch \+ \(var\(--rollup-cell-x\) \* 2\)\)\);/);
  assert.match(html, /column-gap: 0;/);
  assert.match(html, /border-radius: var\(--token-radius\);/);
  assert.match(html, /max-width: calc\(8ch \+ \(var\(--rollup-cell-x\) \* 2\)\);/);
  assert.match(html, /\.rollup-cell--time,\n    \.rollup-cell--order \{[\s\S]*justify-content: center;[\s\S]*overflow: visible;[\s\S]*text-overflow: clip;/);
  assert.match(html, /text-overflow: ellipsis;/);
  assert.doesNotMatch(rollupCss, /border-radius: 999px;/);
});

test("renderVisualIdentifierHtml gives outlined token treatments horizontal padding", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /\.token--column, \.token--mono_text, \.token--tiny_text, \.token--eyebrow \{/);
  assert.match(html, /padding: 3px 7px;/);
  assert.match(html, /border-radius: var\(--token-radius\);/);
});

test("renderVisualIdentifierHtml preserves fixed cells when row values are empty", () => {
  const payload = JSON.parse(JSON.stringify(contract));
  payload.sample_rows.push({
    ring: "Ring 6",
    time: "",
    class_number: "",
    class_name: "",
    class_type: "",
    schedule_sequence_type: "",
    status: "",
    metric: "",
    rollups: [{ horse: "Darcy", rider: "Hidden Rider", time: "10:45A", order: "2/22", in: "40m", walk: "2m", status: "UPC" }],
  });

  const html = renderVisualIdentifierHtml(buildPreviewModel(payload));
  const darcyRowStart = html.indexOf('data-horses="Darcy"');
  const darcyRow = darcyRowStart === -1 ? "" : html.slice(darcyRowStart, html.indexOf("</article>", darcyRowStart));

  assert.match(html, /cell-empty/);
  assert.match(html, /Darcy/);
  assert.match(html, /10:45A/);
  assert.match(html, /2\/22/);
  assert.doesNotMatch(darcyRow, /In: 40m/);
  assert.doesNotMatch(darcyRow, /Walk: 2m/);
});

test("renderVisualIdentifierHtml uses ring walk in the eyebrow instead of status pills", () => {
  const payload = JSON.parse(JSON.stringify(contract));
  payload.sample_rows[0].ring_walk = "5m";

  const html = renderVisualIdentifierHtml(buildPreviewModel(payload));

  assert.match(html, /ring-eyebrow/);
  assert.match(html, /ring-status-controls/);
  assert.match(html, /\.ring-title \{[\s\S]*flex: 0 0 auto;/);
  assert.match(html, /\.ring-eyebrow \{[\s\S]*justify-content: flex-end;[\s\S]*flex: 1 1 auto;/);
  assert.match(html, /ring-walk/);
  assert.match(html, /\.ring-walk:empty \{[\s\S]*display: none;/);
  assert.match(html, /WALK 5m/);
  assert.doesNotMatch(html, /ring-states/);
});

test("visual_identifier_contract gives key class and group flags distinct shades", () => {
  const actualContract = JSON.parse(fs.readFileSync("./daily_schedule_app_ui/visual_identifier_contract.json", "utf8"));
  const flags = actualContract.token_groups.find((group) => group.id === "flags").tokens;
  const targetIds = ["is_warmup", "is_mulligan", "is_add_back", "is_classic", "is_usf", "is_handy"];
  const shades = targetIds.map((id) => flags.find((token) => token.id === id).shade);

  assert.deepEqual(shades, ["slate", "red", "amber", "green", "blue", "violet"]);
  assert.equal(new Set(shades).size, targetIds.length);
});

test("renderVisualIdentifierHtml uses identical status controls in Ring and Time eyebrows", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const ringCard = html.slice(html.indexOf('<section class="ring-card">'), html.indexOf("</section>", html.indexOf('<section class="ring-card">')));
  const timeCard = html.slice(html.indexOf('<section class="time-card">'), html.indexOf("</section>", html.indexOf('<section class="time-card">')));

  assert.match(ringCard, /ring-status-controls[\s\S]*data-status-filter="NOW"[\s\S]*data-status-filter="NEXT"[\s\S]*data-status-filter="DONE"/);
  assert.match(timeCard, /ring-status-controls[\s\S]*data-status-filter="NOW"[\s\S]*data-status-filter="NEXT"[\s\S]*data-status-filter="DONE"/);
  assert.match(html, /setActiveByValue\(statusButtons, "statusFilter", activeStatus\)/);
});

test("renderVisualIdentifierHtml includes a time-only view using the same row data plus ring number", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /time-card/);
  assert.match(html, /time-line/);
  assert.match(html, /ring-num-col/);
  assert.match(html, /ring-token/);
  assert.match(html, /class-num-token/);
  assert.match(html, /--token-radius/);
  assert.match(html, /R6/);
  assert.match(html, /INTL/);
  assert.match(html, /GRA/);
  assert.match(html, /H1/);
  assert.match(html, /DER/);
  assert.match(html, /Ring 6/);
});

test("renderVisualIdentifierHtml places compact schedule views before identifier reference", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.ok(html.indexOf('<section class="ring-card">') < html.indexOf("<h2>Status Language</h2>"));
  assert.ok(html.indexOf('<section class="time-card">') < html.indexOf("<h2>Status Language</h2>"));
});

test("renderRails creates ring anchors and horse filters from row data", () => {
  const rails = renderRails(buildPreviewModel(contract));

  assert.match(rails, /data-rail-row="rings"/);
  assert.match(rails, /filter-actions/);
  assert.match(rails, /quick-filter-group/);
  assert.match(rails, /data-quick-filter="first_up"/);
  assert.match(rails, /data-quick-filter="am"/);
  assert.match(rails, /data-quick-filter="pm"/);
  assert.match(rails, /data-rollup-only-toggle/);
  assert.match(rails, /aria-pressed="false"/);
  assert.match(rails, /rollup-switch__track[\s\S]*rollup-switch__label">Team/);
  assert.doesNotMatch(rails, /data-switch-state/);
  assert.doesNotMatch(rails, />OFF</);
  assert.doesNotMatch(rails, />Trips</);
  assert.match(rails, /data-ring-anchor="R6"/);
  assert.match(rails, /data-ring-anchor="INTL"/);
  assert.match(rails, /data-ring-anchor="GRA"/);
  assert.match(rails, /data-ring-anchor="H1"/);
  assert.match(rails, /data-ring-anchor="DER"/);
  assert.match(rails, /data-rail-row="horses"/);
  assert.match(rails, /data-horse-filter="LongHorseName"/);
  assert.doesNotMatch(rails, /All Status/);
  assert.doesNotMatch(rails, /All Horses/);
  assert.doesNotMatch(rails, /data-ring-anchor=""/);
  assert.doesNotMatch(rails, /data-horse-filter=""/);
});

test("renderVisualIdentifierHtml keeps ring rail as anchors and horse rail as filter behavior", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /data-ring-anchor="R6"/);
  assert.match(html, /scrollIntoView/);
  assert.match(html, /data-horse-filter="LongHorseName"/);
  assert.match(html, /data-has-rollups="false"/);
  assert.match(html, /body\.dataset\.horseFilter/);
  assert.match(html, /activeHorse === horse \? "" : horse/);
  assert.match(html, /activeStatus === status \? "" : status/);
  assert.match(html, /let activeQuickFilter = ""/);
  assert.match(html, /body\.dataset\.quickFilter = activeQuickFilter/);
  assert.match(html, /activeQuickFilter === "first_up" && band\.dataset\.firstUp === "true"/);
  assert.match(html, /activeQuickFilter === "am" && band\.dataset\.period === "am"/);
  assert.match(html, /activeQuickFilter === "pm" && band\.dataset\.period === "pm"/);
  assert.match(html, /activeQuickFilter === quickFilter \? "" : quickFilter/);
  assert.match(html, /let activeRollupOnly = false/);
  assert.match(html, /body\.dataset\.rollupOnly = activeRollupOnly \? "true" : ""/);
  assert.match(html, /band\.dataset\.hasRollups === "true"/);
  assert.match(html, /horseMatches && statusMatches && quickMatches && rollupOnlyMatches/);
  assert.match(html, /rollupOnlyToggle\.classList\.toggle\("is-active", activeRollupOnly\)/);
  assert.doesNotMatch(html, /rollupOnlyState/);
  assert.doesNotMatch(html, /"ON" : "OFF"/);
  assert.match(html, /setActive\(horseButtons, activeHorse \? button : null\)/);
  assert.match(html, /setActiveByValue\(statusButtons, "statusFilter", activeStatus\)/);
  assert.match(html, /band\.classList\.toggle\("is-filter-hidden", !bandMatches\)/);
  assert.match(html, /rollup\.classList\.toggle\("is-filter-hidden", !rollupMatches\)/);
});

test("renderVisualIdentifierHtml styles the trip-related switch as a left-right toggle", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /\.filter-actions \{[\s\S]*justify-content: flex-end;/);
  assert.match(html, /\.quick-filter-group \{[\s\S]*display: inline-flex;[\s\S]*gap: 5px;/);
  assert.match(html, /\.quick-filter-group \.rail-button\.is-active \{[\s\S]*color: var\(--blue\);/);
  assert.match(html, /\.rollup-switch \{[\s\S]*border-radius: var\(--token-radius\);[\s\S]*padding: 4px 8px;/);
  assert.match(html, /\.rollup-switch__track \{[\s\S]*width: 30px;[\s\S]*border-radius: var\(--token-radius\);/);
  assert.match(html, /\.rollup-switch__thumb \{[\s\S]*transform: translateX\(0\);/);
  assert.match(html, /\.rollup-switch\.is-active \.rollup-switch__thumb \{[\s\S]*transform: translateX\(15px\);/);
});
