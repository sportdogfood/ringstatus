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
      group: "Small Pony Hunter",
      time: "8:40A",
      class_number: "411",
      class_name: "Small Pony Hunter U/S",
      class_type: "HUN",
      schedule_sequence_type: "Over Fences",
      status: "NOW",
      rollups: [{ horse: "LongHorseName", rider: "Test Rider", time: "8:55A", order: "5/14", in: "15m", walk: "2m", status: "NOW" }],
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
  assert.match(html, /class-num-token/);
  assert.match(html, /class-name-col/);
  assert.match(html, /class-type-col/);
  assert.match(html, /sequence-shade--teal/);
  assert.match(html, /\.c-name \{[\s\S]*font-weight: 560;[\s\S]*padding-left: 3px;/);
  assert.match(html, /cell-token/);
  assert.match(html, /time-status--now/);
  assert.match(html, /time-clock/);
  assert.match(html, /time-text">8:40A/);
  assert.match(html, /\.time-col \{[\s\S]*display: inline-flex;[\s\S]*gap: 3px;/);
  assert.match(html, /\.time-clock \{[\s\S]*flex: 0 0 9px;[\s\S]*overflow: visible;/);
  assert.match(html, /\.time-clock svg \{[\s\S]*min-width: 9px;[\s\S]*overflow: visible;/);
  assert.match(html, /\.time-text \{[\s\S]*flex: 1 1 auto;[\s\S]*text-overflow: ellipsis;/);
  assert.match(html, /rollup-row/);
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
  assert.doesNotMatch(html, /data-rail-row="status"/);
  assert.doesNotMatch(html, /status-token/);
});

test("renderVisualIdentifierHtml keeps class number width identical in ring and time rows", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const classLineCss = html.includes(".class-line {") ? html.slice(html.indexOf(".class-line {"), html.indexOf("}", html.indexOf(".class-line {")) + 1) : "";
  const timeLineCss = html.slice(html.indexOf(".time-line {"), html.indexOf("}", html.indexOf(".time-line {")) + 1);

  assert.match(html, /--schedule-cols: 6ch 4\.5ch 4ch minmax\(0, 1fr\) 4ch;/);
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

  assert.match(html, /rollup-cell rollup-cell--horse">LongHorseName/);
  assert.match(html, /rollup-cell rollup-cell--time">8:55A/);
  assert.match(html, /rollup-cell rollup-cell--order">5\/14/);
  assert.match(html, /\.rollup-cell--time,\n    \.rollup-cell--order \{[\s\S]*border-left: 1px solid rgba\(154, 163, 180, \.22\);[\s\S]*padding-left: 5px;/);
  assert.doesNotMatch(html, /rollup-cell--in/);
  assert.doesNotMatch(html, /rollup-cell--walk/);
  assert.doesNotMatch(html, /In: /);
  assert.doesNotMatch(html, /Walk/);
  assert.doesNotMatch(html, /epill__state/);
  assert.match(html, /rollup-row--now[\s\S]*rollup-cell--horse[\s\S]*rollup-cell--time[\s\S]*rollup-cell--order/);
});

test("renderVisualIdentifierHtml locks rollup table widths and shared radius", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const rollupCss = html.slice(html.indexOf(".rollup-row {"), html.indexOf(".rollup-cell {"));

  assert.match(html, /grid-template-columns: minmax\(0, max-content\) minmax\(6ch, 6ch\) minmax\(5ch, 5ch\);/);
  assert.match(html, /border-radius: var\(--token-radius\);/);
  assert.match(html, /max-width: 8ch;/);
  assert.match(html, /\.rollup-cell--time,\n    \.rollup-cell--order \{[\s\S]*overflow: visible;[\s\S]*text-overflow: clip;/);
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

  assert.match(html, /cell-empty/);
  assert.match(html, /Darcy/);
  assert.match(html, /10:45A/);
  assert.match(html, /2\/22/);
  assert.doesNotMatch(html, /In: 40m/);
  assert.doesNotMatch(html, /Walk: 2m/);
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
  assert.match(rails, /data-ring-anchor="R6"/);
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
  assert.match(html, /body\.dataset\.horseFilter/);
  assert.match(html, /activeHorse === horse \? "" : horse/);
  assert.match(html, /activeStatus === status \? "" : status/);
  assert.match(html, /setActive\(horseButtons, activeHorse \? button : null\)/);
  assert.match(html, /setActiveByValue\(statusButtons, "statusFilter", activeStatus\)/);
  assert.match(html, /band\.classList\.toggle\("is-filter-hidden", !bandMatches\)/);
  assert.match(html, /rollup\.classList\.toggle\("is-filter-hidden", !rollupMatches\)/);
});
