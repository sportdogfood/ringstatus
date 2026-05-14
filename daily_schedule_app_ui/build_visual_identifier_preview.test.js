const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildPreviewModel,
  normalizeStatusTerm,
  renderVisualIdentifierHtml,
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
  ],
  sample_rows: [
    {
      ring: "Ring 6",
      time: "8:40A",
      class_number: "411",
      class_name: "Small Pony Hunter U/S",
      class_type: "EQ",
      status: "NOW",
      rollups: [{ horse: "LongHorseName", time: "8:55A", order: "5/14", in: "15m", walk: "2m", status: "NOW" }],
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

  assert.equal(model.statusTerms.length, 2);
  assert.equal(model.tokenGroups.length, 2);
  assert.equal(model.sampleRows[0].status, "NOW");
});

test("renderVisualIdentifierHtml uses text tokens and no icon-only status controls", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /NOW/);
  assert.match(html, /DONE/);
  assert.match(html, /Ring 6/);
  assert.match(html, /Small Pony Hunter U\/S/);
  assert.doesNotMatch(html, /<svg/i);
  assert.doesNotMatch(html, /ring_btn--icon/);
});

test("renderVisualIdentifierHtml keeps the Ring row fixed-column contract", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /time-col/);
  assert.match(html, /class-num-col/);
  assert.match(html, /class-name-col/);
  assert.match(html, /class-type-col/);
  assert.match(html, /status-col/);
  assert.match(html, /trips-col/);
  assert.match(html, /cell-token/);
  assert.match(html, /trip-metric/);
  assert.match(html, /time-mark/);
  assert.match(html, /time-mark--now/);
  assert.match(html, /rollup-row/);
  assert.doesNotMatch(html, /epill__sep/);
});

test("renderVisualIdentifierHtml keeps compact rollups in horse time order in walk order", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));
  const rollup = html.slice(html.indexOf('<span class="rollup-row rollup-row--now">'), html.indexOf("</span></div>", html.indexOf('<span class="rollup-row rollup-row--now">')));

  assert.match(html, /rollup-cell rollup-cell--horse">LongHorseName/);
  assert.match(html, /rollup-cell rollup-cell--time">8:55A/);
  assert.match(html, /rollup-cell rollup-cell--order">5\/14/);
  assert.match(html, /rollup-cell rollup-cell--in">In: 15m/);
  assert.match(html, /rollup-cell rollup-cell--walk">Walk: 2m/);
  assert.doesNotMatch(html, /epill__state/);
  assert.ok(rollup.indexOf("rollup-cell--horse") < rollup.indexOf("rollup-cell--time"));
  assert.ok(rollup.indexOf("rollup-cell--time") < rollup.indexOf("rollup-cell--order"));
  assert.ok(rollup.indexOf("rollup-cell--order") < rollup.indexOf("In: 15m"));
  assert.ok(rollup.indexOf("In: 15m") < rollup.indexOf("Walk: 2m"));
});

test("renderVisualIdentifierHtml locks rollup table widths and shared radius", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /grid-template-columns: 9ch 6ch 5ch 5ch 6ch;/);
  assert.match(html, /border-radius: var\(--token-radius\);/);
  assert.match(html, /max-width: 9ch;/);
  assert.match(html, /text-overflow: ellipsis;/);
  assert.doesNotMatch(html, /\.rollup-row[\s\S]*?border-radius: 999px;/);
});

test("renderVisualIdentifierHtml preserves fixed cells when row values are empty", () => {
  const payload = JSON.parse(JSON.stringify(contract));
  payload.sample_rows.push({
    ring: "Ring 6",
    time: "",
    class_number: "",
    class_name: "",
    class_type: "",
    status: "",
    metric: "",
    rollups: [{ horse: "Darcy", time: "10:45A", order: "2/22", in: "40m", walk: "2m", status: "UPC" }],
  });

  const html = renderVisualIdentifierHtml(buildPreviewModel(payload));

  assert.match(html, /cell-empty/);
  assert.match(html, /Darcy/);
  assert.match(html, /10:45A/);
  assert.match(html, /2\/22/);
  assert.match(html, /In: 40m/);
  assert.match(html, /Walk: 2m/);
});

test("renderVisualIdentifierHtml uses ring walk in the eyebrow instead of status pills", () => {
  const payload = JSON.parse(JSON.stringify(contract));
  payload.sample_rows[0].ring_walk = "5m";

  const html = renderVisualIdentifierHtml(buildPreviewModel(payload));

  assert.match(html, /ring-walk/);
  assert.match(html, /WALK 5m/);
  assert.doesNotMatch(html, /ring-states/);
});

test("renderVisualIdentifierHtml includes a time-only view using the same row data plus ring number", () => {
  const html = renderVisualIdentifierHtml(buildPreviewModel(contract));

  assert.match(html, /time-card/);
  assert.match(html, /time-line/);
  assert.match(html, /ring-num-col/);
  assert.match(html, /ring-token/);
  assert.match(html, /--token-radius/);
  assert.match(html, /R6/);
  assert.match(html, /Ring 6/);
});
