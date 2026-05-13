const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildPreviewModel,
  normalizeStatusTerm,
  renderVisualIdentifierHtml,
} = require("./build_visual_identifier_preview");

const contract = {
  meta: { version: "test" },
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
      rollups: [{ name: "Knox", oog: "5/14", time: "8:55A", status: "NOW" }],
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
