import assert from "node:assert/strict";
import test from "node:test";

import { printReportHtml } from "../src/lib/wec-packing.js";

function printReportWithRows(rowCount) {
  return {
    wave: {
      wave: "wave_one",
      daysTill: 11
    },
    tabGroups: [
      {
        id: "tab:barn",
        label: "Barn",
        listIds: ["barn_hardware"]
      }
    ],
    lists: [
      {
        id: "barn_hardware",
        label: "Barn Hardware"
      }
    ],
    items: Array.from({ length: rowCount }, (_, index) => ({
      id: `item_${index + 1}`,
      name: index === 0 ? "Barn Banner" : `Barn Item ${index + 1}`,
      packListIds: ["barn_hardware"],
      needed: index === 0 ? 1 : 2,
      packed: 0,
      left: index === 0 ? 1 : 2
    }))
  };
}

test("prints packing lists as aligned table rows without inline quantity strings", () => {
  const html = printReportHtml(
    printReportWithRows(2),
    "https://ringstatus.com/test/wec-packing/print?target=tab%3Abarn"
  );

  assert.match(html, /<h1>Barn<\/h1>/);
  assert.match(html, /BARN HARDWARE/);
  assert.match(html, /<th scope="col">NAME<\/th>/);
  assert.match(html, /<th scope="col">NEEDED<\/th>/);
  assert.match(html, /<th scope="col">PACKED<\/th>/);
  assert.match(html, /<th scope="col">LEFT<\/th>/);
  assert.match(html, /class="packing-print-notes-row"/);
  assert.doesNotMatch(html, /Need:/);
  assert.doesNotMatch(html, /Packed:/);
  assert.doesNotMatch(html, /Left:/);
});

test("repeats page and section headers when a section continues", () => {
  const html = printReportHtml(
    printReportWithRows(25),
    "https://ringstatus.com/test/wec-packing/print?target=tab%3Abarn"
  );

  assert.equal((html.match(/<h1>Barn<\/h1>/g) || []).length, 2);
  assert.equal((html.match(/BARN HARDWARE/g) || []).length, 2);
  assert.match(html, /CONTINUED/);
});
