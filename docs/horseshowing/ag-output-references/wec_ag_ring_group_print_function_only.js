// Source reference:
// C:\Users\gombc\Downloads\wec_ag_ring_group_flyup_test.backup-20260705-140656.html
//
// Scope:
// Isolated print behavior only. This is not the special full-width row shape,
// not the base row contract, not the filter/hide/focus function, and not the
// interactive AG Grid display.
//
// Behavior shape:
// - Build a dedicated printSheet from current visibleRings.
// - Hide the interactive shell/flyout/backdrop during print.
// - Print the dedicated sheet, not the live AG viewport.
// - Use compact black-white print styling with tight margins.
//
// Key output controls:
// - One-page intent comes from tight @page margin, compact font sizes, short
//   row heights, small gaps, print-only sheet, and multi-column layout.
// - Column count is controlled by .print-columns columns value.
// - Orientation is controlled by @page size.
// - Black-white output is controlled by print CSS colors/backgrounds/borders.

const PRINT_LAYOUTS = {
  oneColumn: {
    pageSize: "portrait",
    margin: "0.25in",
    columns: "1 auto",
    columnGap: "0"
  },
  twoColumnPortrait: {
    pageSize: "portrait",
    margin: "0.25in",
    columns: "2 250px",
    columnGap: "10px"
  },
  threeColumnLandscape: {
    pageSize: "landscape",
    margin: "0.28in",
    columns: "3 250px",
    columnGap: "12px"
  }
};

const PRINT_ONE_PAGE_ATTRIBUTES = {
  pageMargin: "0.25in to 0.28in",
  pageOrientation: "portrait or landscape through @page size",
  sheetMode: "print a dedicated .print-sheet instead of the AG viewport",
  headerSize: "h1 14px, meta 8px, compact border",
  rowSize: "8px font, 14px min-height, 1.08 line-height",
  rowPadding: "1px 2px",
  rowBreak: "break-inside: avoid",
  groupBreak: "break-inside: avoid",
  printColumns: "columns: 1/2/3 with tight column-gap",
  colorMode: "black-white, white page, grayscale separators",
  overflow: "rollup text nowrap ellipsis"
};

const PRINT_CSS = `
.print-sheet { display: none; }

@media print {
  @page {
    size: landscape;
    margin: 0.28in;
  }

  body {
    overflow: visible;
    background: #fff;
    color: #111;
  }

  .shell,
  .flyout,
  .flyout-backdrop {
    display: none !important;
  }

  .print-sheet {
    display: block;
    font-family: Arial, Helvetica, sans-serif;
    color: #111;
  }

  .print-title {
    display: flex;
    justify-content: space-between;
    align-items: end;
    border-bottom: 1px solid #222;
    margin-bottom: 5px;
    padding-bottom: 4px;
  }

  .print-title h1 {
    margin: 0;
    font-size: 14px;
  }

  .print-title p {
    margin: 0;
    font-size: 8px;
    text-align: right;
    line-height: 1.05;
  }

  .print-columns {
    columns: 3 250px;
    column-gap: 12px;
  }

  .print-ring-group {
    break-inside: avoid;
    margin-bottom: 6px;
    padding: 1px 2px 2px;
  }

  .print-ring-group:nth-child(even) {
    background: #f4f4f4;
  }

  .print-ring {
    font-size: 11px;
    font-weight: 800;
    border-bottom: 1px solid #333;
    margin-bottom: 2px;
    padding-bottom: 1px;
    line-height: 1.05;
  }

  .print-row {
    display: grid;
    grid-template-columns: 46px minmax(0, 1fr);
    gap: 5px;
    font-size: 8px;
    min-height: 14px;
    align-items: center;
    padding: 1px 2px;
    line-height: 1.08;
    break-inside: avoid;
    border-bottom: 1px solid #e7e7e7;
  }

  .print-row.is-zebra {
    background: #f8f8f8;
  }

  .print-row.has-rollup {
    grid-template-rows: auto auto;
    background: #e8e8e8;
    padding-bottom: 2px;
    border-bottom-color: #d2d2d2;
  }

  .print-rollup {
    grid-column: 1 / -1;
    color: #333;
    font-size: 8px;
    font-weight: 700;
    line-height: 1;
    text-transform: uppercase;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    padding-top: 1px;
  }
}
`;

function printLayoutCss(layoutName) {
  const layout = PRINT_LAYOUTS[layoutName] || PRINT_LAYOUTS.threeColumnLandscape;

  return `
@media print {
  @page {
    size: ${layout.pageSize};
    margin: ${layout.margin};
  }

  .print-columns {
    columns: ${layout.columns};
    column-gap: ${layout.columnGap};
  }
}
`;
}

function buildPrintSheet() {
  const generatedAt = new Date().toISOString();

  const body = visibleRings
    .map(ring =>
      '<section class="print-ring-group">' +
        '<div class="print-ring">' + escapeHtml(ring.title) + "</div>" +
        ring.classes
          .map((row, index) =>
            '<div class="print-row' +
              (index % 2 ? " is-zebra" : "") +
              (row.horse_items && row.horse_items.length ? " has-rollup" : "") +
            '">' +
              printRollup(row) +
              '<span class="print-cell time">' + escapeHtml(row.time || "--") + "</span>" +
              '<span class="print-cell class">' + escapeHtml(classLabel(row)) + "</span>" +
            "</div>"
          )
          .join("") +
      "</section>"
    )
    .join("");

  printSheet.innerHTML =
    '<div class="print-title">' +
      "<h1>WEC Print Review</h1>" +
      "<p>" + escapeHtml(metadataStatusPrefix()) + "<br>" + escapeHtml(generatedAt) + "</p>" +
    '</div>' +
    '<div class="print-columns">' + body + "</div>";
}

function printRollup(row) {
  const value = rollupText(row);
  return value ? '<div class="print-rollup">' + escapeHtml(value) + "</div>" : "";
}

function printGrid() {
  buildPrintSheet();
  setTimeout(() => window.print(), 50);
}

function bindPrintButton() {
  document.getElementById("printBtn").addEventListener("click", printGrid);
}
