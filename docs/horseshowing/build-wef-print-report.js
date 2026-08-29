const fs = require("fs");
const path = require("path");

const repoRoot = path.resolve(__dirname, "..", "..");
const templateDir = path.join(__dirname, "wef-print-template");
const reportsDir = path.join(__dirname, "reports");

const showNo = process.argv[2] || "14906";
const focusDay = process.argv[3] || "2026-06-10";
const dataFile = `${showNo}-${focusDay}-crt-mobile-data.json`;
const htmlFile = `${showNo}-${focusDay}-wef-print.html`;

const style = fs.readFileSync(path.join(templateDir, "wef-print-style.fragment.html"), "utf8");
const shell = fs.readFileSync(path.join(templateDir, "wef-print-shell.fragment.html"), "utf8");
let script = fs.readFileSync(path.join(templateDir, "wef-print-script.fragment.html"), "utf8");

script = script
  .replace(
    'return "9999";',
    'return "8778";'
  )
  .replace(
    'const values = items.map((row) => row?.[TENANT_SCHED_DISPLAY_KEY]);',
    'const values = items.map((row) => row?.[TENANT_SCHED_DISPLAY_KEY] ?? row?.sched_display ?? row?.group_display);'
  )
  .replace(
    'ACTIVE_TENANT_CONFIG.title ??\n      first.show_days_report_title ??',
    'first.show_days_report_title ??\n      ACTIVE_TENANT_CONFIG.title ??'
  );

const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=816, initial-scale=1" />
<title>WEF Print ${showNo} ${focusDay}</title>
${style}
</head>
<body>
${shell}
${script}
</body>
</html>
`;

fs.mkdirSync(reportsDir, { recursive: true });
const out = path.join(reportsDir, htmlFile);
fs.writeFileSync(out, html);

console.log(JSON.stringify({
  htmlOut: out,
  dataFile: path.join(reportsDir, dataFile)
}, null, 2));
