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

const wecPrintOverrides = `<style>
  :root{--ring:#815374;}
  .title{font-size:16px;}
  .subtitle{font-size:11px;}
  .ring-head{font-size:12px;padding:5px 7px;}
  .gdisplay{font-size:12px;margin-bottom:-7px;}
  .class-list{padding:4px 7px 5px 7px;}
  .c{grid-template-columns:54px 1fr;column-gap:7px;padding:3px 0;padding-right:112px;}
  .t,.n{font-size:10.5px;}
</style>`;

script = script
  .replace(
    'return "9999";',
    'return "8778";'
  )
  .replace(
    'SCHEDULE_URL = "https://ringstatus-proxy.gombcg.workers.dev/docs/9999/schedules/schedule.json";',
    `SCHEDULE_URL = ${JSON.stringify(dataFile)};`
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
<title>WEC WEF Print ${showNo} ${focusDay}</title>
${style}
${wecPrintOverrides}
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
