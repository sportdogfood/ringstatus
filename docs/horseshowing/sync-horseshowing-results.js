const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const BASE_URL = "https://www.horseshowing.com";

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function clean(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function htmlDecode(value) {
  return clean(String(value ?? "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#039;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">"));
}

function stripTags(value) {
  return htmlDecode(String(value ?? "").replace(/<br\s*\/?>/gi, " ").replace(/<[^>]+>/g, " "));
}

function csvCell(value) {
  const text = String(value ?? "");
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function writeCsv(file, rows) {
  const headers = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => csvCell(row[header])).join(","))
  ];
  fs.writeFileSync(file, `${lines.join("\n")}\n`);
}

function slug(value) {
  return clean(value).replace(/[^A-Za-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function searchFilterFromText(text) {
  const tokens = clean(text).split(/\s+/).filter((token) => token.length >= 1);
  if (!tokens.length) return "";
  return tokens.map((token) => {
    const safe = token.replace(/'/g, "\\'");
    return `(cl.name like '%${safe}%' OR cl.number = '${safe}' OR ss.name = '${safe}')`;
  }).join(" AND ");
}

class HorseShowingSession {
  constructor() {
    this.cookies = new Map();
  }

  cookieHeader() {
    return [...this.cookies.entries()].map(([key, value]) => `${key}=${value}`).join("; ");
  }

  storeCookies(response) {
    const raw = response.headers.get("set-cookie");
    if (!raw) return;
    for (const item of raw.split(/,(?=[^;,]+=)/)) {
      const [pair] = item.split(";");
      const [key, ...rest] = pair.split("=");
      if (key && rest.length) this.cookies.set(key.trim(), rest.join("=").trim());
    }
  }

  async request(url, options = {}) {
    const response = await fetch(url, {
      ...options,
      headers: {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36",
        ...(this.cookieHeader() ? { cookie: this.cookieHeader() } : {}),
        ...(options.headers || {})
      }
    });
    this.storeCookies(response);
    const text = await response.text();
    if (!response.ok) throw new Error(`${url} HTTP ${response.status}: ${text.slice(0, 300)}`);
    return text;
  }
}

function parseClassRows(html) {
  return [...String(html || "").matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)].map((match) => {
    const cells = [...match[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => stripTags(cell[1]));
    if (cells.length < 5) return null;
    return {
      class_number: cells[0],
      class_name: cells[1],
      entry_count: Number.parseInt(cells[2], 10) || 0,
      class_no: cells[3],
      sect_no: cells[4]
    };
  }).filter(Boolean);
}

async function resolveClassRowsByNumber(session, classNumbers) {
  const resolved = new Map();
  for (const classNumber of [...new Set(classNumbers.map(clean).filter(Boolean))]) {
    const filter = searchFilterFromText(classNumber);
    const html = await session.request(`${BASE_URL}/srched_classes.php?${new URLSearchParams({ filter }).toString()}`, {
      headers: {
        "x-requested-with": "XMLHttpRequest",
        referer: `${BASE_URL}/hrot4.php`
      }
    });
    const exact = parseClassRows(html).find((row) => String(row.class_number) === String(classNumber));
    if (exact) resolved.set(String(classNumber), exact);
  }
  return resolved;
}

function parseClassHeader(block) {
  const headerHtml = (block.match(/<th class="th_nb"[^>]*>([\s\S]*?)<\/th>/i) || [])[1] || "";
  const entries = Number.parseInt(((headerHtml.match(/Entries:\s*(\d+)/i) || [])[1] || ""), 10) || 0;
  const label = stripTags(headerHtml.replace(/<span[\s\S]*?<\/span>/gi, ""));
  const classNumber = (label.match(/^(\d+[A-Za-z]?)\)/) || [])[1] || "";
  const className = clean(label.replace(/^\d+[A-Za-z]?\)\s*/, ""));
  return { class_label: label, class_number: classNumber, class_name: className, result_entry_count: entries };
}

function parseResults(html, classRows) {
  const byNumber = new Map(classRows.map((row) => [String(row.class_number), row]));
  const results = [];
  const classes = [];
  const blocks = [...String(html || "").matchAll(/<div class="lg[^"]*">([\s\S]*?)<\/div>\s*<!-- lg -->/gi)];
  for (let blockIndex = 0; blockIndex < blocks.length; blockIndex += 1) {
    const blockMatch = blocks[blockIndex];
    const block = blockMatch[1];
    const parsedClass = parseClassHeader(block);
    const classSource = byNumber.get(String(parsedClass.class_number)) || classRows[blockIndex] || {};
    const hasScore = /<th[^>]*>\s*Score\s*<\/th>/i.test(block);
    const hasPrize = /<th[^>]*>\s*Prize/i.test(block);
    classes.push({
      ...parsedClass,
      class_no: classSource.class_no || "",
      sect_no: classSource.sect_no || "",
      has_score: hasScore,
      has_prize: hasPrize
    });
    const body = block.split(/<\/thead>/i)[1] || "";
    for (const rowMatch of body.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
      const cells = [...rowMatch[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => stripTags(cell[1]));
      if (cells.length < 5) continue;
      results.push({
        class_no: classSource.class_no || "",
        sect_no: classSource.sect_no || "",
        class_number: parsedClass.class_number,
        class_name: parsedClass.class_name,
        place: cells[0],
        entry_no: cells[1],
        horse: cells[2],
        rider: cells[3],
        owner: cells[4],
        score: hasScore ? cells[5] || "" : "",
        prize: hasPrize ? cells[cells.length - 1] || "" : ""
      });
    }
  }
  return { classes, results };
}

function attachResolvedClassRows(parsed, resolvedByNumber) {
  for (const row of parsed.classes) {
    const resolved = resolvedByNumber.get(String(row.class_number));
    if (!resolved) continue;
    row.class_no = resolved.class_no || row.class_no || "";
    row.sect_no = resolved.sect_no || row.sect_no || "";
    row.entry_count = resolved.entry_count || "";
  }
  for (const row of parsed.results) {
    const resolved = resolvedByNumber.get(String(row.class_number));
    if (!resolved) continue;
    row.class_no = resolved.class_no || row.class_no || "";
    row.sect_no = resolved.sect_no || row.sect_no || "";
  }
}

function parseHrotStandings(html) {
  const standings = [];
  const tables = [...String(html || "").matchAll(/<table[^>]*footable[^>]*>([\s\S]*?)<\/table>/gi)];
  for (const tableMatch of tables) {
    const table = tableMatch[1];
    const title = stripTags((table.match(/<th[^>]*colspan[^>]*>([\s\S]*?)<\/th>/i) || [])[1] || "");
    const rows = [...table.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
    for (const rowMatch of rows) {
      const cells = [...rowMatch[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => stripTags(cell[1]));
      if (cells.length < 6) continue;
      standings.push({
        division: title,
        standing: cells[0],
        points: cells[1],
        entry_no: cells[2],
        horse: cells[3],
        rider: cells[4],
        owner: cells[5]
      });
    }
  }
  return standings;
}

async function main() {
  const showNo = argValue("--show-no", "14906");
  const search = argValue("--search", "");
  const classNosArg = argValue("--class-nos", "");
  const hrotSearch = argValue("--hrot-search", "");
  const hrotName = argValue("--name", "");
  const allClasses = process.argv.includes("--all-classes");
  const outDir = argValue("--out-dir", "docs/horseshowing/results-lane");
  fs.mkdirSync(outDir, { recursive: true });

  const session = new HorseShowingSession();
  await session.request(`${BASE_URL}/show.php?show=${encodeURIComponent(showNo)}`, {
    headers: { referer: `${BASE_URL}/showsel.php` }
  });

  if (hrotSearch || hrotName) {
    if (!hrotSearch || !hrotName) throw new Error("HROT mode requires both --hrot-search and --name.");
    const form = new URLSearchParams({
      hrot_search: hrotSearch,
      name: hrotName
    });
    const resultHtml = await session.request(`${BASE_URL}/hrot_results.php`, {
      method: "POST",
      body: form.toString(),
      headers: {
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
        origin: BASE_URL,
        referer: `${BASE_URL}/hrot4.php`
      }
    });
    const parsed = parseResults(resultHtml, []);
    const resolvedByNumber = await resolveClassRowsByNumber(session, parsed.classes.map((row) => row.class_number));
    attachResolvedClassRows(parsed, resolvedByNumber);
    const standings = parseHrotStandings(resultHtml);
    const stem = `${showNo}-hrot-${slug(hrotSearch)}-${slug(hrotName)}`;
    const payload = {
      ok: true,
      show_no: showNo,
      hrot_search: hrotSearch,
      name: hrotName,
      standings_rows: standings.length,
      result_classes: parsed.classes.length,
      result_rows: parsed.results.length,
      generated_at: new Date().toISOString(),
      standings,
      classes: parsed.classes,
      results: parsed.results
    };

    fs.writeFileSync(path.join(outDir, `${stem}.html`), resultHtml);
    fs.writeFileSync(path.join(outDir, `${stem}.json`), `${JSON.stringify(payload, null, 2)}\n`);
    writeCsv(path.join(outDir, `${stem}-standings.csv`), standings);
    writeCsv(path.join(outDir, `${stem}-classes.csv`), parsed.classes);
    writeCsv(path.join(outDir, `${stem}-results.csv`), parsed.results);
    console.log(JSON.stringify({
      ok: true,
      show_no: showNo,
      hrot_search: hrotSearch,
      name: hrotName,
      standings_rows: standings.length,
      result_classes: parsed.classes.length,
      result_rows: parsed.results.length,
      out_dir: outDir,
      stem
    }, null, 2));
    return;
  }

  let classRows = [];
  if (allClasses || search) {
    const filter = allClasses ? "(cl.number like '%')" : searchFilterFromText(search);
    const html = await session.request(`${BASE_URL}/srched_classes.php?${new URLSearchParams({ filter }).toString()}`, {
      headers: {
        "x-requested-with": "XMLHttpRequest",
        referer: `${BASE_URL}/hrot4.php`
      }
    });
    classRows = parseClassRows(html);
  }

  const explicitClassNos = classNosArg
    ? classNosArg.split(/[,\s]+/).map(clean).filter(Boolean)
    : [];
  const classNos = explicitClassNos.length
    ? explicitClassNos
    : classRows.map((row) => row.class_no).filter(Boolean);

  if (!classNos.length) throw new Error("No class_nos resolved. Provide --class-nos or a --search that returns classes.");

  const form = new URLSearchParams({
    class_nos: JSON.stringify(classNos),
    sect_nos: JSON.stringify([])
  });
  const resultHtml = await session.request(`${BASE_URL}/show_results4.php`, {
    method: "POST",
    body: form.toString(),
    headers: {
      "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
      "x-requested-with": "XMLHttpRequest",
      origin: BASE_URL,
      referer: `${BASE_URL}/hrot4.php`
    }
  });

  if (!classRows.length) {
    classRows = classNos.map((classNo) => ({ class_no: classNo, class_number: "", class_name: "", entry_count: 0, sect_no: "" }));
  }
  const parsed = parseResults(resultHtml, classRows);
  const classHash = crypto.createHash("sha1").update(classNos.join("|")).digest("hex").slice(0, 8);
  const stem = `${showNo}-results-${search ? slug(search) : `class-nos-${classNos.length}-${classHash}`}`;
  const payload = {
    ok: true,
    show_no: showNo,
    search,
    requested_class_nos: classNos,
    class_search_rows: classRows.length,
    result_classes: parsed.classes.length,
    result_rows: parsed.results.length,
    generated_at: new Date().toISOString(),
    classes: parsed.classes,
    results: parsed.results
  };

  fs.writeFileSync(path.join(outDir, `${stem}.html`), resultHtml);
  fs.writeFileSync(path.join(outDir, `${stem}.json`), `${JSON.stringify(payload, null, 2)}\n`);
  writeCsv(path.join(outDir, `${stem}-classes.csv`), parsed.classes);
  writeCsv(path.join(outDir, `${stem}-results.csv`), parsed.results);
  console.log(JSON.stringify({
    ok: true,
    show_no: showNo,
    search,
    requested_class_nos: classNos.length,
    result_classes: parsed.classes.length,
    result_rows: parsed.results.length,
    out_dir: outDir,
    stem
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
