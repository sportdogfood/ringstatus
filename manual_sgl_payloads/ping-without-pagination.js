(async () => {
  const base = "https://sglapi.wellingtoninternational.com/classes";
  const all = [];

  for (let page = 1; page <= 25; page++) {
    const params = new URLSearchParams({
      sort_on: "number",
      sort_type: "asc",
      page: String(page),
      search_text: "",
      show_id: "200000063",
      customer_id: "15"
    });

    const url = `${base}?${params.toString()}`;

    const res = await fetch(url, {
      credentials: "include"
    });

    console.log("page", page, "status", res.status);

    if (!res.ok) break;

    const data = await res.json();

    const rows = Array.isArray(data)
      ? data
      : data.data || data.classes || data.results || [];

    console.log("rows", rows.length, rows);

    all.push(...rows);
  }

  window.wellingtonClasses = all;

  console.log("TOTAL ROWS:", all.length);
  console.table(all);
})();