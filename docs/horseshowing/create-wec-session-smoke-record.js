const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const TABLE_NAME = "wec_sessions";

if (!AIRTABLE_TOKEN) {
  throw new Error("AIRTABLE_TOKEN is required");
}

async function main() {
  const now = new Date().toISOString();
  const sessionId = `smoke_${Date.now()}`;
  const response = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE_NAME)}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      typecast: true,
      fields: {
        session_id: sessionId,
        device_id: "local_smoke_test",
        show_no: 14906,
        focus_day: "2026-06-14",
        started_at: now,
        last_seen_at: now,
        status: "active",
        page: "wec-session-simple-test",
        source: "local smoke"
      }
    })
  });

  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable failed ${response.status}: ${text}`);
  console.log(text);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
