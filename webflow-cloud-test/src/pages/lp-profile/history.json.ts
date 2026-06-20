import historyJson from "../../assets/lp-profile/history.json?raw";

export function GET() {
  return new Response(historyJson, {
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
