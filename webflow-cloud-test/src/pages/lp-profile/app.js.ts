import appJs from "../../assets/lp-profile/app.js?raw";

export function GET() {
  return new Response(appJs, {
    headers: {
      "content-type": "text/javascript; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
