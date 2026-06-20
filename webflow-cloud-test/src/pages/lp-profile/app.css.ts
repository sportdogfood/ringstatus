import appCss from "../../assets/lp-profile/app.css?raw";

export function GET() {
  return new Response(appCss, {
    headers: {
      "content-type": "text/css; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
