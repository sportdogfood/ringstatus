import clientCss from "../../assets/lpt/client.css?raw";

export function GET() {
  return new Response(clientCss, {
    headers: {
      "content-type": "text/css; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
