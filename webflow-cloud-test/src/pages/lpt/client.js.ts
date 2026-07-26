import clientJs from "../../assets/lpt/client.js?raw";

export function GET() {
  return new Response(clientJs, {
    headers: {
      "content-type": "text/javascript; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
