import clientJs from "../../assets/rs-recognition/client.js?raw";

export function GET() {
  return new Response(clientJs, {
    headers: {
      "content-type": "text/javascript; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
