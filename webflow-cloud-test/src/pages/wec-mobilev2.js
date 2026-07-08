import html from "../assets/wec-mobilev2/source.html?raw";

export const GET = async () => new Response(html, {
  headers: {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store"
  }
});
