import layerJson from "../../assets/lp-profile/layer.json?raw";

export function GET() {
  return new Response(layerJson, {
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
