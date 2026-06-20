globalThis.process ??= {};
globalThis.process.env ??= {};
const layerJson = '{\n  "version": 1,\n  "updatedAt": "2026-05-19T00:50:57.403Z",\n  "horses": {},\n  "competitions": {},\n  "classes": {},\n  "videos": {}\n}';
function GET() {
  return new Response(layerJson, {
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "public, max-age=60"
    }
  });
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
