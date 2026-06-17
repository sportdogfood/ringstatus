globalThis.process ??= {};
globalThis.process.env ??= {};
import { c as createComponent } from "./astro-component_m2FK-tsi.mjs";
import { e as addAttribute, h as renderHead, r as renderTemplate } from "./worker-entry_BEZKNIIY.mjs";
const pages = [
  {
    slug: "tack",
    title: "Tack, packed right.",
    subtitle: "A simple landing page for organizing show-day tack, packing, and reminders.",
    cta: "Open Tack"
  },
  {
    slug: "barn",
    title: "Barn work, organized.",
    subtitle: "Keep feed, turnout, lessons, and packing in one simple flow.",
    cta: "Open Barn"
  },
  {
    slug: "show",
    title: "Show day, simplified.",
    subtitle: "Fast pages for riders, trainers, and grooms who need the right information now.",
    cta: "Open Show"
  }
];
function getStaticPaths() {
  return pages.map((page2) => ({
    params: { slug: page2.slug },
    props: { page: page2 }
  }));
}
const $$slug = createComponent(($$result, $$props, $$slots) => {
  const Astro2 = $$result.createAstro($$props, $$slots);
  Astro2.self = $$slug;
  const { page: page2 } = Astro2.props;
  return renderTemplate`<html lang="en"> <head><meta charset="utf-8"><title>${page2.title}</title><meta name="description"${addAttribute(page2.subtitle, "content")}>${renderHead()}</head> <body> <main> <p>RingStatus Test Page</p> <h1>${page2.title}</h1> <p>${page2.subtitle}</p> <a href="#">${page2.cta}</a> </main> </body></html>`;
}, "C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/webflow-cloud-test/src/pages/[slug].astro", void 0);
const $$file = "C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/webflow-cloud-test/src/pages/[slug].astro";
const $$url = "/test/[slug]";
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  default: $$slug,
  file: $$file,
  getStaticPaths,
  url: $$url
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
