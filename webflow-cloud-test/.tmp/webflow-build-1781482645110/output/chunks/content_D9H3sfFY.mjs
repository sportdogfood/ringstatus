globalThis.process ??= {};
globalThis.process.env ??= {};
function normalizeContext(input = {}) {
  const tags = normalizeTags(input.tags || input.user_iam || input.userIam);
  const season = slug(input.season || input.currentSeason || input.current_season || "");
  const profile = objectValue(input.profile);
  const preferences = objectValue(input.preferences);
  return {
    tags,
    season,
    profile,
    preferences
  };
}
function buildPersonalizedSection(dataset = {}, inputContext = {}) {
  const context = normalizeContext(inputContext);
  const seasons = objectValue(dataset.seasons);
  const seasonKey = context.season || slug(dataset.defaultSeason || firstKey(seasons) || "");
  const season = objectValue(seasons[seasonKey]);
  const seasonLabel = String(season.label || titleCase(seasonKey || "season")).trim();
  const tagSet = new Set(context.tags.map(slug));
  const activityPool = Array.isArray(season.activities) ? season.activities : [];
  const activities = activityPool.filter((activity) => matchesTags(activity, tagSet)).map(normalizeActivity);
  return {
    ok: true,
    context: {
      season: seasonKey,
      tags: context.tags,
      profile: context.profile,
      preferences: context.preferences
    },
    season: {
      key: seasonKey,
      label: seasonLabel
    },
    copy: {
      headline: templateText(season.headline || "I love when its {season} season", { season: seasonLabel }),
      intro: templateText(season.intro || "In the {season} I like to do these activities", { season: seasonLabel })
    },
    activities,
    fallback: activities.length ? "" : String(dataset.fallback || "No activities matched this profile yet.")
  };
}
function normalizeTags(value) {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
  if (!value) return [];
  return String(value).split(",").map((item) => item.trim()).filter(Boolean);
}
function matchesTags(activity, tagSet) {
  if (!tagSet.size) return true;
  const activityTags = normalizeTags(activity.tags).map(slug);
  if (!activityTags.length) return false;
  return activityTags.every((tag) => tagSet.has(tag));
}
function normalizeActivity(activity) {
  return {
    title: String(activity.title || "").trim(),
    description: String(activity.description || "").trim(),
    tags: normalizeTags(activity.tags)
  };
}
function templateText(value, replacements) {
  return String(value || "").replace(/\{season\}/gi, replacements.season);
}
function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}
function firstKey(value) {
  return Object.keys(value || {})[0] || "";
}
function slug(value) {
  return String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}
function titleCase(value) {
  return String(value).split(/[-_\s]+/).filter(Boolean).map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase()).join(" ");
}
const config = {
  runtime: "edge"
};
const defaultDatasetUrl = "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/personalized-section/personalized-section-content.json";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};
const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });
const GET = async ({ url }) => {
  const datasetUrl = new URL(url.searchParams.get("datasetUrl") || defaultDatasetUrl);
  const dataset = await fetchDataset(datasetUrl);
  const context = normalizeContext({
    season: url.searchParams.get("season") || "",
    tags: url.searchParams.get("tags") || ""
  });
  return json(buildPersonalizedSection(dataset, context));
};
const POST = async ({ request, url }) => {
  const payload = await readJson(request);
  const datasetUrl = new URL(payload.datasetUrl || defaultDatasetUrl);
  const dataset = await fetchDataset(datasetUrl);
  return json(buildPersonalizedSection(dataset, payload.context || payload));
};
async function fetchDataset(url) {
  const response = await fetch(url);
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    return {
      fallback: `Personalized content failed to load from ${url.pathname}.`,
      seasons: {}
    };
  }
  return result;
}
async function readJson(request) {
  const text = await request.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {};
  }
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2) + "\n", {
    status,
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json; charset=utf-8"
    }
  });
}
const _page = /* @__PURE__ */ Object.freeze(/* @__PURE__ */ Object.defineProperty({
  __proto__: null,
  GET,
  OPTIONS,
  POST,
  config
}, Symbol.toStringTag, { value: "Module" }));
const page = () => _page;
export {
  page
};
