export const config = {
  runtime: "edge"
};

import { buildPersonalizedSection, normalizeContext, resolveDatasetUrl } from "../../lib/personalized-content.js";

const defaultDatasetUrl = "https://cdn.jsdelivr.net/gh/sportdogfood/ringstatus@main/webflow/personalized-section/personalized-section-content.json";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

export const OPTIONS = async () => new Response(null, { status: 204, headers: corsHeaders });

export const GET = async ({ url }) => {
  const datasetUrl = new URL(resolveDatasetUrl(url.searchParams.get("datasetUrl"), defaultDatasetUrl));
  const dataset = await fetchDataset(datasetUrl);
  const context = normalizeContext({
    season: url.searchParams.get("season") || "",
    tags: url.searchParams.get("tags") || ""
  });
  return json(buildPersonalizedSection(dataset, context));
};

export const POST = async ({ request, url }) => {
  const payload = await readJson(request);
  const datasetUrl = new URL(resolveDatasetUrl(payload.datasetUrl, defaultDatasetUrl));
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
