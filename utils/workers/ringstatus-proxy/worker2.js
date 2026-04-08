// ringstatus-proxy Worker (FULL DROP)
// Supports:
//   GET  /health
//   GET  /docs/*   -> proxies UPSTREAM_BASE/docs/*
//   GET  /items/*  -> proxies UPSTREAM_BASE/items/*
//   POST /docs/commit-bulk  -> commits docs/* files to GitHub repo/branch via Git API
//   POST /items/commit-bulk -> commits items/* files to GitHub repo/branch via Git API
//
// Required env vars (Worker Settings -> Variables):
//   UPSTREAM_BASE   (e.g. https://raw.githubusercontent.com/sportdogfood/ringstatus-data/main)
//   GITHUB_REPO     (e.g. sportdogfood/ringstatus-data)
//   GITHUB_BRANCH   (e.g. main)
//   GITHUB_TOKEN    (GitHub PAT with contents:read/write to that repo)  [SECRET]
// Optional:
//   CACHE_TTL       (seconds; default 300)

const DEFAULT_TTL = 300;

// ---------- small utils ----------
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
};

function withCors(res) {
  const h = new Headers(res.headers);
  for (const [k, v] of Object.entries(corsHeaders)) h.set(k, v);
  return new Response(res.body, { status: res.status, headers: h });
}

function json(data, status = 200, extraHeaders = {}) {
  const h = new Headers({ "Content-Type": "application/json; charset=utf-8", ...extraHeaders, ...corsHeaders });
  return new Response(JSON.stringify(data, null, 2) + "\n", { status, headers: h });
}

function text(body, status = 200, extraHeaders = {}) {
  const h = new Headers({ "Content-Type": "text/plain; charset=utf-8", ...extraHeaders, ...corsHeaders });
  return new Response(String(body), { status, headers: h });
}

function safePath(rel) {
  if (typeof rel !== "string" || !rel.length) return false;
  if (rel.includes("..")) return false;
  if (!/^[a-z0-9][\w\-./]+$/i.test(rel)) return false;
  // conservative extension allowlist
  if (!/\.(json|txt|html|xml|csv|ndjson|md|tmpl|css|js)$/i.test(rel)) return false;
  return true;
}

function normalizeNoLeadingSlash(p) {
  return String(p || "").replace(/^\/+/, "");
}

async function readJsonBody(req) {
  const raw = await req.text();
  if (!raw) return null;
  try {
    const first = JSON.parse(raw);
    // tolerate double-stringified bodies
    if (typeof first === "string") {
      try { return JSON.parse(first); } catch { return first; }
    }
    // tolerate stringified json field
    if (first && typeof first.json === "string") {
      try { first.json = JSON.parse(first.json); } catch {}
    }
    return first;
  } catch {
    return null;
  }
}

function contentTypeForPath(path) {
  const p = String(path || "").toLowerCase();

  if (p.endsWith(".html")) return "text/html; charset=utf-8";
  if (p.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (p.endsWith(".css")) return "text/css; charset=utf-8";
  if (p.endsWith(".json")) return "application/json; charset=utf-8";
  if (p.endsWith(".xml")) return "application/xml; charset=utf-8";
  if (p.endsWith(".csv")) return "text/csv; charset=utf-8";
  if (p.endsWith(".txt")) return "text/plain; charset=utf-8";
  if (p.endsWith(".md")) return "text/markdown; charset=utf-8";
  if (p.endsWith(".tmpl")) return "text/plain; charset=utf-8";
  if (p.endsWith(".ndjson")) return "application/x-ndjson; charset=utf-8";

  return "application/octet-stream";
}

// ---------- proxy GET ----------
async function proxyGet(upstreamUrl, ttlSec, requestedPath) {
  const r = await fetch(upstreamUrl, {
    method: "GET",
    cf: { cacheTtl: ttlSec, cacheEverything: true },
  });

  const h = new Headers(r.headers);
  const contentType = contentTypeForPath(requestedPath);
  const p = String(requestedPath || "").toLowerCase();

  // force correct content-type based on requested file extension
  h.set("content-type", contentType);

  // strip upstream headers that break standalone app rendering
  if (p.endsWith(".html") || p.endsWith(".js")) {
    h.delete("content-security-policy");
    h.delete("x-frame-options");
  }

  for (const [k, v] of Object.entries(corsHeaders)) h.set(k, v);

  return new Response(r.body, { status: r.status, headers: h });
}

// ---------- GitHub commit-bulk (docs/items) ----------
async function ghApi(env, path, init) {
  const url = `https://api.github.com${path}`;
  const headers = new Headers(init?.headers || {});
  headers.set("Accept", "application/vnd.github+json");
  headers.set("Authorization", `Bearer ${env.GITHUB_TOKEN}`);
  headers.set("User-Agent", "ringstatus-proxy");
  if (init?.json) headers.set("Content-Type", "application/json; charset=utf-8");

  const res = await fetch(url, {
    ...init,
    headers,
    body: init?.json ? JSON.stringify(init.json) : init?.body,
  });

  const txt = await res.text();
  let j = null;
  try { j = JSON.parse(txt); } catch {}
  return { ok: res.ok, status: res.status, json: j, text: txt };
}

function requireGitEnv(env) {
  const need = ["GITHUB_REPO", "GITHUB_BRANCH", "GITHUB_TOKEN"];
  for (const k of need) {
    if (!env[k] || !String(env[k]).trim()) throw new Error(`Missing env ${k}`);
  }
}

function validateBulkFiles(files, requiredPrefix) {
  if (!Array.isArray(files) || files.length === 0) throw new Error("files[] required");

  const allowDocsExt = /\.(html|xml|json|js|css)$/i;
  const allowItemsExt = /\.(json|txt|csv|ndjson|md|tmpl)$/i;

  const allowExt = requiredPrefix === "docs/" ? allowDocsExt : allowItemsExt;

  for (let i = 0; i < files.length; i++) {
    const f = files[i];
    if (!f || !f.path || !f.content_base64) throw new Error(`files[${i}] requires path and content_base64`);
    const p = String(f.path);
    if (!p.toLowerCase().startsWith(requiredPrefix)) throw new Error(`files[${i}].path must start with '${requiredPrefix}'`);

    const clean = p.slice(requiredPrefix.length);
    if (!safePath(clean)) throw new Error(`files[${i}].path not safe`);
    if (!allowExt.test(p)) throw new Error(`files[${i}].path extension not allowed`);
  }
}

async function commitBulkToGit(env, body, requiredPrefix) {
  requireGitEnv(env);

  const { message, overwrite, force, files } = body || {};
  const shouldForce = overwrite === true || force === true;

  validateBulkFiles(files, requiredPrefix);

  const repo = env.GITHUB_REPO;
  const branch = env.GITHUB_BRANCH;

  // 1) get current ref
  const refPath = `/repos/${repo}/git/refs/heads/${encodeURIComponent(branch)}`;
  const ref = await ghApi(env, refPath, { method: "GET" });
  if (!ref.ok) throw new Error(`ref ${ref.status}: ${ref.text.slice(0, 200)}`);
  const baseCommit = ref.json?.object?.sha;
  if (!baseCommit) throw new Error("Missing base commit sha");

  // 2) get base tree
  const commit = await ghApi(env, `/repos/${repo}/git/commits/${baseCommit}`, { method: "GET" });
  if (!commit.ok) throw new Error(`commit ${commit.status}: ${commit.text.slice(0, 200)}`);
  const baseTree = commit.json?.tree?.sha;
  if (!baseTree) throw new Error("Missing base tree sha");

  // 3) create blobs
  const blobShas = [];
  for (const f of files) {
    const blob = await ghApi(env, `/repos/${repo}/git/blobs`, {
      method: "POST",
      json: { content: f.content_base64, encoding: "base64" },
    });
    if (!blob.ok) throw new Error(`blob ${blob.status}: ${blob.text.slice(0, 200)}`);
    blobShas.push({ path: f.path, sha: blob.json?.sha });
  }

  // 4) create tree
  const tree = await ghApi(env, `/repos/${repo}/git/trees`, {
    method: "POST",
    json: {
      base_tree: baseTree,
      tree: blobShas.map(x => ({ path: x.path, mode: "100644", type: "blob", sha: x.sha })),
    },
  });
  if (!tree.ok) throw new Error(`tree ${tree.status}: ${tree.text.slice(0, 200)}`);
  const newTree = tree.json?.sha;
  if (!newTree) throw new Error("Missing new tree sha");

  // 5) create commit
  const commitMsg = message || `${requiredPrefix} bulk publish`;
  const newCommit = await ghApi(env, `/repos/${repo}/git/commits`, {
    method: "POST",
    json: { message: commitMsg, tree: newTree, parents: [baseCommit] },
  });
  if (!newCommit.ok) throw new Error(`commit-post ${newCommit.status}: ${newCommit.text.slice(0, 200)}`);

  const newSha = newCommit.json?.sha;
  if (!newSha) throw new Error("Missing new commit sha");

  // 6) move ref
  const patch = await ghApi(env, refPath, {
    method: "PATCH",
    json: { sha: newSha, force: shouldForce },
  });
  if (!patch.ok) throw new Error(`ref-patch ${patch.status}: ${patch.text.slice(0, 200)}`);

  return {
    ok: true,
    commit: { sha: newSha, url: `https://github.com/${repo}/commit/${newSha}` },
    committed_paths: files.map(f => f.path),
  };
}

// ---------- Worker entry ----------
export default {
  async fetch(req, env, ctx) {
    try {
      const url = new URL(req.url);
      const path = url.pathname;
      const method = req.method.toUpperCase();

      if (method === "OPTIONS") return text("OK", 204);

      const ttl = Number(env.CACHE_TTL || DEFAULT_TTL) || DEFAULT_TTL;

      // health
      if (method === "GET" && path === "/health") return text("OK", 200);

      // proxy /api/* -> SGL_API (strip /api prefix) added as latest
      if (path.startsWith("/api/")) {
        const incomingUrl = new URL(req.url);
        const forwardPath = path.replace(/^\/api/, "") || "/";

        const upstreamUrl = new URL(`https://sgl-api.example${forwardPath}`);
        upstreamUrl.search = incomingUrl.search;

        const proxiedReq = new Request(upstreamUrl.toString(), req);
        const res = await env.SGL_API.fetch(proxiedReq);

        return withCors(res);
      }

      // proxy GET /docs/*
      if (method === "GET" && path.startsWith("/docs/")) {
        const rel = normalizeNoLeadingSlash(path.slice("/docs/".length));
        if (!safePath(rel)) return json({ ok: false, error: "Invalid path" }, 400);

        const upstream = `${env.UPSTREAM_BASE}/docs/${rel}`;
        return await proxyGet(upstream, ttl, rel);
      }

      // proxy GET /items/*
      if (method === "GET" && path.startsWith("/items/")) {
        const rel = normalizeNoLeadingSlash(path.slice("/items/".length));
        if (!safePath(rel)) return json({ ok: false, error: "Invalid path" }, 400);

        const upstream = `${env.UPSTREAM_BASE}/items/${rel}`;
        return await proxyGet(upstream, ttl, rel);
      }

      // commit-bulk docs
      if (method === "POST" && path === "/docs/commit-bulk") {
        const body = await readJsonBody(req);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, 400);

        const out = await commitBulkToGit(env, body, "docs/");
        return json(out, 200);
      }

      // commit-bulk items
      if (method === "POST" && path === "/items/commit-bulk") {
        const body = await readJsonBody(req);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, 400);

        const out = await commitBulkToGit(env, body, "items/");
        return json(out, 200);
      }

      return json({ ok: false, error: "Not found" }, 404);
    } catch (e) {
      return json({ ok: false, error: String(e?.message || e).slice(0, 300) }, 500);
    }
  },
};
