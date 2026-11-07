const { spawn } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { SoftPayloadError } = require("./soft_payload_guard");

const DEFAULT_DIRECT_BASE_URL = "https://sglapi.wellingtoninternational.com";
const DEFAULT_WORKER_HOSTS = new Set([
  "broad-tooth-b8ed.gombcg.workers.dev",
]);

function envValue(env, name) {
  return env && Object.prototype.hasOwnProperty.call(env, name) ? env[name] : process.env[name];
}

function splitCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function workerHosts(env = process.env) {
  const hosts = new Set(DEFAULT_WORKER_HOSTS);
  for (const host of splitCsv(envValue(env, "SGL_WORKER_HOSTS"))) hosts.add(host);
  return hosts;
}

function parseUrl(value) {
  try {
    return new URL(String(value || ""));
  } catch {
    return null;
  }
}

function isSglDataPath(pathname) {
  const pathText = String(pathname || "").toLowerCase();
  return pathText === "/ring" ||
    pathText.startsWith("/classsignup") ||
    pathText.startsWith("/schedule") ||
    pathText === "/classes" ||
    pathText.startsWith("/classes/") ||
    pathText.startsWith("/entries/") ||
    pathText.startsWith("/people/");
}

function isSglDataUrl(url, { env = process.env } = {}) {
  const parsed = parseUrl(url);
  if (!parsed) return false;

  const host = parsed.hostname.toLowerCase();
  if (!isSglDataPath(parsed.pathname)) return false;
  return host === "sglapi.wellingtoninternational.com" || workerHosts(env).has(host);
}

function resolveSglDirectUrl(url, { env = process.env } = {}) {
  const parsed = parseUrl(url);
  if (!parsed) return String(url || "");

  const directBase = String(envValue(env, "SGL_DIRECT_BASE_URL") || DEFAULT_DIRECT_BASE_URL).replace(/\/+$/, "");
  const direct = new URL(directBase);
  const host = parsed.hostname.toLowerCase();

  if (host === direct.hostname.toLowerCase()) return parsed.toString();
  if (!workerHosts(env).has(host) || !isSglDataPath(parsed.pathname)) return parsed.toString();

  direct.pathname = parsed.pathname;
  direct.search = parsed.search;
  direct.hash = "";
  return direct.toString();
}

function transportMode(env = process.env) {
  return String(
    envValue(env, "SGL_FETCH_TRANSPORT") ||
    envValue(env, "SGL_FETCH_MODE") ||
    "powershell"
  ).trim().toLowerCase();
}

function shouldUsePowerShellSglFetch(url, { platform = process.platform, env = process.env } = {}) {
  const mode = transportMode(env);
  if (["node", "off", "disabled", "false", "0"].includes(mode)) return false;
  if (!["powershell", "iwr", "auto", "local"].includes(mode)) return false;
  if (platform !== "win32") return false;
  return isSglDataUrl(url, { env });
}

function responseLikeFromMeta(meta = {}) {
  const headers = new Map();
  if (meta.content_length !== null && meta.content_length !== undefined) {
    headers.set("content-length", String(meta.content_length));
  }
  if (meta.raw_content_length !== null && meta.raw_content_length !== undefined) {
    headers.set("x-raw-content-length", String(meta.raw_content_length));
  }

  return {
    ok: Number(meta.status_code) >= 200 && Number(meta.status_code) < 300,
    status: Number(meta.status_code) || 0,
    headers: {
      get(name) {
        return headers.get(String(name || "").toLowerCase()) ?? null;
      },
    },
  };
}

function powershellExecutable(env = process.env) {
  return envValue(env, "SGL_FETCH_POWERSHELL_EXE") || envValue(env, "POWERSHELL_EXE") || "powershell.exe";
}

function adapterScriptPath(env = process.env) {
  return envValue(env, "SGL_FETCH_SCRIPT") || path.resolve(__dirname, "..", "sgl_fetch.ps1");
}

function tempOutputPath() {
  const dir = path.join(os.tmpdir(), "ringstatus-sgl-fetch");
  fs.mkdirSync(dir, { recursive: true });
  const name = `sgl-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.json`;
  return path.join(dir, name);
}

function parsePowerShellMetadata(stdout) {
  const lines = String(stdout || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const last = lines[lines.length - 1] || "";
  return JSON.parse(last);
}

async function fetchTextViaPowerShell(url, { env = process.env } = {}) {
  const directUrl = resolveSglDirectUrl(url, { env });
  const outputPath = tempOutputPath();
  const scriptPath = adapterScriptPath(env);
  const exe = powershellExecutable(env);
  const args = [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    scriptPath,
    "-Url",
    directUrl,
    "-OutputPath",
    outputPath,
  ];

  const startedAt = Date.now();
  const result = await new Promise((resolve) => {
    const child = spawn(exe, args, {
      cwd: path.resolve(__dirname, ".."),
      env,
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
    child.on("error", (error) => resolve({ exitCode: -1, stdout, stderr, error }));
    child.on("close", (exitCode) => resolve({ exitCode, stdout, stderr, error: null }));
  });

  let meta = null;
  try {
    meta = parsePowerShellMetadata(result.stdout);
  } catch (error) {
    throw new Error(
      `PowerShell SGL fetch did not return metadata for ${directUrl}: ${String(error?.message || error)} ` +
      `stderr=${String(result.stderr || "").slice(0, 500)}`
    );
  }

  meta.duration_ms = Date.now() - startedAt;
  if (/^soft_payload_/i.test(String(meta.reason || meta.error || ""))) {
    throw new SoftPayloadError(meta.reason || meta.error, {
      reason: meta.reason || meta.error,
      endpoint: directUrl,
      http_status: Number(meta.status_code) || null,
      body_length: meta.body_length ?? null,
      content_length: meta.content_length ?? null,
      raw_content_length: meta.raw_content_length ?? null,
      transport: "powershell_iwr",
      metadata: meta,
    });
  }

  if (result.exitCode !== 0 || !meta.ok) {
    throw new Error(
      `PowerShell SGL fetch failed for ${directUrl}: ` +
      `${meta.error || result.stderr || `exit ${result.exitCode}`}`.slice(0, 800)
    );
  }

  const text = fs.readFileSync(outputPath, "utf8");
  if (String(envValue(env, "SGL_FETCH_KEEP_FILES") || "0") !== "1") {
    fs.rmSync(outputPath, { force: true });
  }

  return {
    text,
    response: responseLikeFromMeta(meta),
    endpoint: directUrl,
    originalEndpoint: url,
    transport: "powershell_iwr",
    metadata: meta,
  };
}

async function fetchTextWithConfiguredTransport(url, nodeFetchText, options = {}) {
  if (!shouldUsePowerShellSglFetch(url, options)) {
    return nodeFetchText(url);
  }
  return fetchTextViaPowerShell(url, options);
}

module.exports = {
  fetchTextViaPowerShell,
  fetchTextWithConfiguredTransport,
  isSglDataUrl,
  resolveSglDirectUrl,
  responseLikeFromMeta,
  shouldUsePowerShellSglFetch,
};
