const VALID_MODES = new Set(["DAY", "NIGHT", "OVERNIGHT", "IDLE", "OFF"]);
const CONTROL_MODES = new Set(["IDLE", "OFF"]);

const DEFAULT_CADENCE_SECONDS = {
  DAY: 180,
  NIGHT: 300,
  OVERNIGHT: 999,
  IDLE: 1800,
  OFF: 3600,
};

function normalizeHeartbeatMode(value, fallback = "DAY") {
  const mode = String(value ?? "").trim().toUpperCase();
  if (VALID_MODES.has(mode)) return mode;

  const fallbackMode = String(fallback ?? "").trim().toUpperCase();
  return VALID_MODES.has(fallbackMode) ? fallbackMode : "DAY";
}

function isHeartbeatControlMode(value) {
  return CONTROL_MODES.has(normalizeHeartbeatMode(value));
}

function modeAllowsHeavy(value) {
  return !isHeartbeatControlMode(value);
}

function positiveInteger(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return null;
  return Math.floor(number);
}

function modeEnvName(mode, suffix) {
  return `${normalizeHeartbeatMode(mode)}_${suffix}`;
}

function cadenceSecondsForMode(mode, env = process.env) {
  const normalized = normalizeHeartbeatMode(mode);
  const explicit =
    positiveInteger(env[modeEnvName(normalized, "CADENCE_SECONDS")]) ??
    positiveInteger(env[modeEnvName(normalized, "INTERVAL_SECONDS")]);

  return explicit ?? DEFAULT_CADENCE_SECONDS[normalized] ?? DEFAULT_CADENCE_SECONDS.DAY;
}

function resolveHeartbeatCadenceSeconds(fields = {}, env = process.env) {
  const mode = normalizeHeartbeatMode(fields.mode);
  return (
    positiveInteger(fields.cadence) ??
    positiveInteger(fields.set_intervals) ??
    cadenceSecondsForMode(mode, env)
  );
}

module.exports = {
  VALID_MODES,
  CONTROL_MODES,
  DEFAULT_CADENCE_SECONDS,
  normalizeHeartbeatMode,
  isHeartbeatControlMode,
  modeAllowsHeavy,
  cadenceSecondsForMode,
  resolveHeartbeatCadenceSeconds,
};
