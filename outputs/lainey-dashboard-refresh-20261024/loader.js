(function () {
"use strict";
if (window.LPEngine && window.LPEngine.version === "1.2.9") return;
var VERSION = "1.2.9";
var SCHEMA_VERSION = "lp-usef-classes-v1.6";
var CACHE_KEY = "lp-usef-classes-v1";
var SESSION_KEY = "lp-engine-session-fingerprint-v1";
var SOURCE_PATH = "/engine-data";
var CACHE_TTL_MS = 12 * 60 * 60 * 1000;
var REQUIRED_REFERENCES = ["horses", "competitions", "heights", "years", "disciplines", "skills", "results", "riders", "playlists"];
var state = "idle";
var data = null;
var index = null;
var pending = null;
function clean(value) {
return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}
function normalize(value) {
return clean(value)
.toLowerCase()
.replace(/&/g, " and ")
.replace(/[^a-z0-9]+/g, " ")
.trim()
.replace(/\s+/g, " ");
}
function typeKey(value) {
return normalize(value).replace(/\s+/g, "-");
}
function text(root, selector) {
var node = root && root.querySelector(selector);
return node ? clean(node.textContent) : "";
}
function attributeOrText(root, selector, attributeName) {
if (!root) return "";
var node = root.matches && root.matches(selector) ? root : root.querySelector(selector);
return node ? clean(node.getAttribute(attributeName) || node.textContent) : "";
}
function texts(root, selector) {
return Array.prototype.map.call(
root ? root.querySelectorAll(selector) : [],
function (node) { return clean(node.textContent); }
).filter(Boolean);
}
function uniqueText(values) {
var seen = Object.create(null);
return values.filter(function (value) {
var normalized = normalize(value);
if (!normalized || seen[normalized]) return false;
seen[normalized] = true;
return true;
});
}
function uniqueKeys(values) {
var seen = Object.create(null);
return values.filter(function (value) {
var canonical = clean(value);
if (!canonical || seen[canonical]) return false;
seen[canonical] = true;
return true;
});
}
function matchToken(value) {
return clean(value).toLowerCase();
}
function uniqueMatchText(values) {
var seen = Object.create(null);
return values.map(clean).filter(function (value) {
var token = matchToken(value);
if (!token || seen[token]) return false;
seen[token] = true;
return true;
});
}
function numeric(value) {
var match = clean(value).match(/-?\d+(?:\.\d+)?/);
return match ? Number(match[0]) : null;
}
function isoDate(value) {
var timestamp = Date.parse(clean(value));
return Number.isFinite(timestamp) ? new Date(timestamp).toISOString().slice(0, 10) : "";
}
function hash(value) {
var result = 2166136261;
for (var i = 0; i < value.length; i += 1) {
result ^= value.charCodeAt(i);
result = Math.imul(result, 16777619);
}
return (result >>> 0).toString(36);
}
function readLink(root, selector) {
var node = root && root.querySelector(selector);
return node ? clean(node.getAttribute("href") || node.textContent) : "";
}
function referenceColor(node) {
var colorNode = node && node.querySelector("[data-ref-color]");
if (!colorNode) return "";
var direct = clean(colorNode.getAttribute("data-ref-color") || colorNode.textContent);
if (direct) return direct;
var style = clean(colorNode.getAttribute("style"));
var match = style.match(/(?:background-color|color)\s*:\s*(#[0-9a-f]{3,8}|rgba?\([^)]*\))/i);
return match ? clean(match[1]) : "";
}
function parseReferenceItem(node, type, position) {
var refKey = text(node, "[data-ref-key]");
if (!refKey) return null;
return {
id: type + "-" + refKey,
key: refKey,
sourceKey: refKey,
label: text(node, "[data-ref-label]"),
display: text(node, "[data-ref-display]"),
displaySafe: text(node, "[data-ref-display-safe]"),
showName: text(node, "[data-ref-show-name]"),
aliasesText: text(node,"[data-ref-aliases]"),
aliases: uniqueText(text(node,"[data-ref-aliases]").split(/[,;|]/)),
sort: numeric(text(node, "[data-ref-sort]")),
usef: text(node, "[data-ref-usef]"),
usefLink: readLink(node, "[data-ref-usef-link]"),
usefLinkText: text(node, "[data-ref-usef-link-text]"),
hasVideo: text(node, "[data-ref-has-video]"),
favorite: text(node, "[data-ref-favorite]"),
playlistId:text(node,"[data-ref-playlist-id]"),horseKey:text(node,"[data-ref-horse]"),cmsLink:readLink(node,"[data-cms-link]"),cmsLinkText:text(node,"[data-cms-link-text]"),playlistUrl:text(node,"[data-ref-link-text]"),
color: referenceColor(node),
sourcePosition: position
};
}
function parseAggregateItem(node,position) {
var key=text(node,"[data-agg-metric-key]");
if(!key)return null;
var colorNode=node.querySelector("[data-agg-color]");
var color="";
if(colorNode){color=clean(colorNode.getAttribute("data-agg-color")||colorNode.textContent);if(!color){var match=clean(colorNode.getAttribute("style")).match(/(?:background-color|color)\s*:\s*(#[0-9a-f]{3,8}|rgba?\([^)]*\))/i);color=match?clean(match[1]):""}}
return{id:"aggs-"+key,key:key,sourceKey:text(node,"[data-agg-source-key]"),label:text(node,"[data-agg-label]"),display:text(node,"[data-agg-label]"),displaySafe:"",showName:"",aliases:[],sort:null,usef:"",usefLink:"",usefLinkText:"",hasVideo:"",favorite:"",color:color,calculation:text(node,"[data-agg-calculation]"),outputId:text(node,"[data-agg-output-id]"),sourcePosition:position};
}
function parseReferences(doc) {
var groups = Object.create(null);
doc.querySelectorAll("[data-lp-reference]").forEach(function (list) {
var type = typeKey(list.getAttribute("data-lp-reference"));
if (!type || type === "classes") return;
if (!groups[type]) groups[type] = [];
list.querySelectorAll('[data-ref-item="' + type + '"]').forEach(function (node) {
var item=type==="aggs"?parseAggregateItem(node,groups[type].length):parseReferenceItem(node,type,groups[type].length);
if (item) groups[type].push(item);
});
});
REQUIRED_REFERENCES.forEach(function (type) {
if (!groups[type]) groups[type] = [];
});
return groups;
}
function buildReferenceLookups(references) {
return Object.keys(references).reduce(function (result, type) {
var lookup = Object.create(null);
references[type].forEach(function (item) {
uniqueMatchText([item.key, item.label])
.forEach(function (candidate) {
var token = matchToken(candidate);
if (!lookup[token]) lookup[token] = [];
if (lookup[token].indexOf(item) === -1) lookup[token].push(item);
});
});
result[type] = lookup;
return result;
}, Object.create(null));
}
function noteResolution(diagnostics, type, status, values) {
if (!diagnostics[type]) diagnostics[type] = { unresolved: 0, ambiguous: 0, samples: [] };
diagnostics[type][status] += 1;
var sample = uniqueText(values).join(" | ");
if (sample && diagnostics[type].samples.length < 10 && diagnostics[type].samples.indexOf(sample) === -1) {
diagnostics[type].samples.push(sample);
}
}
function resolveOne(values, type, lookups, diagnostics) {
var candidates = uniqueMatchText(values);
if (!candidates.length) return null;
var lookup = lookups[type] || Object.create(null);
var matches = [];
candidates.forEach(function (candidate) {
(lookup[matchToken(candidate)] || []).forEach(function (item) {
if (matches.indexOf(item) === -1) matches.push(item);
});
});
if (matches.length === 1) return matches[0];
if (matches.length > 1) noteResolution(diagnostics, type, "ambiguous", candidates);
return null;
}
function resolveMany(values, type, lookups, diagnostics) {
return uniqueMatchText(values).map(function (value) {
return resolveOne([value], type, lookups, diagnostics);
}).filter(Boolean);
}
function parseClasses(doc, references) {
var duplicateCounts = Object.create(null);
var diagnostics = Object.create(null);
var lookups = buildReferenceLookups(references);
var classes = Array.prototype.map.call(
doc.querySelectorAll("[data-lp-class-row]"),
function (node, position) {
var classRecId = attributeOrText(node, "[data-class-rec-id]", "data-class-rec-id");
var usefId = attributeOrText(node, "[data-ref-usef]", "data-ref-usef");
var youtubeId = attributeOrText(node, "[data-class-youtubeId]", "data-class-youtubeId");
var title = text(node, "[data-class-title]");
var horse = text(node, "[data-class-horse]");
var rider = text(node, "[data-class-rider]");
var dateRaw = text(node, "[data-class-date]");
var resultRaw = text(node, "[data-class-result]");
var resultLabel = text(node, "[data-class-result-ref]");
var competition = text(node, "[data-class-competition]");
var height = text(node, "[data-class-height]");
var discipline = text(node, "[data-class-discipline]");
var skill = text(node, "[data-class-skill]");
var yearRaw = text(node, "[data-class-year]");
var disciplineRefs = uniqueMatchText(texts(node, "[data-class-discipline-ref]"));
var skillRefs = uniqueMatchText(texts(node, "[data-class-skill-ref]"));
var date = isoDate(dateRaw);
var yearCandidate = yearRaw;
var horseRef = resolveOne([horse], "horses", lookups, diagnostics);
var riderRef = resolveOne([rider], "riders", lookups, diagnostics);
var competitionRef = resolveOne([competition], "competitions", lookups, diagnostics);
var resultRef = resolveOne([resultLabel], "results", lookups, diagnostics);
var heightRef = resolveOne([height], "heights", lookups, diagnostics);
var yearRef = resolveOne([yearCandidate], "years", lookups, diagnostics);
var disciplineMatches = resolveMany(disciplineRefs, "disciplines", lookups, diagnostics);
var skillMatches = resolveMany(skillRefs, "skills", lookups, diagnostics);
var place = resultRef && Number.isFinite(resultRef.sort) && resultRef.sort >= 1 && resultRef.sort <= 8 ? resultRef.sort : null;
var signature = [
classRecId, usefId, youtubeId, title, horse, rider, dateRaw, resultRaw, resultLabel, competition,
height, discipline, skill, yearRaw,
disciplineRefs.join("|"), skillRefs.join("|")
].join("\u001f");
var signatureHash = hash(signature);
var duplicateNumber = (duplicateCounts[signatureHash] || 0) + 1;
duplicateCounts[signatureHash] = duplicateNumber;
return {
id: "class-" + signatureHash + "-" + duplicateNumber,
classRecId: classRecId,
usefId: usefId,
youtubeId: youtubeId,
sourcePosition: position,
title: title,
usef: text(node,"[data-ref-usef]"),
usefLink: readLink(node,"[data-ref-usef-link]"),
usefLinkText: text(node,"[data-ref-usef-link-text]"),
horse: horse,
horseKey: horseRef ? horseRef.key : "",
rider: rider,
riderKey: riderRef ? riderRef.key : "",
date: date,
dateRaw: dateRaw,
dateMs: date ? Date.parse(date + "T00:00:00Z") : null,
year: yearRef ? clean(yearRef.display || yearRef.label || yearRef.sourceKey) : "",
yearKey: yearRef ? yearRef.key : "",
result: resultRaw,
resultLabel: resultLabel,
resultKey: resultRef ? resultRef.key : "",
place: place,
competition: competition,
competitionKey: competitionRef ? competitionRef.key : "",
height: height,
heightKey: heightRef ? heightRef.key : "",
discipline: discipline,
disciplineKey: disciplineMatches.length ? disciplineMatches[0].key : "",
disciplineRefs: disciplineRefs,
disciplineRefKeys: uniqueKeys(disciplineMatches.map(function (item) { return item.key; })),
skill: skill,
skillKey: skillMatches.length ? skillMatches[0].key : "",
skillRefs: skillRefs,
skillRefKeys: uniqueKeys(skillMatches.map(function (item) { return item.key; }))
};
}
);
return { classes: classes, diagnostics: diagnostics };
}
function freezeRows(rows) {
rows.forEach(function (row) {
Object.keys(row).forEach(function (name) {
if (Array.isArray(row[name])) Object.freeze(row[name]);
});
Object.freeze(row);
});
return Object.freeze(rows);
}
function parseDocument(doc) {
var source = doc.querySelector(".lp-data-collections");
if (!source) throw new Error("LPEngine: .lp-data-collections was not found");
var references = parseReferences(source);
var parsed = parseClasses(source, references);
var classes = parsed.classes;
if (!classes.length) throw new Error("LPEngine: no class records were found");
Object.keys(references).forEach(function (type) {
references[type] = freezeRows(references[type]);
});
var sourceFingerprint = hash(JSON.stringify({ classes: classes, references: references }));
return Object.freeze({
schemaVersion: SCHEMA_VERSION,
sourceFingerprint: sourceFingerprint,
source: SOURCE_PATH,
loadedAt: new Date().toISOString(),
classes: freezeRows(classes),
references: Object.freeze(references),
resolution: Object.freeze(parsed.diagnostics),
counts: Object.freeze({
classes: classes.length,
references: Object.freeze(Object.keys(references).reduce(function (result, type) {
result[type] = references[type].length;
return result;
}, Object.create(null)))
})
});
}
function append(bucket, bucketKey, row) {
if (!bucketKey) return;
if (!bucket[bucketKey]) bucket[bucketKey] = [];
bucket[bucketKey].push(row);
}
function freezeBucket(bucket) {
Object.keys(bucket).forEach(function (bucketKey) {
Object.freeze(bucket[bucketKey]);
});
return Object.freeze(bucket);
}
function buildIndex(classes) {
var byId = Object.create(null);
var byHorse = Object.create(null);
var byYear = Object.create(null);
var byResult = Object.create(null);
var byHeight = Object.create(null);
var byDiscipline = Object.create(null);
var byRider = Object.create(null);
var byCompetition = Object.create(null);
var bySkill = Object.create(null);
classes.forEach(function (row) {
byId[row.id] = row;
append(byHorse, row.horseKey, row);
append(byYear, row.yearKey, row);
append(byResult, row.resultKey, row);
append(byHeight, row.heightKey, row);
append(byRider,row.riderKey,row);
append(byCompetition,row.competitionKey,row);
row.disciplineRefKeys.forEach(function (value) { append(byDiscipline, value, row); });
row.skillRefKeys.forEach(function (value) { append(bySkill,value,row); });
});
return Object.freeze({
byId: Object.freeze(byId),
byHorse: freezeBucket(byHorse),
byYear: freezeBucket(byYear),
byResult: freezeBucket(byResult),
byHeight: freezeBucket(byHeight),
byDiscipline: freezeBucket(byDiscipline),
byRider: freezeBucket(byRider),
byCompetition: freezeBucket(byCompetition),
bySkill: freezeBucket(bySkill)
});
}
function validCache(payload, allowExpired) {
if (!payload || payload.schemaVersion !== SCHEMA_VERSION || payload.engineVersion !== VERSION) return false;
if (!Number.isFinite(payload.savedAt)) return false;
if (!clean(payload.sourceFingerprint)) return false;
if (!payload.data || payload.data.sourceFingerprint !== payload.sourceFingerprint) return false;
if (!allowExpired && Date.now() - payload.savedAt > CACHE_TTL_MS) return false;
return !!(payload.data && Array.isArray(payload.data.classes) && payload.data.classes.length);
}
function readCache(allowExpired) {
try {
var payload = JSON.parse(localStorage.getItem(CACHE_KEY) || "null");
return validCache(payload, allowExpired) ? payload : null;
} catch (error) {
return null;
}
}
function writeCache(nextData) {
try {
var filtered = null;
try { filtered = JSON.parse(localStorage.getItem("lp:classes-filtered") || "null"); } catch (error) {}
if (!filtered || filtered.sourceFingerprint !== nextData.sourceFingerprint) {
localStorage.removeItem("lp:classes-filtered");
}
localStorage.setItem(CACHE_KEY, JSON.stringify({
schemaVersion: SCHEMA_VERSION,
engineVersion: VERSION,
savedAt: Date.now(),
sourceFingerprint: nextData.sourceFingerprint,
data: nextData
}));
} catch (error) {
window.dispatchEvent(new CustomEvent("lp:engine-cache-error", { detail: { error: error } }));
}
}
function readSessionFingerprint() {
try { return sessionStorage.getItem(SESSION_KEY) || ""; } catch (error) { return ""; }
}
function writeSessionFingerprint(value) {
try { sessionStorage.setItem(SESSION_KEY, value); } catch (error) {}
}
function hydrate(nextData, detail) {
data = nextData;
index = buildIndex(data.classes);
state = "ready";
window.dispatchEvent(new CustomEvent("lp:engine-ready", {
detail: Object.assign({ engine: api, count: data.classes.length }, detail || {})
}));
return api;
}
function fetchSource() {
return fetch(SOURCE_PATH, { credentials: "same-origin", cache: "no-store" })
.then(function (response) {
if (!response.ok) throw new Error("LPEngine: source returned HTTP " + response.status);
return response.text();
})
.then(function (html) {
var doc = new DOMParser().parseFromString(html, "text/html");
var nextData = parseDocument(doc);
writeCache(nextData);
writeSessionFingerprint(nextData.sourceFingerprint);
return hydrate(nextData, { source: "network", stale: false, fingerprint: nextData.sourceFingerprint });
});
}
function load(options) {
var force = !!(options && options.force);
if (!force && state === "ready" && data) return Promise.resolve(api);
if (pending) return pending;
state = "loading";
pending = fetchSource().catch(function (error) {
state = "error";
window.dispatchEvent(new CustomEvent("lp:engine-error", { detail: { error: error } }));
throw error;
}).finally(function () {
pending = null;
});
return pending;
}
function refresh() {
return load({ force: true });
}
function clearCache() {
try {
localStorage.removeItem(CACHE_KEY);
localStorage.removeItem("lp:classes-filtered");
localStorage.removeItem("horse-fav-aggs");
sessionStorage.removeItem(SESSION_KEY);
} catch (error) {}
data = null;
index = null;
pending = null;
state = "idle";
}
var api = {
version: VERSION,
schemaVersion: SCHEMA_VERSION,
source: SOURCE_PATH,
cacheKey: CACHE_KEY,
cacheTtlMs: CACHE_TTL_MS,
load: load,
refresh: refresh,
clearCache: clearCache,
normalizeKey: normalize
};
Object.defineProperties(api, {
ready: { enumerable: true, get: function () { return load(); } },
state: { enumerable: true, get: function () { return state; } },
data: { enumerable: true, get: function () { return data; } },
index: { enumerable: true, get: function () { return index; } }
});
window.LPEngine = api;
document.documentElement.setAttribute("data-lp-engine-autostart", "pending");
load().then(function () {
document.documentElement.setAttribute("data-lp-engine-autostart", "ready");
}).catch(function () {
document.documentElement.setAttribute("data-lp-engine-autostart", "error");
});
})();

