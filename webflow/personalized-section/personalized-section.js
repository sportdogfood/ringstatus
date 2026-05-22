(async () => {
  const config = window.RS_PERSONALIZED_CONFIG || {};
  const root = document.querySelector(config.rootSelector || "#rs-personalized-section");
  if (!root) return;
  if (root.dataset.rsPersonalizedMounted === "true") return;
  root.dataset.rsPersonalizedMounted = "true";

  const state = {
    context: normalizeContext(config.context || readContext(root)),
    endpointUrl: config.endpointUrl || "",
    datasetUrl: config.datasetUrl || "./personalized-section-content.json"
  };

  root.innerHTML = loading();

  try {
    const section = state.endpointUrl
      ? await loadFromEndpoint(state)
      : buildSection(await loadJson(state.datasetUrl), state.context);
    render(root, section);
  } catch (error) {
    console.error("[rs-personalized-section]", error);
    root.innerHTML = `<div class="lp-row is-static">Personalized content failed to load.</div>`;
  }

  async function loadFromEndpoint(currentState) {
    const response = await fetch(currentState.endpointUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        datasetUrl: currentState.datasetUrl,
        context: currentState.context
      })
    });
    const result = await response.json();
    if (!response.ok || !result.ok) {
      throw new Error(result.error || `Endpoint failed: ${response.status}`);
    }
    return result;
  }

  async function loadJson(url) {
    const response = await fetch(url);
    const result = await response.json();
    if (!response.ok) throw new Error(`Dataset failed: ${response.status}`);
    return result;
  }

  function buildSection(dataset, context) {
    const seasonKey = slug(context.season || dataset.defaultSeason || firstKey(dataset.seasons));
    const season = dataset.seasons?.[seasonKey] || {};
    const seasonLabel = season.label || titleCase(seasonKey);
    const tagSet = new Set(context.tags.map(slug));
    const activities = (season.activities || [])
      .filter((activity) => {
        const tags = normalizeTags(activity.tags).map(slug);
        if (!tagSet.size) return true;
        return tags.length && tags.every((tag) => tagSet.has(tag));
      })
      .map((activity) => ({
        title: String(activity.title || "").trim(),
        description: String(activity.description || "").trim(),
        tags: normalizeTags(activity.tags)
      }));

    return {
      ok: true,
      season: { key: seasonKey, label: seasonLabel },
      context,
      copy: {
        headline: templateText(season.headline || "I love when its {season} season", seasonLabel),
        intro: templateText(season.intro || "In the {season} I like to do these activities", seasonLabel)
      },
      activities,
      fallback: activities.length ? "" : (dataset.fallback || "No activities matched this profile yet.")
    };
  }

  function render(target, section) {
    const activities = section.activities || [];
    target.innerHTML = `
      <div class="lp-shell rs-personalized-shell">
        <header class="lp-header">
          <div class="lp-header-copy">
            <h2>${escapeHtml(section.copy?.headline || "Personalized picks")}</h2>
            <p class="lp-subtitle">${escapeHtml(section.copy?.intro || "")}</p>
          </div>
        </header>
        <main class="lp-content">
          <section class="lp-panel is-active">
            <section class="lp-section-block packing-theme-horses">
              <div class="lp-section-title packing-section-title">
                <h3>${escapeHtml(section.season?.label || "Activities")}</h3>
              </div>
              <div class="lp-list" data-rs-personalized-list>
                ${activities.length ? activities.map(activityRow).join("") : `<div class="lp-row is-static">${escapeHtml(section.fallback)}</div>`}
              </div>
            </section>
          </section>
        </main>
      </div>
    `;
  }

  function activityRow(activity) {
    return `
      <div class="lp-row packing-row rs-personalized-row">
        <span class="lp-row-title">${escapeHtml(activity.title)}</span>
        <span class="lp-row-meta">${escapeHtml(activity.description)}</span>
      </div>
    `;
  }

  function loading() {
    return `<div class="lp-row is-static">Loading personalized content...</div>`;
  }

  function readContext(target) {
    return {
      season: target.dataset.season || "",
      tags: target.dataset.tags || ""
    };
  }

  function normalizeContext(input) {
    return {
      season: slug(input.season || input.currentSeason || input.current_season || ""),
      tags: normalizeTags(input.tags || input.user_iam || input.userIam)
    };
  }

  function normalizeTags(value) {
    if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
    if (!value) return [];
    return String(value).split(",").map((item) => item.trim()).filter(Boolean);
  }

  function templateText(value, season) {
    return String(value || "").replace(/\{season\}/gi, season);
  }

  function firstKey(value) {
    return Object.keys(value || {})[0] || "";
  }

  function slug(value) {
    return String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  }

  function titleCase(value) {
    return String(value || "")
      .split(/[-_\s]+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
      .join(" ");
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }
})();
