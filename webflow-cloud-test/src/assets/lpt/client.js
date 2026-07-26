(() => {
  "use strict";

  const root = document.querySelector(".lpt-shell");
  if (!root || root.dataset.lptReady === "true") return;

  root.dataset.lptApp = "";
  root.dataset.lptReady = "true";

  const state = {
    activeElement: null,
    filters: {
      years: new Set(),
      shows: new Set(),
      tags: new Set(),
      favoritesOnly: false
    }
  };

  const slug = (value) => String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");

  const words = (value) => String(value || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);

  const sectionType = (section) => {
    const title = section.querySelector(".lpt-section-title")?.textContent?.trim().toLowerCase() || "";
    if (title.includes("horse")) return "horses";
    if (title.includes("video")) return "videos";
    if (title.includes("class")) return "classes";
    if (title.includes("show") || title.includes("competition")) return "competitions";
    return slug(title) || "content";
  };

  const recordType = (element, fallback) => {
    const declared = element.dataset.lptCardType || element.dataset.lptRecordType;
    if (declared) return declared;
    return fallback === "horses" ? "horse"
      : fallback === "videos" ? "video"
      : fallback === "competitions" ? "competition"
      : fallback === "classes" ? "class"
      : fallback;
  };

  const recordTitle = (element) => (
    element.dataset.lptTitle
    || element.querySelector(".lpt-card-title,.lpt-row-title")?.textContent
    || element.textContent
    || "Detail"
  ).trim();

  const openMode = (type) => {
    if (type === "horse" || type === "video") return "detail";
    const path = location.pathname.toLowerCase();
    if (type === "competition" && (path.includes("lp-shows") || path.includes("lp-riding"))) return "detail";
    if (type === "class" && path.includes("lp-classes")) return "detail";
    return "quick";
  };

  const decorateRecords = () => {
    root.querySelectorAll(".lpt-section").forEach((section) => {
      const laneType = sectionType(section);
      section.dataset.lptSection = laneType;

      section.querySelectorAll(".lpt-card,.lpt-row").forEach((record, index) => {
        const type = recordType(record, laneType);
        record.dataset.lptCard = "";
        record.dataset.lptCardType = type;
        record.dataset.lptRecordId ||= slug(recordTitle(record)) || `${type}-${index + 1}`;
        record.dataset.lptOpen ||= openMode(type);
        record.setAttribute("aria-haspopup", "dialog");
      });
    });
  };

  const laneStep = (track) => {
    const first = track.querySelector(".lpt-card:not([data-lpt-filtered-out])");
    if (!first) return track.clientWidth;
    const gap = parseFloat(getComputedStyle(track).gap) || 0;
    return first.getBoundingClientRect().width + gap;
  };

  const updateLane = (track) => {
    const nav = track.closest(".lpt-section")?.querySelector(".lpt-lane-nav");
    if (!nav) return;
    const max = Math.max(0, track.scrollWidth - track.clientWidth - 2);
    nav.querySelector("[data-lpt-lane-prev]").disabled = track.scrollLeft <= 2;
    nav.querySelector("[data-lpt-lane-next]").disabled = track.scrollLeft >= max;
  };

  const decorateLanes = () => {
    root.querySelectorAll(".lpt-section").forEach((section) => {
      const type = section.dataset.lptSection;
      if (type !== "horses" && type !== "videos") return;

      const track = section.querySelector(".lpt-grid");
      const head = section.querySelector(".lpt-section-head");
      if (!track || !head) return;

      section.dataset.lptLaneType = type;
      track.dataset.lptCardLane = "";
      track.dataset.lptLaneTrack = "";

      let nav = head.querySelector(".lpt-lane-nav");
      if (!nav) {
        nav = document.createElement("div");
        nav.className = "lpt-lane-nav";
        nav.setAttribute("aria-label", `${type} carousel controls`);
        nav.innerHTML = [
          '<button class="lpt-lane-button" type="button" data-lpt-lane-prev>Prev</button>',
          '<button class="lpt-lane-button" type="button" data-lpt-lane-next>Next</button>'
        ].join("");
        head.querySelector(".lpt-section-link")?.remove();
        head.append(nav);
      }

      track.addEventListener("scroll", () => updateLane(track), { passive: true });
      updateLane(track);
    });
  };

  const drawer = root.querySelector(".lpt-drawer");
  const backdrop = document.createElement("div");
  backdrop.className = "lpt-overlay-backdrop";
  backdrop.dataset.lptOverlayClose = "";
  root.append(backdrop);

  const youtubeEmbed = (value) => {
    if (!value) return "";
    try {
      const url = new URL(value, location.origin);
      let id = "";
      if (url.hostname.includes("youtu.be")) id = url.pathname.split("/").filter(Boolean)[0] || "";
      else if (url.pathname.includes("/shorts/") || url.pathname.includes("/embed/")) id = url.pathname.split("/").filter(Boolean).pop() || "";
      else id = url.searchParams.get("v") || "";
      return id ? `https://www.youtube-nocookie.com/embed/${encodeURIComponent(id)}?rel=0` : "";
    } catch {
      return "";
    }
  };

  const setDrawerRecord = (record) => {
    if (!drawer) return;
    const title = recordTitle(record);
    const titleNode = drawer.querySelector(".lpt-drawer-title");
    if (titleNode) titleNode.textContent = title;

    drawer.dataset.lptRecordType = record.dataset.lptCardType;
    drawer.dataset.lptRecordId = record.dataset.lptRecordId;

    drawer.querySelectorAll("[data-lpt-field]").forEach((field) => {
      const key = field.dataset.lptField;
      const value = record.dataset[key] || "";
      field.textContent = value;
      field.closest(".lpt-drawer-row")?.toggleAttribute("hidden", !value);
    });

    drawer.querySelector(".lpt-video-player")?.remove();
    if (record.dataset.lptCardType === "video") {
      const src = youtubeEmbed(record.dataset.lptVideoUrl);
      if (src) {
        const player = document.createElement("iframe");
        player.className = "lpt-video-player";
        player.src = src;
        player.title = title;
        player.allow = "accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share";
        player.allowFullscreen = true;
        drawer.querySelector(".lpt-drawer-head")?.after(player);
      }
    }
  };

  const openDrawer = (record) => {
    if (!drawer) return;
    state.activeElement = record;
    setDrawerRecord(record);
    drawer.classList.remove("is-detail", "is-quick");
    drawer.classList.add(`is-${record.dataset.lptOpen || "quick"}`, "is-open");
    backdrop.classList.add("is-open");
    drawer.setAttribute("aria-modal", "true");
    drawer.setAttribute("role", "dialog");
    drawer.setAttribute("aria-hidden", "false");
    document.body.classList.add("lpt-overlay-open");
    drawer.querySelector(".lpt-drawer-close,button,a,input,select,textarea,[tabindex]:not([tabindex='-1'])")?.focus();
  };

  const closeDrawer = () => {
    if (!drawer?.classList.contains("is-open")) return;
    drawer.classList.remove("is-open");
    backdrop.classList.remove("is-open");
    drawer.setAttribute("aria-hidden", "true");
    document.body.classList.remove("lpt-overlay-open");
    drawer.querySelector(".lpt-video-player")?.remove();
    window.setTimeout(() => drawer.classList.remove("is-detail", "is-quick"), 300);
    state.activeElement?.focus();
    state.activeElement = null;
  };

  const selectedValues = (name) => new Set(
    [...root.querySelectorAll(`[data-lpt-filter="${name}"].is-active,[data-lpt-filter="${name}"][aria-pressed="true"]`)]
      .map((element) => String(element.dataset.lptValue || element.value || element.textContent).trim().toLowerCase())
      .filter(Boolean)
  );

  const matches = (record) => {
    const years = state.filters.years;
    const shows = state.filters.shows;
    const tags = state.filters.tags;
    const recordTags = new Set(words(record.dataset.lptTags));
    const yearOk = !years.size || years.has(String(record.dataset.lptYear || "").toLowerCase());
    const showOk = !shows.size || shows.has(String(record.dataset.lptShow || "").toLowerCase());
    const tagsOk = !tags.size || [...tags].every((tag) => recordTags.has(tag));
    const favoriteOk = !state.filters.favoritesOnly || record.dataset.lptFavorite === "true";
    return yearOk && showOk && tagsOk && favoriteOk;
  };

  const applyFilters = () => {
    state.filters.years = selectedValues("year");
    state.filters.shows = selectedValues("show");
    state.filters.tags = selectedValues("tag");
    state.filters.favoritesOnly = Boolean(
      root.querySelector('[data-lpt-filter="favorite"].is-active,[data-lpt-filter="favorite"][aria-pressed="true"]')
    );

    root.querySelectorAll("[data-lpt-card]").forEach((record) => {
      const hidden = !matches(record);
      record.toggleAttribute("data-lpt-filtered-out", hidden);
      record.setAttribute("aria-hidden", String(hidden));
    });

    root.querySelectorAll("[data-lpt-lane-track]").forEach(updateLane);
    root.dispatchEvent(new CustomEvent("lpt:filters-changed", {
      detail: {
        years: [...state.filters.years],
        shows: [...state.filters.shows],
        tags: [...state.filters.tags],
        favoritesOnly: state.filters.favoritesOnly
      }
    }));
  };

  root.addEventListener("click", (event) => {
    const laneButton = event.target.closest("[data-lpt-lane-prev],[data-lpt-lane-next]");
    if (laneButton) {
      const section = laneButton.closest(".lpt-section");
      const track = section?.querySelector("[data-lpt-lane-track]");
      if (!track) return;
      const direction = laneButton.hasAttribute("data-lpt-lane-prev") ? -1 : 1;
      track.scrollBy({ left: direction * laneStep(track), behavior: "smooth" });
      return;
    }

    const filter = event.target.closest("[data-lpt-filter]");
    if (filter) {
      event.preventDefault();
      const active = filter.getAttribute("aria-pressed") === "true" || filter.classList.contains("is-active");
      filter.classList.toggle("is-active", !active);
      filter.setAttribute("aria-pressed", String(!active));
      applyFilters();
      return;
    }

    if (event.target.closest("[data-lpt-overlay-close],.lpt-drawer-close")) {
      event.preventDefault();
      closeDrawer();
      return;
    }

    const record = event.target.closest("[data-lpt-card]");
    if (record) {
      event.preventDefault();
      openDrawer(record);
      return;
    }

    const navLink = event.target.closest(".lpt-nav a");
    if (navLink) window.scrollTo({ top: 0, behavior: "auto" });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeDrawer();
  });

  window.addEventListener("resize", () => {
    root.querySelectorAll("[data-lpt-lane-track]").forEach(updateLane);
  }, { passive: true });

  decorateRecords();
  decorateLanes();
  applyFilters();

  window.LPT = Object.freeze({
    applyFilters,
    closeDrawer,
    openRecord(recordId) {
      const record = root.querySelector(`[data-lpt-record-id="${CSS.escape(recordId)}"]`);
      if (record) openDrawer(record);
    }
  });
})();
