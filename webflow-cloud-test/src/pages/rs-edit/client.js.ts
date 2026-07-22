import { HOME_BINDINGS } from "../../lib/rs-edit-home-bindings.js";

export const GET = async () => new Response(clientSource, {
  headers: {
    "Content-Type": "application/javascript; charset=utf-8",
    "Cache-Control": "no-store"
  }
});

const clientSource = String.raw`(() => {
  const pageKey = new URLSearchParams(location.search).get("page") || document.documentElement.dataset.rsEditPage || "home";
  const apiRoot = document.documentElement.dataset.rsEditApi || "https://ringstatus.com/test/rs-edit";
  const state = { fields: new Map(), dirty: new Map(), runtimeParents: new Map() };
  const homeBindings = ${JSON.stringify(HOME_BINDINGS)};

  const status = (message, type = "info") => {
    const node = document.querySelector("[data-rs-edit-status]");
    if (node) { node.textContent = message; node.dataset.state = type; }
  };

  function bindText(field, node) {
    node.classList.add("rs-edit-text");
    node.setAttribute("contenteditable", "true");
    node.setAttribute("spellcheck", "true");
    node.textContent = field.textContent;
    node.addEventListener("input", () => state.dirty.set(field.fieldKey, {
      fieldKey: field.fieldKey,
      fieldType: "text",
      textContent: node.textContent || ""
    }));
  }

  function bindTextGroup(fields, node) {
    const textNodes = [...node.childNodes].filter((child) => child.nodeType === Node.TEXT_NODE);
    if (textNodes.length !== fields.length) return false;
    fields.forEach((field, index) => {
      const span = document.createElement("span");
      span.dataset.rsEditKey = field.fieldKey;
      textNodes[index].replaceWith(span);
      bindText(field, span);
    });
    return true;
  }

  function resolveRuntimeBinding(binding) {
    const section = document.querySelector('[mcpid="' + CSS.escape(binding.mcp) + '"]');
    if (!section) return null;
    const walker = document.createTreeWalker(section, NodeFilter.SHOW_TEXT);
    let occurrence = 0;
    while (walker.nextNode()) {
      const textNode = walker.currentNode;
      if (textNode.nodeValue !== binding.text) continue;
      occurrence += 1;
      if (occurrence !== binding.occurrence) continue;
      const parentNode = textNode.parentElement;
      const span = document.createElement("span");
      span.dataset.rsEditKey = binding.fieldKey;
      textNode.replaceWith(span);
      if (parentNode) state.runtimeParents.set(binding.parentElementId, parentNode);
      return span;
    }
    return null;
  }

  function bindColor(field, node) {
    node.classList.add("rs-edit-color");
    const input = document.createElement("input");
    input.type = "color";
    input.value = field.colorHex || "#000000";
    input.setAttribute("aria-label", "Choose background color");
    input.addEventListener("input", () => {
      node.style.backgroundColor = input.value;
      state.dirty.set(field.fieldKey, { fieldKey: field.fieldKey, fieldType: "color", colorHex: input.value });
    });
    node.insertAdjacentElement("afterend", input);
  }

  async function load() {
    status("Loading editable fields…");
    const response = await fetch(apiRoot + "/content?page=" + encodeURIComponent(pageKey), { credentials: "omit" });
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || "load_failed");
    for (const field of result.fields) {
      state.fields.set(field.fieldKey, field);
      const node = document.querySelector('[data-rs-edit-key="' + CSS.escape(field.fieldKey) + '"]');
      if (!node) continue;
      if (field.fieldType === "text") bindText(field, node);
      if (field.fieldType === "color") bindColor(field, node);
    }
    for (const node of document.querySelectorAll("[data-rs-edit-keys]")) {
      const fields = (node.getAttribute("data-rs-edit-keys") || "")
        .split("|").map((key) => state.fields.get(key)).filter(Boolean);
      if (fields.length) bindTextGroup(fields, node);
    }
    if (pageKey === "home") {
      for (const binding of homeBindings) {
        if (document.querySelector('[data-rs-edit-key="' + CSS.escape(binding.fieldKey) + '"]')) continue;
        const field = state.fields.get(binding.fieldKey);
        const node = field && resolveRuntimeBinding(binding);
        if (field && node) bindText(field, node);
      }
      for (const field of result.fields.filter((item) => item.fieldType === "color")) {
        const node = state.runtimeParents.get(field.elementId);
        if (node) bindColor(field, node);
      }
    }
    status(result.count + " approved fields loaded.", "ready");
  }

  async function save() {
    const changes = [...state.dirty.values()];
    if (!changes.length) { status("Nothing changed.", "ready"); return; }
    let saveKey = sessionStorage.getItem("rs-edit-save-key") || "";
    if (!saveKey) saveKey = prompt("Enter the rs-edit save key") || "";
    if (!saveKey) { status("Save cancelled.", "error"); return; }
    sessionStorage.setItem("rs-edit-save-key", saveKey);
    status("Saving edits…");
    const response = await fetch(apiRoot + "/save", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-RS-Edit-Key": saveKey },
      body: JSON.stringify({ pageKey, changes })
    });
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || "save_failed");
    state.dirty.clear();
    status(result.count + " edits saved. No Webflow update or publish was run.", "saved");
  }

  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-rs-edit-save]");
    if (!button) return;
    event.preventDefault();
    save().catch((error) => status("Save failed: " + error.message, "error"));
  });

  load().catch((error) => status("Editor failed: " + error.message, "error"));
})();`;
