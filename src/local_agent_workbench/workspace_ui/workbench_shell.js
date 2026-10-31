"use strict";

window.WorkbenchShell = ({request, onChoose, onNewTask}) => {
  const ids = ["settings", "global-settings", "settings-open", "settings-close", "model-open",
    "model-picker", "model-close", "model-search", "model-options", "model-empty", "new-from-history",
    "library-connections", "library-models", "library-status", "library-reload", "connection-add",
    "model-add", "library-form", "library-form-title", "library-id", "library-source-field",
    "library-source", "library-model-field", "library-connection", "library-model", "library-save", "library-cancel"];
  const ui = Object.fromEntries(ids.map((id) => [id, document.getElementById(id)]));
  const sections = ["team", "connections", "models", "general"];
  let view = {project: null, agents: [], selected: "", editable: false};
  let library = null, reading = 0, busy = false, uncertain = false, adding = null, opener = null, section = "team";
  const element = (tag, text, className = "") => {
    const node = document.createElement(tag); node.textContent = text; node.className = className; return node;
  };
  const notice = (message = "") => { ui["library-status"].textContent = message; ui["library-status"].hidden = !message; };
  const valid = (data) => data && /^[a-f0-9]{64}$/.test(data.revision) && typeof data.editable === "boolean"
    && Array.isArray(data.connections) && data.connections.length <= 128 && Array.isArray(data.models) && data.models.length <= 128
    && new Set(data.connections.map((c) => c.id)).size === data.connections.length
    && new Set(data.models.map((m) => m.id)).size === data.models.length
    && data.connections.every((c) => typeof c.id === "string" && ["codex", "claude", "local", "openrouter"].includes(c.kind)
      && typeof c.enabled === "boolean" && typeof c.credential_configured === "boolean" && ["loaded", "restart_required"].includes(c.status))
    && data.models.every((m) => typeof m.id === "string" && data.connections.some((c) => c.id === m.connection_id)
      && (m.model === null || typeof m.model === "string") && typeof m.verified === "boolean" && Array.isArray(m.capabilities)
      && ["loaded", "restart_required"].includes(m.status));
  const controls = () => {
    const locked = busy || uncertain || !library?.editable;
    ui["connection-add"].disabled = locked;
    ui["model-add"].disabled = locked || !library?.connections.length;
    ui["library-save"].disabled = locked;
    ui["library-cancel"].disabled = busy;
    ui["library-reload"].disabled = busy;
    for (const id of ["library-id", "library-source", "library-connection", "library-model"]) ui[id].disabled = locked;
    ui["library-form"].hidden = !adding || section !== (adding === "connection" ? "connections" : "models");
  };
  const renderLibrary = () => {
    for (const id of ["library-connections", "library-models"]) ui[id].replaceChildren();
    for (const row of library?.connections || []) {
      const item = element("article", "", "library-row");
      item.append(element("span", row.status === "restart_required" ? "Restart required" : row.enabled ? "Enabled" : "Disabled", "count-pill"),
        element("strong", row.id), element("small", `${row.kind === "openrouter" ? "OpenRouter API" : row.kind === "local" ? "Local inference" : `${row.kind} subscription`}. ${row.credential_configured ? "Credential reference configured" : "Credentials not configured"}.`));
      ui["library-connections"].append(item);
    }
    for (const row of library?.models || []) {
      const item = element("article", "", "library-row");
      item.append(element("span", row.status === "restart_required" ? "Restart required" : row.verified ? "Declared verified" : "Unverified", "count-pill"),
        element("strong", row.model || "Native account default"), element("small", `${row.connection_id} / ${row.id}`));
      ui["library-models"].append(item);
    }
    if (!library?.connections.length) ui["library-connections"].append(element("p", "No connections configured.", "library-empty"));
    if (!library?.models.length) ui["library-models"].append(element("p", "No models configured.", "library-empty"));
    controls();
  };
  const loadLibrary = async () => {
    if (busy) return;
    const epoch = ++reading;
    try {
      const data = await request("/api/settings/library");
      if (epoch !== reading) return;
      if (!valid(data)) throw new Error("Library unavailable. Reload to retry.");
      library = data; renderLibrary();
      if (uncertain) notice("Setup outcome was uncertain. Inspect stored entries before making another change.");
      else notice(data.editable ? "" : "Set up version two runtime configuration to add connections and models.");
    } catch (error) {
      if (epoch === reading) { library = null; renderLibrary(); notice(error.message); }
    }
    ui["library-reload"].hidden = false;
  };
  const settingsTab = (name) => {
    if (!sections.includes(name)) return;
    section = name;
    for (const section of sections) {
      const button = document.getElementById(`settings-tab-${section}`);
      button.setAttribute("aria-selected", String(section === name)); button.tabIndex = section === name ? 0 : -1;
      document.getElementById(`settings-panel-${section}`).hidden = section !== name;
    }
    controls();
    if (["connections", "models"].includes(name)) loadLibrary();
    else ui["library-status"].hidden = true;
    ui["library-reload"].hidden = !["connections", "models"].includes(name);
  };
  const closeSetup = () => {
    const kind = adding;
    adding = null; controls();
    if (kind) ui[kind === "connection" ? "connection-add" : "model-add"].focus();
  };
  const openSetup = (kind) => {
    if (!library?.editable || busy || uncertain) return;
    if (adding === kind) { controls(); ui["library-id"].focus(); return; }
    adding = kind; ui["library-form-title"].textContent = kind === "connection" ? "New connection" : "New model";
    ui["library-id"].value = ""; ui["library-model"].value = "";
    ui["library-source-field"].hidden = kind !== "connection"; ui["library-model-field"].hidden = kind !== "model";
    ui["library-connection"].replaceChildren();
    for (const row of library.connections) {
      const option = element("option", row.id); option.value = row.id; ui["library-connection"].append(option);
    }
    if (library.connections.length) ui["library-connection"].value = library.connections[0].id;
    controls(); ui["library-id"].focus();
  };
  const saveSetup = async (event) => {
    event.preventDefault();
    if (busy || uncertain || !library?.editable || !adding) return;
    const entry = {id: ui["library-id"].value.trim()};
    if (adding === "connection") entry.kind = ui["library-source"].value;
    else { entry.connection_id = ui["library-connection"].value; entry.model = ui["library-model"].value.trim() || null; }
    const kind = adding, revision = library.revision;
    busy = true; reading++; controls(); notice("Saving setup…");
    try {
      const data = await request("/api/settings/library", {revision, kind, entry});
      const row = (kind === "connection" ? data?.connections : data?.models)?.find((r) => r.id === entry.id);
      if (!valid(data) || data.revision === revision || !row || row.status !== "restart_required"
        || (kind === "connection" ? row.enabled || row.kind !== entry.kind : row.verified || row.model !== entry.model || row.connection_id !== entry.connection_id)) throw new Error("Save outcome is unreadable. Reload to inspect stored setup.");
      library = data; closeSetup(); renderLibrary(); notice("Saved inactive setup. Restart and configure locally before use.");
    } catch (error) {
      uncertain = ![409, 422].includes(error.status);
      if (uncertain) notice(`${error.message} No automatic retry.`);
      else { library = null; notice(`${error.message} Reload before saving again.`); }
      ui["library-reload"].hidden = false;
    } finally {
      busy = false; controls();
      if (!adding && ui.settings.open && section === (kind === "connection" ? "connections" : "models")) {
        ui[kind === "connection" ? "connection-add" : "model-add"].focus();
      }
    }
  };
  const renderModels = () => {
    ui["model-options"].replaceChildren();
    const query = ui["model-search"].value.trim().toLocaleLowerCase(), project = view.project;
    const rows = [{id: "", model: "Record only", runtime: "No worker"}, ...view.agents];
    for (const row of rows) {
      if (query && !`${row.id} ${row.model || "Native account default"} ${row.runtime}`.toLocaleLowerCase().includes(query)) continue;
      const button = element("button", "", "model-option"); button.type = "button";
      button.setAttribute("aria-pressed", String(row.id === view.selected));
      button.append(element("strong", row.model || "Native account default"), element("small", row.id ? `${row.runtime} / ${row.id}` : "Store a request without selecting a worker"));
      button.addEventListener("click", () => {
        if (view.project !== project || !view.editable || (row.id && !view.agents.some((a) => a.id === row.id))) return;
        onChoose(row.id); ui["model-picker"].close();
      });
      ui["model-options"].append(button);
    }
    ui["model-empty"].hidden = ui["model-options"].children.length !== 0;
  };
  for (const [index, section] of sections.entries()) {
    const button = document.getElementById(`settings-tab-${section}`);
    button.addEventListener("click", () => settingsTab(section));
    button.addEventListener("keydown", (event) => {
      const next = ["ArrowDown", "ArrowRight"].includes(event.key) ? (index + 1) % sections.length
        : ["ArrowUp", "ArrowLeft"].includes(event.key) ? (index + sections.length - 1) % sections.length
        : event.key === "Home" ? 0 : event.key === "End" ? sections.length - 1 : null;
      if (next !== null) { event.preventDefault(); settingsTab(sections[next]); document.getElementById(`settings-tab-${sections[next]}`).focus(); }
    });
  }
  ui["global-settings"].addEventListener("click", () => { opener = ui["global-settings"]; settingsTab("connections"); ui.settings.showModal(); });
  ui.settings.addEventListener("close", () => { (opener || ui["settings-open"]).focus(); });
  ui["connection-add"].addEventListener("click", () => openSetup("connection"));
  ui["model-add"].addEventListener("click", () => openSetup("model"));
  ui["library-cancel"].addEventListener("click", closeSetup);
  ui["library-form"].addEventListener("submit", saveSetup);
  ui["library-reload"].addEventListener("click", () => { uncertain = false; loadLibrary(); });
  ui["model-open"].addEventListener("click", () => {
    if (!view.editable) return;
    ui["model-search"].value = ""; renderModels(); ui["model-picker"].showModal(); ui["model-search"].focus();
  });
  ui["model-close"].addEventListener("click", () => ui["model-picker"].close());
  ui["model-picker"].addEventListener("close", () => ui["model-open"].focus());
  ui["model-search"].addEventListener("input", renderModels);
  ui["new-from-history"].addEventListener("click", onNewTask);
  return {
    settingsTab(name) { opener = ui["settings-open"]; settingsTab(name); },
    update(next) {
      if (view.project !== next.project && ui["model-picker"].open) ui["model-picker"].close();
      view = next;
      const selected = view.agents.find((a) => a.id === view.selected);
      ui["model-open"].disabled = !view.editable;
      ui["model-open"].textContent = `Model: ${view.selected ? selected?.model || (selected ? "Native account default" : "Profile unavailable") : "Record only"}`;
      if (ui["model-picker"].open) renderModels();
    },
  };
};
