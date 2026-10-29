"use strict";

window.WorkbenchProjectControl = ({request, onSelect, onSaved}) => {
  const ui = Object.fromEntries(["settings", "settings-open", "settings-close", "settings-project",
    "settings-status", "team-enabled", "team-members", "team-save", "team-reload", "connection-models",
    "project-team", "attempt-list", "attention-list", "attempt-status", "attention-count"]
    .map((id) => [id, document.getElementById(id)]));
  const drafts = new Map(), pending = new Set(), held = new Map();
  let project = null, epoch = 0, read = 0, activityRead = 0, settings = null, memberInputs = [];
  const element = (tag, text, className = "") => {
    const node = document.createElement(tag); node.textContent = text; node.className = className; return node;
  };
  const valid = (value, id) => value?.project_id === id && Array.isArray(value.profiles)
    && (value.team === null || (Number.isSafeInteger(value.revision) && value.revision > 0
      && value.team?.project_id === id && typeof value.team.enabled === "boolean"
      && Array.isArray(value.team.agent_ids) && value.team.agent_ids.every((v) => typeof v === "string")))
    && value.profiles.every((p) => typeof p?.id === "string" && typeof p.role === "string"
      && typeof p.runtime === "string" && typeof p.connection_id === "string"
      && typeof p.model_id === "string" && typeof p.eligible === "boolean"
      && typeof p.executable === "boolean" && typeof p.reason === "string");
  const sameMembers = (a, b) => a.length === b.length && [...a].sort().every((v, i) => v === [...b].sort()[i]);
  const controls = () => {
    const blockedLead = settings?.team?.lead_id && !settings.profiles.some((p) => p.id === settings.team.lead_id && p.eligible);
    const draft = drafts.get(project), locked = !settings?.team || pending.has(project) || held.has(project) || blockedLead;
    ui["team-enabled"].disabled = locked;
    ui["team-save"].disabled = locked || !draft?.dirty || !draft.ids.length;
    ui["team-reload"].disabled = !project || pending.has(project);
    for (const input of memberInputs) input.disabled = locked || input.dataset.lead === "true"
      || (input.dataset.eligible !== "true" && !input.checked);
    ui["settings-status"].textContent = held.get(project)?.message || (pending.has(project)
      ? "Saving one change. Closing this dialog does not cancel it."
      : blockedLead ? "Lead unavailable. Restore or reassign it in runtime configuration before saving this team."
      : !settings ? "Reading project settings…" : !settings.team ? "No project team is configured. Add it in the runtime configuration."
      : draft?.dirty ? "Unsaved changes. Removing members or disabling requests cancellation of their owned work."
      : `Stored team revision ${settings.revision}. Edits affect this project only.`);
  };
  const render = () => {
    ui["settings-project"].textContent = project ? `Project ${project}` : "Choose a project";
    ui["settings-open"].disabled = !project;
    ui["team-members"].replaceChildren(); ui["connection-models"].replaceChildren(); ui["project-team"].replaceChildren();
    memberInputs = [];
    const draft = drafts.get(project);
    ui["team-enabled"].checked = draft?.enabled || false;
    for (const profile of settings?.profiles || []) {
      const member = settings.team?.agent_ids.includes(profile.id);
      if (member) ui["project-team"].append(element("li", `${profile.id} · ${profile.role.replaceAll("_", " ")} · Configured${profile.executable ? ", native supported" : ", unavailable"}`));
      const label = document.createElement("label"), input = document.createElement("input");
      input.type = "checkbox"; input.value = profile.id; input.checked = draft?.ids.includes(profile.id) || false;
      input.dataset.eligible = String(profile.eligible); input.setAttribute("aria-label", `Include ${profile.id}`);
      input.dataset.lead = String(settings.team?.lead_id === profile.id);
      memberInputs.push(input);
      input.addEventListener("change", () => {
        if (pending.has(project) || held.has(project)) return;
        const value = drafts.get(project);
        value.ids = input.checked ? [...new Set([...value.ids, profile.id])] : value.ids.filter((id) => id !== profile.id);
        value.dirty = true; controls();
      });
      label.append(input, element("span", `${profile.id} · ${profile.role.replaceAll("_", " ")} · ${profile.runtime}`),
        element("small", settings.team?.lead_id === profile.id
          ? "Designated Lead. Change its assignment in runtime configuration before removing it." : profile.reason)); ui["team-members"].append(label);
    }
    const seen = new Set();
    for (const profile of settings?.profiles || []) {
      if (!profile.connection_id) continue;
      const key = `${profile.connection_id}/${profile.model_id}`;
      if (seen.has(key)) continue; seen.add(key);
      ui["connection-models"].append(element("li", `${profile.connection_id} · ${profile.runtime} · ${profile.model || "Native account default"}. ${profile.connection_enabled ? "Configured enabled" : "Configured disabled"}; authentication and quota not checked.`));
    }
    if (!ui["project-team"].children.length) ui["project-team"].append(element("li", "No configured team members available."));
    controls();
  };
  const loadSettings = async (reset = false) => {
    if (!project || pending.has(project)) return;
    const id = project, version = epoch, reading = ++read;
    try {
      const value = await request(`/api/projects/${encodeURIComponent(id)}/settings`);
      if (id !== project || version !== epoch || reading !== read) return;
      if (!valid(value, id)) throw new Error("Project settings response is unreadable.");
      const blocked = held.get(id), desired = drafts.get(id);
      if (blocked?.kind === "uncertain" && value.team && desired && value.revision > desired.revision
        && value.team.enabled === desired.enabled && sameMembers(value.team.agent_ids, desired.ids)) {
        held.delete(id); drafts.delete(id); onSaved(id);
      } else if (reset && blocked?.kind !== "uncertain") { held.delete(id); drafts.delete(id); }
      const unchanged = JSON.stringify(settings) === JSON.stringify(value);
      settings = value;
      if (value.team && (!drafts.has(id) || (!drafts.get(id).dirty && !held.has(id)))) drafts.set(id, {revision: value.revision,
        enabled: value.team.enabled, ids: [...value.team.agent_ids], dirty: false});
      if (unchanged && !reset) controls(); else render();
    } catch (error) {
      if (id === project && version === epoch && reading === read) {
        settings = null; render(); ui["settings-status"].textContent = error.message;
      }
    }
  };
  const save = async () => {
    if (ui["team-save"].disabled || !project) return;
    const id = project, draft = drafts.get(id), payload = {revision: draft.revision, enabled: draft.enabled, agent_ids: [...draft.ids]};
    pending.add(id); read++; controls();
    try {
      const value = await request(`/api/projects/${encodeURIComponent(id)}/team/members`, payload);
      if (!valid(value, id) || value.revision !== payload.revision + 1
        || value.team?.enabled !== payload.enabled || !sameMembers(value.team.agent_ids, payload.agent_ids)) throw new Error("Save response is unreadable; read stored state before another save.");
      drafts.delete(id); held.delete(id); onSaved(id);
    } catch (error) {
      held.set(id, {kind: error.status === 409 || error.status === 422 ? "conflict" : "uncertain",
        message: error.status === 409 || error.status === 422
          ? `${error.message} Unsaved edits retained. Reload stored settings before editing again.`
          : `${error.message} Save outcome is unknown. Reload to reconcile; it will not be repeated automatically.`});
    } finally {
      pending.delete(id);
      if (id === project) { controls(); await loadSettings(); }
    }
  };
  const loadActivity = async () => {
    if (!project) return;
    const id = project, version = epoch, reading = ++activityRead;
    try {
      const value = await request(`/api/projects/${encodeURIComponent(id)}/activity`);
      if (id !== project || version !== epoch || reading !== activityRead) return;
      const attentionStates = ["waiting_input", "waiting_approval", "review_ready", "blocked", "failed"];
      const time = (v) => typeof v === "string" && Number.isFinite(Date.parse(v));
      const list = (rows, total, truncated) => Array.isArray(rows) && rows.length <= 50
        && Number.isSafeInteger(total) && total >= rows.length && typeof truncated === "boolean"
        && truncated === (total > rows.length);
      const owned = (r) => r?.project_id === id && typeof r.job_id === "string" && r.job_id.length > 0
        && typeof r.title === "string";
      if (value?.project_id !== id || value.basis !== "recorded_native_attempts"
        || !time(value.observed_at)
        || !list(value.attention, value.attention_total, value.attention_truncated)
        || !list(value.attempts, value.attempt_total, value.attempts_truncated)
        || value.attention.some((r) => !owned(r) || !attentionStates.includes(r.state))
        || value.attempts.some((r) => !owned(r) || typeof r.attempt_id !== "string" || !r.attempt_id
          || typeof r.profile_id !== "string" || typeof r.runtime !== "string" || !time(r.last_event_at)
          || typeof r.cancel_requested !== "boolean" || typeof r.requires_reconciliation !== "boolean"
          || ![null, "completed", "failed", "cancelled"].includes(r.outcome)
          || !["admitted", "running", "reconcile", "settled"].includes(r.phase))) throw new Error("Recorded activity response is unreadable.");
      const focused = document.activeElement?.dataset.attempt || document.activeElement?.dataset.attentionJob;
      ui["attempt-list"].replaceChildren(); ui["attention-list"].replaceChildren();
      ui["attention-count"].textContent = `Needs you ${value.attention_total}`;
      const add = (target, row, detail) => {
        const button = element("button", "", "task"); button.type = "button";
        button.dataset.attempt = row.attempt_id || ""; button.dataset.attentionJob = row.job_id;
        button.append(element("span", row.title), element("small", detail));
        button.addEventListener("click", () => onSelect(row.project_id, row.job_id)); target.append(button);
        if (focused && [row.attempt_id, row.job_id].includes(focused)) button.focus({preventScroll: true});
      };
      for (const row of value.attention) add(ui["attention-list"], row, row.state.replaceAll("_", " "));
      for (const row of value.attempts) add(ui["attempt-list"], row,
        `${row.profile_id} · ${row.runtime} · ${row.cancel_requested ? "Cancellation requested · " : ""}${row.requires_reconciliation ? "Reconciliation needed · " : ""}${row.phase}${row.outcome ? `, ${row.outcome}` : ""} · Last event ${new Date(row.last_event_at).toLocaleString()}`);
      if (!value.attention.length) ui["attention-list"].append(element("p", "No recorded tasks need attention.", "subtle"));
      if (!value.attempts.length) ui["attempt-list"].append(element("p", "No native attempts recorded.", "subtle"));
      ui["attempt-status"].textContent = `${value.attempt_total} recorded attempts. Checked ${new Date(value.observed_at).toLocaleTimeString()}. ${value.attempts_truncated || value.attention_truncated ? "Showing up to 50 rows in each list. " : ""}Recorded state is not a provider heartbeat. One native execution lane.`;
    } catch {
      if (id === project && version === epoch && reading === activityRead) {
        ui["attempt-status"].textContent = "Activity unavailable. Any displayed rows are last known state; refresh to retry.";
        ui["attention-count"].textContent = "Needs you unknown";
      }
    }
  };
  ui["team-enabled"].addEventListener("change", () => {
    const draft = drafts.get(project);
    if (draft && !pending.has(project) && !held.has(project)) { draft.enabled = ui["team-enabled"].checked; draft.dirty = true; controls(); }
  });
  ui["team-save"].addEventListener("click", save);
  ui["team-reload"].addEventListener("click", () => loadSettings(true));
  ui["settings-open"].addEventListener("click", () => { ui.settings.showModal(); loadSettings(); });
  ui["settings-close"].addEventListener("click", () => ui.settings.close());
  ui.settings.addEventListener("close", () => ui["settings-open"].focus());
  return {
    select(id) {
      project = id; epoch++; read++; activityRead++; settings = null;
      ui["attempt-list"].replaceChildren(); ui["attention-list"].replaceChildren();
      ui["attempt-status"].textContent = "Reading recorded attempts…"; ui["attention-count"].textContent = "Needs you …";
      render(); loadSettings(); loadActivity();
    },
    refresh() { loadActivity(); if (!drafts.get(project)?.dirty && !pending.has(project)) loadSettings(); },
  };
};
