"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const ui = Object.fromEntries([
    "project-list", "project-empty", "task-list", "task-empty", "load-older", "new-task",
    "health", "breadcrumb", "task-title", "task-state", "task-meta", "attention", "notice",
    "welcome", "transcript", "request-text", "stream-status", "activity", "event-empty",
    "event-limit", "prompt", "runtime", "create-task", "composer-help", "context-title",
    "policy", "cloud", "requested-runtime", "worktree", "action-title", "action-description",
    "next-action", "header-action", "refresh", "composer", "composer-wrap",
  ].map((id) => [id, $(id)]));
  const state = {
    projects: [], project: null, job: null, selected: null, jobs: new Map(), listed: new Set(), cursor: null,
    drafts: new Map(), selections: new Map(), projectEpoch: 0, taskEpoch: 0,
    listEpoch: 0, detailEpoch: 0, stream: null, lastEvent: 0n, events: 0,
    creating: false, actions: new Set(), loadingOlder: false, pagesLoaded: 0, ready: false,
  };
  const attentionStates = new Set(["waiting_input", "waiting_approval", "review_ready", "blocked"]);
  const label = (value) => String(value || "Unknown").replaceAll("_", " ");
  const draft = () => {
    if (!state.drafts.has(state.project)) state.drafts.set(state.project, {text: "", runtime: "auto"});
    return state.drafts.get(state.project);
  };
  const tell = (message = "") => {
    ui.notice.textContent = message;
    ui.notice.hidden = !message;
  };
  const request = async (path, payload) => {
    let response;
    try {
      response = await fetch(path, {
        method: payload === undefined ? "GET" : "POST", cache: "no-store", credentials: "same-origin",
        headers: payload === undefined ? {} : {"Content-Type": "application/json"},
        body: payload === undefined ? undefined : JSON.stringify(payload),
      });
    } catch {
      throw new Error("Connection lost. Refresh task history before trying the action again; it may already have succeeded.");
    }
    let data;
    try { data = await response.json(); }
    catch { throw new Error("The backend returned an unreadable response. Refresh state to reconcile the task."); }
    if (!response.ok) {
      throw new Error(typeof data.detail === "string" ? data.detail : "The request was not accepted. Refresh state before trying again.");
    }
    return data;
  };
  const validJob = (job, project = state.project) => job && typeof job.id === "string"
    && job.project_id === project && typeof job.state === "string" && typeof job.title === "string";
  const setURL = () => {
    const url = new URL(location.href);
    url.search = "";
    if (state.project) url.searchParams.set("project", state.project);
    if (state.selected) url.searchParams.set("task", state.selected);
    history.replaceState(null, "", url);
  };
  const stopStream = () => {
    if (state.stream) state.stream.close();
    state.stream = null;
  };
  const clearActivity = () => {
    ui.activity.replaceChildren(); state.events = 0; state.lastEvent = 0n;
    ui["event-empty"].hidden = false; ui["event-limit"].hidden = true;
  };
  const renderProjects = () => {
    ui["project-list"].replaceChildren();
    for (const project of state.projects) {
      const button = document.createElement("button"); button.type = "button"; button.className = "project";
      button.setAttribute("aria-current", String(project.id === state.project));
      button.append(document.createTextNode(project.id));
      const meta = document.createElement("small");
      meta.textContent = `${label(project.sensitivity)} · ${project.cloud_allowed ? "Cloud permitted" : "Cloud blocked"}`;
      button.append(meta); button.addEventListener("click", () => selectProject(project.id));
      ui["project-list"].append(button);
    }
    ui["project-empty"].hidden = state.projects.length !== 0;
    ui["new-task"].disabled = !state.project;
  };
  const renderTasks = () => {
    const focused = document.activeElement?.dataset.job;
    ui["task-list"].replaceChildren();
    const jobs = [...state.jobs.values()].sort((a, b) =>
      b.created_at.localeCompare(a.created_at) || b.id.localeCompare(a.id));
    for (const job of jobs) {
      const button = document.createElement("button"); button.type = "button"; button.className = "task";
      button.dataset.job = job.id;
      button.setAttribute("aria-current", String(job.id === state.selected));
      const title = document.createElement("span"); title.textContent = job.title;
      const meta = document.createElement("small");
      meta.textContent = `${attentionStates.has(job.state) ? "Needs you · " : ""}${label(job.state)}`;
      button.append(title, meta); button.addEventListener("click", () => selectTask(job.id));
      ui["task-list"].append(button);
      if (focused === job.id) button.focus({preventScroll: true});
    }
    ui["task-empty"].hidden = jobs.length !== 0;
    ui["task-empty"].textContent = state.project ? "No tasks yet. Create your first task." : "Choose a project to see its tasks.";
    ui["load-older"].hidden = !state.cursor;
    ui["load-older"].disabled = state.loadingOlder;
  };
  const nextStep = () => {
    const job = state.job;
    if (!job) return ["Create a task", "Select a project and describe a bounded request.", null];
    if (["created", "classified", "planning"].includes(job.state) && !job.worktree_ready) {
      return ["Inspect a demo plan", "This read only preview does not inspect files or call an AI provider.", "plan"];
    }
    if (job.state === "queued" && !job.worktree_ready) {
      return ["Prepare an isolated worktree", "Create a local checkout from this task's recorded commit. The project checkout stays unchanged.", "worktree"];
    }
    if (job.state === "queued" && job.worktree_ready) {
      return ["Worktree is prepared", "Native execution is not connected to this workspace yet. No provider has been started by this UI.", null];
    }
    if (attentionStates.has(job.state)) {
      return ["This task needs you", "The recorded state needs operator attention. Review and follow up controls are not connected in this shell yet.", null];
    }
    return ["Inspect recorded activity", "This view follows durable backend events. It does not start, stop or resume a provider session.", null];
  };
  const renderTask = () => {
    const project = state.projects.find((item) => item.id === state.project);
    const job = state.job;
    ui.breadcrumb.textContent = state.project || "Workspace";
    ui["task-title"].textContent = job ? job.title : state.selected ? "Loading task…" : "What would you like to work on?";
    ui["task-state"].textContent = job ? label(job.state) : state.selected ? "Loading" : "New task";
    ui["task-meta"].textContent = job ? `${job.id} · Requested ${label(job.runtime)} · Updated ${new Date(job.updated_at).toLocaleString()}`
      : "A stored request, a demo plan and a visible worktree boundary.";
    ui.attention.hidden = !job || !attentionStates.has(job.state);
    ui.attention.textContent = job ? `Needs you: ${label(job.state)}. See the next step below.` : "";
    ui.welcome.hidden = Boolean(state.selected);
    ui.transcript.hidden = !job;
    ui["request-text"].textContent = job ? job.request : "";
    ui["context-title"].textContent = project ? project.id : "No project selected";
    ui.policy.textContent = project ? `${label(project.sensitivity)} · ${label(project.permission_mode)}` : "Not available";
    ui.cloud.textContent = project ? project.cloud_allowed ? "Permitted by project configuration" : "Blocked by project configuration" : "Not available";
    ui["requested-runtime"].textContent = job ? `${label(job.runtime)}${job.model ? ` · ${job.model}` : ""}` : "Not selected";
    ui.worktree.textContent = job && job.worktree_ready ? "Recorded in local runtime storage" : "Not created";
    const [title, description, action] = nextStep();
    ui["action-title"].textContent = title; ui["action-description"].textContent = description;
    for (const button of [ui["next-action"], ui["header-action"]]) {
      button.hidden = !action;
      button.textContent = state.actions.has(state.selected) ? "Working…"
        : action === "plan" ? "Generate demo plan" : "Create isolated worktree";
      button.disabled = state.actions.has(state.selected);
    }
    const editable = state.ready && Boolean(project) && !state.selected && !state.creating;
    ui.composer.dataset.mode = state.selected ? "history" : "draft";
    ui["composer-wrap"].dataset.mode = ui.composer.dataset.mode;
    ui.prompt.disabled = !editable; ui.runtime.disabled = !editable;
    ui["create-task"].disabled = !editable || !ui.prompt.value.trim();
    ui["create-task"].textContent = state.creating ? "Creating…" : "Create task ↗";
    ui["composer-help"].textContent = state.selected
      ? "This is a recorded task. Follow up chat is not connected yet. Choose New task to create a separate request."
      : state.creating ? "Creating one task. If the connection drops, inspect task history before submitting again."
      : "Draft stays in this page only. Cmd/Ctrl + Enter creates one task. Native execution and paid API calls are not started here.";
  };
  const loadTasks = async (older = false) => {
    if (!state.project || (older && (!state.cursor || state.loadingOlder))) return;
    const project = state.project, epoch = state.projectEpoch, listEpoch = ++state.listEpoch;
    const cursor = older ? state.cursor : null;
    state.loadingOlder = older; ui["load-older"].disabled = older;
    const query = new URLSearchParams({project_id: project, limit: "50"});
    if (cursor) query.set("before_id", cursor);
    try {
      const data = await request(`/api/jobs?${query}`);
      if (epoch !== state.projectEpoch || listEpoch !== state.listEpoch) return;
      if (!Array.isArray(data.jobs) || data.jobs.some((job) => !validJob(job, project))) throw new Error("Task history is unreadable.");
      const historyGap = !older && data.jobs.length === 50
        && !state.listed.has(data.jobs.at(-1).id);
      for (const job of data.jobs) { state.jobs.set(job.id, job); state.listed.add(job.id); }
      if (older || state.pagesLoaded === 0 || historyGap) state.cursor = data.next_cursor;
      state.pagesLoaded += 1;
      renderTasks();
    } catch (error) {
      if (epoch === state.projectEpoch && listEpoch === state.listEpoch) tell(error.message);
    } finally {
      if (epoch === state.projectEpoch && listEpoch === state.listEpoch) {
        state.loadingOlder = false; ui["load-older"].disabled = false;
      }
    }
  };
  const loadDetail = async () => {
    if (!state.selected) return;
    const id = state.selected, taskEpoch = state.taskEpoch, detailEpoch = ++state.detailEpoch;
    try {
      const job = await request(`/api/jobs/${encodeURIComponent(id)}`);
      if (taskEpoch !== state.taskEpoch || detailEpoch !== state.detailEpoch) return;
      if (!validJob(job) || job.id !== id || typeof job.request !== "string") throw new Error("Task detail is unreadable.");
      state.job = job; state.jobs.set(id, job); renderTask(); renderTasks();
      if (!state.stream) connectStream();
    } catch (error) {
      if (taskEpoch === state.taskEpoch && detailEpoch === state.detailEpoch) tell(error.message);
    }
  };
  const appendEvent = (data) => {
    const article = document.createElement("article"); article.className = "event";
    const title = document.createElement("h3"); title.textContent = label(data.event_type.replaceAll(".", " "));
    const time = document.createElement("time"); time.dateTime = data.created_at;
    time.textContent = new Date(data.created_at).toLocaleString();
    const body = document.createElement("p"); const payload = data.payload;
    const text = payload.text || payload.message;
    body.textContent = typeof text === "string" ? text : Object.entries(payload)
      .map(([key, value]) => `${label(key)}: ${typeof value === "object" ? JSON.stringify(value) : String(value)}`).join("\n");
    if (payload.source === "demo") title.textContent += " · Demo preview";
    article.append(title, time, body); ui.activity.append(article);
    state.events += 1; ui["event-empty"].hidden = true;
    if (ui.activity.children.length > 500) ui.activity.firstElementChild.remove();
    ui["event-limit"].hidden = state.events <= 500;
  };
  let detailTimer;
  const connectStream = () => {
    stopStream();
    const id = state.selected, epoch = state.taskEpoch;
    const current = () => id === state.selected && epoch === state.taskEpoch && source === state.stream;
    const source = new EventSource(`/api/jobs/${encodeURIComponent(id)}/events`);
    state.stream = source; ui["stream-status"].textContent = "Connecting to event ledger…";
    source.onopen = () => { if (current()) ui["stream-status"].textContent = "Live local ledger"; };
    source.onerror = () => {
      if (current()) ui["stream-status"].textContent = "Reconnecting to ledger…";
    };
    source.addEventListener("workbench.event", (message) => {
      if (!current()) return;
      try {
        if (!/^[1-9]\d{0,18}$/.test(message.lastEventId)) throw new Error("Invalid event identity");
        const eventId = BigInt(message.lastEventId);
        if (eventId > 9223372036854775807n) throw new Error("Invalid event identity");
        const data = JSON.parse(message.data);
        if (data.job_id !== id || typeof data.event_type !== "string" || typeof data.created_at !== "string"
          || !data.payload || typeof data.payload !== "object" || Array.isArray(data.payload)) throw new Error("Invalid event");
        if (eventId <= state.lastEvent) return;
        state.lastEvent = eventId; appendEvent(data);
        clearTimeout(detailTimer);
        detailTimer = setTimeout(() => { if (current()) loadDetail(); }, 100);
      } catch {
        source.close(); if (current()) { state.stream = null; ui["stream-status"].textContent = "Invalid ledger event. Refresh to reconnect."; }
      }
    });
    source.addEventListener("workbench.error", () => {
      if (current()) { source.close(); state.stream = null; ui["stream-status"].textContent = "Ledger unavailable. Refresh to reconnect."; }
    });
  };
  const selectTask = (id, focus = false) => {
    if (id === state.selected && state.job) { loadDetail(); return; }
    stopStream(); clearTimeout(detailTimer); state.taskEpoch += 1;
    state.selected = id; state.job = null; state.selections.set(state.project, id);
    clearActivity(); tell(); setURL();
    ui.prompt.value = id ? "" : draft().text; ui.runtime.value = draft().runtime;
    renderTask(); renderTasks();
    if (id) loadDetail(); else if (focus && !state.creating) ui.prompt.focus();
  };
  const selectProject = async (id, initialTask) => {
    if (id === state.project && initialTask === undefined) return;
    state.projectEpoch += 1; state.jobs = new Map(); state.listed = new Set(); state.cursor = null;
    state.loadingOlder = false; state.pagesLoaded = 0;
    state.project = id; renderProjects();
    selectTask(initialTask === undefined ? state.selections.get(id) || null : initialTask);
    await loadTasks();
  };
  const createTask = async (event) => {
    event.preventDefault();
    if (!state.project || state.selected || state.creating || !ui.prompt.value.trim()) return;
    const project = state.project, text = ui.prompt.value, runtime = ui.runtime.value;
    const epoch = state.projectEpoch, taskEpoch = state.taskEpoch;
    if (new TextEncoder().encode(text).length > 262144) { tell("Request is too large. Keep it within 256 KiB."); return; }
    state.creating = true; tell(); renderTask();
    try {
      const job = await request("/api/jobs", {project_id: project, request: text, runtime});
      if (!job || typeof job.id !== "string" || job.project_id !== project) throw new Error("Submission response is unreadable. Inspect task history before trying again.");
      const submittedDraft = state.drafts.get(project);
      if (submittedDraft && submittedDraft.text === text) {
        submittedDraft.text = "";
        if (state.project === project && !state.selected && ui.prompt.value === text) ui.prompt.value = "";
      }
      if (epoch === state.projectEpoch && taskEpoch === state.taskEpoch) {
        await loadTasks();
        if (epoch === state.projectEpoch && taskEpoch === state.taskEpoch) selectTask(job.id);
      }
    } catch (error) {
      if (epoch === state.projectEpoch) tell(error.message);
      if (epoch === state.projectEpoch) await loadTasks();
    } finally {
      state.creating = false; renderTask();
    }
  };
  const performAction = async () => {
    const action = nextStep()[2], id = state.selected, epoch = state.taskEpoch;
    if (!action || state.actions.has(id)) return;
    state.actions.add(id); tell(); renderTask();
    try {
      await request(`/api/jobs/${encodeURIComponent(id)}/${action}`, {});
      if (epoch === state.taskEpoch) await loadDetail();
    } catch (error) { if (epoch === state.taskEpoch) tell(error.message); }
    finally { state.actions.delete(id); if (epoch === state.taskEpoch) renderTask(); }
  };
  const refresh = async () => {
    tell(); await loadTasks(); await loadDetail();
  };
  ui.prompt.addEventListener("input", () => { draft().text = ui.prompt.value; renderTask(); });
  ui.runtime.addEventListener("change", () => { draft().runtime = ui.runtime.value; });
  ui.composer.addEventListener("submit", createTask);
  ui.prompt.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey) && !event.isComposing) {
      event.preventDefault(); ui.composer.requestSubmit();
    }
  });
  ui["new-task"].addEventListener("click", () => selectTask(null, true));
  ui["load-older"].addEventListener("click", () => loadTasks(true));
  ui["next-action"].addEventListener("click", performAction);
  ui["header-action"].addEventListener("click", performAction);
  ui.refresh.addEventListener("click", refresh);
  window.addEventListener("pagehide", stopStream);
  const initialize = async () => {
    try {
      const data = await request("/api/bootstrap");
      state.projects = data.projects; state.ready = true; renderProjects();
      ui.health.textContent = `Local backend connected · v${data.version}`;
      const params = new URLSearchParams(location.search);
      const project = state.projects.find((item) => item.id === params.get("project")) || state.projects[0];
      if (project) await selectProject(project.id, params.get("task"));
      else renderTask();
    } catch (error) { ui.health.textContent = "Local backend unavailable"; tell(error.message); }
  };
  initialize();
  setInterval(() => { if (!document.hidden && state.ready && !state.loadingOlder) { loadTasks(); loadDetail(); } }, 10000);
})();
