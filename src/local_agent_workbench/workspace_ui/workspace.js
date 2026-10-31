"use strict";

window.WorkbenchReview = ({request, onChange}) => {
  const ui = Object.fromEntries([
    "review", "review-heading", "review-message", "review-error", "review-content", "review-scope",
    "review-hash", "review-base", "review-id", "review-expiry", "review-decision", "review-files",
    "review-checks", "review-risks", "review-diff", "review-patches", "review-view", "review-prepare",
    "review-request", "review-approve", "review-changes", "review-reject", "review-reload", "review-apply",
    "review-summary", "checks-snapshot",
  ].map((id) => [id, document.getElementById(id)]));
  const decisions = new Set(["approved", "changes_requested", "rejected"]);
  const busy = new Set(), uncertain = new Map(), deadlines = new Map();
  let job = null, generation = 0, reading = 0, loading = false;
  let status = null, bundle = null, viewed = null, renderedHash = null, deadline = 0, error = "";
  const hash = (value) => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
  const text = (value) => typeof value === "string";
  const date = (value) => text(value) && Number.isFinite(Date.parse(value));
  const label = (value) => String(value).replaceAll("_", " ");
  const current = (id, epoch, read = reading) => job?.id === id && generation === epoch && read === reading;
  const active = () => status?.review_current && bundle && status.bundle_hash === bundle.bundle_hash;
  const available = () => active() && !loading && !busy.has(job.id) && !uncertain.has(job.id);
  const canPrepare = () => status && status.job_state === "review_ready" && !active();
  const canRequest = () => available() && status.job_state === "review_ready" && !status.approval
    && viewed === bundle.bundle_hash;
  const canDecide = () => available() && status.job_state === "waiting_approval"
    && status.approval?.state === "pending" && performance.now() < deadline
    && viewed === bundle.bundle_hash;
  const canApply = () => available() && status.job_state === "approved"
    && status.approval?.decision === "approved" && viewed === bundle.bundle_hash;
  const element = (tag, value, className = "") => {
    const node = document.createElement(tag); node.textContent = value; node.className = className; return node;
  };
  const renderControls = () => {
    const held = job && uncertain.get(job.id);
    const approval = status?.approval;
    const pendingExpired = approval?.state === "pending" && performance.now() >= deadline;
    ui["review-prepare"].hidden = !canPrepare();
    ui["review-prepare"].disabled = loading || !job || busy.has(job.id) || Boolean(held);
    ui["review-request"].hidden = !active() || Boolean(approval);
    ui["review-request"].disabled = !canRequest();
    ui["review-view"].disabled = !bundle || loading;
    ui["review-reload"].disabled = !job || loading || busy.has(job.id);
    for (const name of ["approve", "changes", "reject"]) ui[`review-${name}`].disabled = !canDecide();
    ui["review-apply"].hidden = status?.approval?.decision !== "approved";
    ui["review-apply"].disabled = !canApply();
    const alert = error || held?.message || "";
    if (ui["review-error"].textContent !== alert) ui["review-error"].textContent = alert;
    ui["review-error"].hidden = !ui["review-error"].textContent;
    let message = "No frozen review is available for this task.";
    if (loading && !status) message = "Reading stored review state…";
    else if (job && busy.has(job.id)) message = "Recording one action. Decisions are locked until state is reconciled.";
    else if (status && bundle && !status.review_current) message = "Historical review. A newer review round exists. Prepare its snapshot explicitly.";
    else if (status?.job_state === "completed") message = "The approved patch was applied locally. No commit or push was made.";
    else if (approval?.decision) message = `Recorded decision: ${label(approval.decision)}. Apply locally is a separate action after inspecting this snapshot.`;
    else if (approval?.state === "expired" || pendingExpired) message = "This approval has expired. Reload does not extend it. Operator recovery is required.";
    else if (active() && viewed !== bundle.bundle_hash) message = "View the unified diff for this exact snapshot before preparing or recording a decision.";
    else if (active() && !approval) message = "Prepare a decision to obtain its stored approval ID and original expiry.";
    else if (active()) message = "Review the ID, scope and expiry below. Each decision is checked against the live worktree.";
    else if (canPrepare()) message = "Review readiness is recorded. Prepare a frozen snapshot; this does not request approval.";
    if (ui["review-message"].textContent !== message) ui["review-message"].textContent = message;
    ui["review-summary"].textContent = job && bundle ? `Snapshot ${bundle.bundle_hash.slice(0, 12)}${approval ? ` / ${label(approval.decision || approval.state)}${pendingExpired ? ", expired" : ""}` : ""}` : "";
  };
  const renderEvidence = () => {
    ui.review.hidden = !job;
    ui["review-content"].hidden = !bundle;
    ui["checks-snapshot"].textContent = bundle ? `Frozen snapshot ${bundle.bundle_hash}. These checks are immutable bundle evidence; current workflow state is separate below.` : "No frozen snapshot loaded. Current workflow state is separate below.";
    if (!bundle) {
      renderedHash = null; ui["review-diff"].hidden = true;
      ui["review-view"].textContent = "View unified diff";
      ui["review-view"].setAttribute("aria-expanded", "false");
      for (const name of ["files", "checks", "risks", "patches"]) ui[`review-${name}`].replaceChildren();
      renderControls(); return;
    }
    const evidence = bundle.bundle, approval = status.approval;
    const count = evidence.diff.files.length;
    ui["review-scope"].textContent = `Project ${job.project_id} · Task ${job.id} · ${count} changed ${count === 1 ? "file" : "files"} · This snapshot only`;
    ui["review-hash"].textContent = bundle.bundle_hash;
    ui["review-base"].textContent = evidence.diff.base_commit;
    ui["review-id"].textContent = approval?.approval_id || "Not requested";
    ui["review-expiry"].textContent = approval ? `${new Date(approval.expires_at).toLocaleString()} · Original decision deadline`
      : "Starts only when you prepare a decision";
    ui["review-decision"].textContent = approval?.decision ? `${label(approval.decision)} · ${new Date(approval.decided_at).toLocaleString()}` : "No recorded decision";
    if (renderedHash === bundle.bundle_hash) { renderControls(); return; }
    renderedHash = bundle.bundle_hash;
    for (const name of ["files", "checks", "risks", "patches"]) ui[`review-${name}`].replaceChildren();
    ui["review-diff"].hidden = true;
    ui["review-view"].textContent = "View unified diff";
    ui["review-view"].setAttribute("aria-expanded", "false");
    for (const file of evidence.diff.files) {
      const title = `${label(file.status)} · ${file.path}${file.previous_path ? ` · Previously ${file.previous_path}` : ""}`;
      ui["review-files"].append(element("li", title));
      const article = document.createElement("article"); article.className = "review-file";
      article.append(element("h4", title));
      const patch = element("pre", file.patch === null ? file.summary || "Content is not displayed. Inspect this file separately before approving." : file.patch);
      patch.tabIndex = 0; patch.setAttribute("aria-label", `Unified diff for ${file.path}`);
      article.append(patch); ui["review-patches"].append(article);
    }
    if (evidence.diff.files.length === 0) ui["review-files"].append(element("li", "No changed files in this snapshot."));
    for (const check of evidence.verification) {
      const item = document.createElement("li");
      item.append(element("strong", label(check.outcome)),
        element("p", `Exit ${check.exit_code ?? "not recorded"} · ${check.duration_ms} ms · ${new Date(check.created_at).toLocaleString()}`),
        element("code", JSON.stringify(check.command_args)));
      ui["review-checks"].append(item);
    }
    if (!evidence.verification.length) ui["review-checks"].append(element("li", "No verification runs are recorded. Review is not proof that tests passed."));
    for (const risk of evidence.risks) ui["review-risks"].append(element("li", risk));
    if (evidence.diff.truncated) ui["review-risks"].append(element("li", "Diff display is incomplete. Some content was omitted by the capture limits."));
    if (!evidence.risks.length && !evidence.diff.truncated) ui["review-risks"].append(element("li", "No recorded warnings. This is not a guarantee of correctness."));
    if (viewed === bundle.bundle_hash) toggleDiff(true);
    renderControls();
  };
  const validStatus = (data, expected) => {
    if (!data || data.job_id !== expected.id || data.project_id !== expected.project_id
      || !text(data.job_state) || typeof data.review_current !== "boolean" || !date(data.server_time)
      || !(data.bundle_hash === null || hash(data.bundle_hash)) || !(data.approval === null || typeof data.approval === "object")) return false;
    const a = data.approval;
    return !a || (text(a.approval_id) && a.approval_id.length > 0 && a.approval_id.length <= 128
      && a.bundle_hash === data.bundle_hash && hash(a.bundle_hash) && date(a.expires_at)
      && ["pending", "expired", "decided"].includes(a.state)
      && (a.state === "decided" ? decisions.has(a.decision) && date(a.decided_at) : a.decision === null && a.decided_at === null));
  };
  const validBundle = (data, expected, boundHash) => {
    const b = data?.bundle, diff = b?.diff;
    return data?.job_id === expected.id && data.bundle_hash === boundHash
      && b?.schema_version === 1 && b.job_id === expected.id && b.project_id === expected.project_id
      && diff && text(diff.base_commit) && /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(diff.base_commit)
      && typeof diff.truncated === "boolean" && Array.isArray(diff.files)
      && diff.files.every((f) => f && text(f.path) && text(f.status) && (f.previous_path === null || text(f.previous_path))
        && (f.patch === null || text(f.patch)) && (f.summary === null || text(f.summary)))
      && Array.isArray(b.risks) && b.risks.every(text) && Array.isArray(b.verification)
      && b.verification.every((v) => v && text(v.outcome) && date(v.created_at)
        && Array.isArray(v.command_args) && v.command_args.every(text) && Number.isFinite(v.duration_ms)
        && (v.exit_code === null || Number.isInteger(v.exit_code)));
  };
  const bundleDigest = async (value) => {
    const canonical = (item) => Array.isArray(item) ? item.map(canonical) : item && typeof item === "object"
      ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, canonical(item[key])])) : item;
    const bytes = new TextEncoder().encode(JSON.stringify(canonical(value)));
    return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)), (byte) => byte.toString(16).padStart(2, "0")).join("");
  };
  const reconcile = (data) => {
    const held = uncertain.get(data.job_id);
    if (!held) return;
    const a = data.approval;
    if ((held.hash && data.bundle_hash && held.hash !== data.bundle_hash)
      || (held.kind === "prepare" && data.review_current)
      || (held.kind === "request" && a && a.bundle_hash === held.hash)
      || (held.kind === "decision" && a?.approval_id === held.id && a.decision)
      || (held.kind === "promote" && data.job_state === "completed" && a?.approval_id === held.id)) uncertain.delete(data.job_id);
  };
  const load = async () => {
    if (!job || loading || busy.has(job.id)) return;
    const expected = job, epoch = generation, read = ++reading, started = performance.now();
    loading = true; error = ""; renderControls();
    try {
      const data = await request(`/api/jobs/${encodeURIComponent(expected.id)}/approval`);
      if (!current(expected.id, epoch, read)) return;
      if (!validStatus(data, expected)) throw new Error("Stored approval response is unreadable. Decisions remain locked.");
      const snapshot = data.bundle_hash ? await request(`/api/jobs/${encodeURIComponent(expected.id)}/review-bundle`) : null;
      if (!current(expected.id, epoch, read)) return;
      if (snapshot && (!validBundle(snapshot, expected, data.bundle_hash)
        || await bundleDigest(snapshot.bundle) !== data.bundle_hash)) throw new Error("Review identity changed or its evidence is unreadable. Reload before deciding.");
      if (!current(expected.id, epoch, read)) return;
      if (bundle?.bundle_hash !== snapshot?.bundle_hash) viewed = null;
      const a = data.approval, known = a && deadlines.get(a.approval_id);
      if (known && (known.expires !== a.expires_at || known.hash !== a.bundle_hash)) throw new Error("Stored approval identity changed. Decisions remain locked.");
      deadline = a ? Math.min(known?.deadline ?? Infinity, started + Date.parse(a.expires_at) - Date.parse(data.server_time)) : 0;
      if (a) deadlines.set(a.approval_id, {expires: a.expires_at, hash: a.bundle_hash, deadline});
      status = data; bundle = snapshot;
      reconcile(data);
    } catch (failure) {
      if (current(expected.id, epoch, read)) { status = null; bundle = null; viewed = null; error = failure.message; }
    } finally {
      if (current(expected.id, epoch, read)) { loading = false; renderEvidence(); }
    }
  };
  const toggleDiff = (open = ui["review-diff"].hidden) => {
    if (!bundle || loading) return;
    ui["review-diff"].hidden = !open;
    ui["review-view"].textContent = open ? "Hide unified diff" : "View unified diff";
    ui["review-view"].setAttribute("aria-expanded", String(open));
    if (open) viewed = bundle.bundle_hash;
    renderControls();
  };
  const perform = async (kind, decision) => {
    if (!job || loading || busy.has(job.id) || uncertain.has(job.id)) return;
    if (kind === "prepare" ? !canPrepare() : kind === "request" ? !canRequest()
      : kind === "promote" ? !canApply() : !canDecide() || !decisions.has(decision)) return;
    const expected = job, epoch = generation, boundHash = bundle?.bundle_hash, id = status.approval?.approval_id;
    const prefix = `/api/jobs/${encodeURIComponent(expected.id)}`;
    const path = kind === "prepare" ? `${prefix}/review-bundle` : kind === "request" ? `${prefix}/approval-request`
      : `/api/approvals/${encodeURIComponent(id)}${kind === "promote" ? "/promote" : ""}`;
    const payload = kind === "prepare" ? {} : ["request", "promote"].includes(kind) ? {bundle_hash: boundHash} : {decision, bundle_hash: boundHash};
    busy.add(expected.id); error = ""; renderControls();
    try {
      const data = await request(path, payload);
      if (data?.job_id !== expected.id || !hash(data.bundle_hash)
        || (kind !== "prepare" && data.bundle_hash !== boundHash)
        || (kind === "request" && (!text(data.approval_id) || !date(data.expires_at) || data.state !== "pending"))
        || (kind === "promote" && (data.approval_id !== id || data.job_state !== "completed"))
        || (kind === "decision" && (data.approval_id !== id || data.decision !== decision))) {
        throw new Error("Action response is unreadable. It may already have succeeded. Reload stored state; do not resubmit.");
      }
    } catch (failure) {
      uncertain.set(expected.id, {kind: failure.status === 409 ? "conflict" : kind, hash: boundHash, id,
        message: `${failure.message} Decisions are locked until stored state confirms the outcome.`});
      if (current(expected.id, epoch)) error = "";
    } finally {
      busy.delete(expected.id);
      if (job?.id === expected.id && job.project_id === expected.project_id) { await load(); onChange(); }
    }
  };
  const select = (selected) => {
    generation += 1; reading += 1; job = selected; loading = false;
    status = null; bundle = null; viewed = null; renderedHash = null; deadline = 0; error = ""; renderEvidence();
  };
  ui["review-view"].addEventListener("click", () => toggleDiff());
  ui["review-prepare"].addEventListener("click", () => perform("prepare"));
  ui["review-request"].addEventListener("click", () => perform("request"));
  for (const [name, value] of [["approve", "approved"], ["changes", "changes_requested"], ["reject", "rejected"]]) {
    ui[`review-${name}`].addEventListener("click", () => perform("decision", value));
  }
  ui["review-reload"].addEventListener("click", load);
  ui["review-apply"].addEventListener("click", () => perform("promote"));
  setInterval(() => { if (job && status?.approval?.state === "pending") renderControls(); }, 1000);
  select(null);
  return {
    select,
    update(selected) {
      if (job?.id !== selected.id || job.project_id !== selected.project_id) select(selected);
      else job = selected;
      return load();
    },
    focus() { ui["review-heading"].focus(); },
  };
};

window.WorkbenchExecution = ({request, onChange}) => {
  const ui = Object.fromEntries(["execution", "execution-heading", "execution-message", "execution-scope",
    "execution-start", "execution-cancel", "execution-verify", "execution-ready", "execution-reload"]
    .map((id) => [id, document.getElementById(id)]));
  let job = null, epoch = 0, reading = 0, data = null, error = "", loading = false;
  const busy = new Set(), held = new Map();
  const hash = (v) => typeof v === "string" && /^[a-f0-9]{64}$/.test(v);
  const current = (id, version, read = reading) => job?.id === id && epoch === version && read === reading;
  const render = () => {
    ui.execution.hidden = !job;
    const native = data?.execution, check = data?.verification, attempt = native?.attempt;
    const blocked = !job || !data || loading || busy.has(job.id) || held.has(job.id);
    ui["execution-scope"].textContent = data?.agent
      ? `Frozen agent ${data.agent.id} · ${data.agent.runtime} · ${data.agent.model || "Native default"} · Writes: ${data.agent.write_paths.join(", ")}`
      : "This task has no executable agent binding. Create a new task with a project agent to run it.";
    ui["execution-start"].hidden = !(data?.job_state === "queued" && job?.worktree_ready && data.agent);
    ui["execution-start"].disabled = blocked || !native?.enabled || Boolean(attempt);
    ui["execution-cancel"].hidden = !attempt || !["admitted", "running"].includes(attempt.phase);
    ui["execution-cancel"].disabled = blocked || attempt?.cancel_requested;
    ui["execution-verify"].hidden = data?.job_state !== "verifying";
    ui["execution-verify"].disabled = blocked || !check?.command.length || check.status !== "not_started";
    ui["execution-ready"].hidden = data?.job_state !== "verifying";
    ui["execution-ready"].disabled = blocked || check?.status !== "passed";
    ui["execution-reload"].disabled = loading || !job || busy.has(job.id);
    let message = "Read task workflow state before starting an action.";
    if (data) message = `Native execution ${native.enabled ? "enabled" : "disabled by local configuration"}. Verification: ${check.status.replaceAll("_", " ")}.`;
    if (attempt) message += ` Attempt ${attempt.attempt_id}: ${attempt.phase}${attempt.outcome ? `, ${attempt.outcome}` : ""}.`;
    if (check?.command.length) message += ` Trusted check: ${JSON.stringify(check.command)}. Runs locally in the isolated worktree; no command comes from the provider.`;
    if (native?.requires_reconciliation) message = "Native termination needs operator reconciliation. No restart or account switch will be attempted.";
    if (busy.has(job?.id)) message = "Recording one action. Wait for its stored result.";
    ui["execution-message"].textContent = error || held.get(job?.id)?.message || message;
  };
  const valid = (v, selected) => v?.job_id === selected.id && v.project_id === selected.project_id
    && typeof v.job_state === "string" && v.execution?.job_id === selected.id
    && hash(v.execution.authorization_hash) && typeof v.execution.enabled === "boolean"
    && typeof v.execution.requires_reconciliation === "boolean"
    && (v.agent === null || (typeof v.agent?.id === "string" && typeof v.agent.runtime === "string"
      && Array.isArray(v.agent.write_paths) && v.agent.write_paths.every((p) => typeof p === "string")))
    && (v.execution.attempt === null || (typeof v.execution.attempt?.attempt_id === "string"
      && ["admitted", "running", "reconcile", "settled"].includes(v.execution.attempt.phase)))
    && hash(v.verification?.authorization_hash) && Array.isArray(v.verification.command)
    && v.verification.command.every((a) => typeof a === "string")
    && ["not_started", "uncertain", "passed", "failed"].includes(v.verification.status);
  const load = async () => {
    if (!job || loading || busy.has(job.id)) return;
    const selected = job, version = epoch, read = ++reading;
    loading = true; error = ""; render();
    try {
      const value = await request(`/api/jobs/${encodeURIComponent(selected.id)}/workflow`);
      if (!current(selected.id, version, read)) return;
      if (!valid(value, selected)) throw new Error("Workflow response is unreadable. Actions remain locked.");
      data = value;
      const pending = held.get(selected.id), attempt = value.execution.attempt;
      if (pending && ((pending.kind === "start" && attempt)
        || (pending.kind === "cancel" && attempt?.attempt_id === pending.id && (attempt.cancel_requested || attempt.phase === "settled"))
        || (pending.kind === "verify" && value.verification.status !== "not_started")
        || (pending.kind === "ready" && value.job_state === "review_ready"))) held.delete(selected.id);
    } catch (failure) {
      if (current(selected.id, version, read)) { data = null; error = failure.message; }
    } finally { if (current(selected.id, version, read)) { loading = false; render(); } }
  };
  const perform = async (kind) => {
    const button = ui[`execution-${kind}`];
    if (!job || !data || button.disabled || button.hidden) return;
    const selected = job, version = epoch, native = data.execution, check = data.verification;
    const id = native.attempt?.attempt_id;
    const suffix = {start: "execution", cancel: "execution/cancel", verify: "verification", ready: "review-readiness"}[kind];
    const body = kind === "start" ? {authorization_hash: native.authorization_hash}
      : kind === "cancel" ? {attempt_id: id} : kind === "verify" ? {authorization_hash: check.authorization_hash} : {};
    busy.add(selected.id); error = ""; render();
    try {
      const result = await request(`/api/jobs/${encodeURIComponent(selected.id)}/${suffix}`, body);
      if (kind === "ready" ? result?.job_id !== selected.id
        : kind === "verify" ? !valid(result, selected) : result?.job_id !== selected.id) {
        throw new Error("Action response is unreadable. Read stored state before trying again.");
      }
    } catch (failure) {
      held.set(selected.id, {kind: failure.status === 409 ? "conflict" : kind, id,
        message: `${failure.message} This action will not be resubmitted automatically.`});
    } finally {
      busy.delete(selected.id);
      if (current(selected.id, version)) { await load(); onChange(); }
    }
  };
  for (const action of ["start", "cancel", "verify", "ready"]) ui[`execution-${action}`].addEventListener("click", () => perform(action));
  ui["execution-reload"].addEventListener("click", load);
  return {
    select(selected) { epoch++; reading++; job = selected; data = null; loading = false; error = ""; render(); },
    update(selected) {
      if (job?.id !== selected.id || job.project_id !== selected.project_id) this.select(selected);
      else job = selected;
      return load();
    },
    focus() { ui["execution-heading"].focus(); },
  };
};

(() => {
  const $ = (id) => document.getElementById(id);
  const ui = Object.fromEntries([
    "project-list", "project-empty", "task-list", "task-empty", "load-older", "new-task",
    "health", "breadcrumb", "task-title", "task-state", "task-meta", "attention", "notice",
    "welcome", "transcript", "request-text", "stream-status", "activity", "event-empty",
    "event-limit", "prompt", "runtime", "create-task", "composer-help", "context-title",
    "policy", "cloud", "requested-runtime", "worktree", "action-title", "action-description",
    "next-action", "header-action", "refresh", "composer", "composer-wrap",
    "control-status", "control-attention", "control-queued", "control-total", "runtime-roster",
    "layout", "view-projects", "view-workspace", "view-operations", "agent", "task-tabs",
    "tab-activity", "tab-changes", "tab-checks", "panel-activity", "panel-changes", "panel-checks",
    "task-search", "attention-filter", "history-heading",
  ].map((id) => [id, $(id)]));
  const state = {
    projects: [], project: null, job: null, selected: null, jobs: new Map(), listed: new Set(), cursor: null,
    drafts: new Map(), selections: new Map(), projectEpoch: 0, taskEpoch: 0,
    listEpoch: 0, detailEpoch: 0, stream: null, lastEvent: 0n, events: 0,
    creating: false, actions: new Set(), loadingOlder: false, pagesLoaded: 0, ready: false, controlEpoch: 0,
    agents: [], agentsLoading: false, agentEpoch: 0, tab: "activity", version: null,
  };
  const attentionStates = new Set(["waiting_input", "waiting_approval", "review_ready", "blocked", "failed"]);
  const tabs = ["activity", "changes", "checks"];
  const selectTab = (name) => {
    state.tab = name;
    for (const tab of tabs) {
      ui[`tab-${tab}`].setAttribute("aria-selected", String(name === tab));
      ui[`tab-${tab}`].tabIndex = name === tab ? 0 : -1;
      ui[`panel-${tab}`].hidden = name !== tab;
    }
  };
  const label = (value) => String(value || "Unknown").replaceAll("_", " ");
  const showPanel = (panel) => {
    ui.layout.dataset.view = panel;
    for (const view of ["projects", "workspace", "operations"]) {
      ui[`view-${view}`].setAttribute("aria-pressed", String(view === panel));
    }
  };
  const resetControl = (message) => {
    ui["control-status"].textContent = message;
    for (const name of ["attention", "queued", "total"]) ui[`control-${name}`].textContent = "…";
    ui["runtime-roster"].replaceChildren();
  };
  const loadControl = async () => {
    if (!state.project) return;
    const project = state.project, epoch = ++state.controlEpoch;
    try {
      const data = await request(`/api/control?${new URLSearchParams({project_id: project})}`);
      if (project !== state.project || epoch !== state.controlEpoch) return;
      const count = (value) => Number.isSafeInteger(value) && value >= 0;
      const names = new Map([["auto", "Auto routing"], ["codex", "Codex"], ["claude", "Claude Code"], ["local", "Local AI"]]);
      if (data.project_id !== project || data.basis !== "recorded_job_states"
          || !count(data.total) || !count(data.needs_attention) || !data.states
          || Object.values(data.states).some((value) => !count(value)) || !Array.isArray(data.runtimes)
          || data.runtimes.length !== 4 || new Set(data.runtimes.map((item) => item?.runtime)).size !== 4
          || data.runtimes.some((item) => !names.has(item?.runtime) || !count(item.total))) {
        throw new Error("Control status is unreadable.");
      }
      ui["control-status"].textContent = "All stored tasks in this project. Refreshed from the local backend.";
      ui["control-attention"].textContent = String(data.needs_attention);
      ui["control-queued"].textContent = String(data.states.queued || 0);
      ui["control-total"].textContent = String(data.total);
      ui["runtime-roster"].replaceChildren();
      for (const item of data.runtimes) {
        const card = document.createElement("article"); card.className = "runtime-card";
        const title = document.createElement("strong"), description = document.createElement("p");
        title.textContent = names.get(item.runtime);
        description.textContent = `${item.total} recorded requests · Recorded preference, not live execution`;
        card.append(title, description); ui["runtime-roster"].append(card);
      }
    } catch {
      if (project === state.project && epoch === state.controlEpoch) resetControl("Control status unavailable. Refresh to retry.");
    }
  };
  const draft = () => {
    if (!state.drafts.has(state.project)) state.drafts.set(state.project, {text: "", runtime: "auto", agent: ""});
    return state.drafts.get(state.project);
  };
  const tell = (message = "") => {
    ui.notice.textContent = message;
    ui.notice.hidden = !message;
  };
  const backendContact = () => {
    ui.health.textContent = `Last backend response ${new Date().toLocaleTimeString()}${state.version ? ` · v${state.version}` : ""}`;
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
      ui.health.textContent = "Backend connection unavailable. Displayed data may be stale.";
      throw new Error("Connection lost. Refresh task history before trying the action again; it may already have succeeded.");
    }
    backendContact();
    let data;
    try { data = await response.json(); }
    catch { throw new Error("The backend returned an unreadable response. Refresh state to reconcile the task."); }
    if (!response.ok) {
      const error = new Error(typeof data.detail === "string" ? data.detail : "The request was not accepted. Refresh state before trying again.");
      error.status = response.status; throw error;
    }
    return data;
  };
  const validJob = (job, project = state.project) => job && typeof job.id === "string"
    && job.project_id === project && typeof job.state === "string" && typeof job.title === "string";
  const review = window.WorkbenchReview({request, onChange: () => loadDetail()});
  const execution = window.WorkbenchExecution({request, onChange: () => loadDetail()});
  const projectControl = window.WorkbenchProjectControl({request,
    onSelect: (project, job) => project === state.project ? selectTask(job) : selectProject(project, job),
    onSaved: (project) => { if (project === state.project) loadAgents(); },
    onOpen: () => shell.settingsTab("team"),
  });
  const shell = window.WorkbenchShell({request,
    onChoose: (id) => { ui.agent.value = id; draft().agent = id; renderTask(); },
    onNewTask: () => selectTask(null, true),
  });
  const loadAgents = async () => {
    const project = state.project, epoch = state.projectEpoch, reading = ++state.agentEpoch;
    state.agents = []; state.agentsLoading = true; ui.agent.replaceChildren(); renderTask();
    const option = (value, title) => { const item = document.createElement("option"); item.value = value; item.textContent = title; return item; };
    ui.agent.append(option("", "Record only, no worker"));
    try {
      const data = await request(`/api/projects/${encodeURIComponent(project)}/agents`);
      if (epoch !== state.projectEpoch || reading !== state.agentEpoch) return;
      if (data?.project_id !== project || !Array.isArray(data.agents)
        || data.agents.some((a) => typeof a?.id !== "string" || a.runtime !== "codex" || a.role !== "implementer")) throw new Error("Project agents unavailable.");
      state.agents = data.agents;
      for (const a of data.agents) ui.agent.append(option(a.id, `${a.id} · ${a.runtime} · ${a.model || "Native default"}`));
    } catch { if (epoch === state.projectEpoch && reading === state.agentEpoch) state.agents = []; }
    finally {
      if (epoch === state.projectEpoch && reading === state.agentEpoch) {
        state.agentsLoading = false;
        const selected = draft().agent;
        if (selected && !state.agents.some((a) => a.id === selected)) {
          const missing = option(selected, `${selected} · Unavailable, choose another profile`);
          missing.disabled = true; ui.agent.append(missing);
        }
        ui.agent.value = selected || "";
        renderTask();
      }
    }
  };
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
      button.setAttribute("aria-expanded", String(project.id === state.project));
      button.append(document.createTextNode(project.id));
      const meta = document.createElement("small");
      meta.textContent = `${label(project.sensitivity)} · ${project.cloud_allowed ? "Cloud permitted" : "Cloud blocked"}`;
      button.append(meta); button.addEventListener("click", () => selectProject(project.id));
      const query = ui["task-search"].value.trim().toLocaleLowerCase();
      button.hidden = Boolean(query) && !project.id.toLocaleLowerCase().includes(query) && project.id !== state.project;
      ui["project-list"].append(button);
      if (project.id === state.project) ui["project-list"].append(ui["task-list"]);
    }
    ui["project-empty"].hidden = state.projects.length !== 0;
    ui["new-task"].disabled = !state.project;
  };
  const renderTasks = () => {
    const focused = document.activeElement?.dataset.job;
    ui["task-list"].replaceChildren();
    const query = ui["task-search"].value.trim().toLocaleLowerCase();
    const jobs = [...state.jobs.values()].filter((job) => (!ui["attention-filter"].checked || attentionStates.has(job.state))
      && (!query || `${state.project} ${job.title} ${job.id}`.toLocaleLowerCase().includes(query))).sort((a, b) =>
      Number(attentionStates.has(b.state)) - Number(attentionStates.has(a.state))
      || b.created_at.localeCompare(a.created_at) || b.id.localeCompare(a.id));
    for (const job of jobs) {
      const button = document.createElement("button"); button.type = "button"; button.className = "task";
      button.dataset.job = job.id;
      button.dataset.attention = String(attentionStates.has(job.state));
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
    if (!jobs.length && state.jobs.size) ui["task-empty"].textContent = "No loaded tasks match these filters.";
    ui["history-heading"].textContent = state.project ? `Tasks in ${state.project}` : "Tasks in selected project";
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
      return ["Review execution scope", "Start the frozen project agent explicitly. No task starts automatically.", "execution"];
    }
    if (["running", "verifying"].includes(job.state)) return ["Inspect task execution", "Read the owned attempt, cancellation and trusted verification state.", "execution"];
    if (["review_ready", "waiting_approval", "approved", "rejected"].includes(job.state)) {
      return ["Inspect the frozen review", "View recorded verification, scope and diff. Decisions stay bound to one stored snapshot.", "review"];
    }
    if (attentionStates.has(job.state)) {
      return ["This task needs you", "The recorded state needs operator attention. Follow up and provider recovery are not connected in this shell yet.", null];
    }
    return ["Inspect recorded activity", "This view follows durable backend events. It does not start, stop or resume a provider session.", null];
  };
  const renderTask = () => {
    const project = state.projects.find((item) => item.id === state.project);
    const job = state.job;
    ui.breadcrumb.textContent = state.project || "Workspace";
    ui["task-title"].textContent = job ? job.title : state.selected ? "Loading task…" : "New task";
    ui["task-state"].textContent = job ? label(job.state) : state.selected ? "Loading" : "New task";
    ui["task-meta"].textContent = job ? `Updated ${new Date(job.updated_at).toLocaleString()}` : "";
    ui.attention.hidden = !job || !attentionStates.has(job.state);
    ui.attention.textContent = job ? `Needs you: ${label(job.state)}. See the next step below.` : "";
    ui.welcome.hidden = Boolean(state.selected);
    ui.transcript.hidden = !job;
    ui["task-tabs"].hidden = !job;
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
        : action === "plan" ? "Generate demo plan" : action === "review" ? "Inspect review"
        : action === "execution" ? "Inspect execution" : "Create isolated worktree";
      button.disabled = state.actions.has(state.selected);
    }
    const editable = state.ready && Boolean(project) && !state.selected && !state.creating;
    ui.composer.dataset.mode = state.selected ? "history" : "draft";
    ui["composer-wrap"].dataset.mode = ui.composer.dataset.mode;
    ui.prompt.disabled = !editable; ui.runtime.disabled = !editable || Boolean(ui.agent.value);
    ui.runtime.hidden = Boolean(ui.agent.value);
    ui.agent.disabled = !editable || state.agentsLoading;
    const unavailableAgent = Boolean(ui.agent.value) && !state.agents.some((a) => a.id === ui.agent.value);
    ui["create-task"].disabled = !editable || state.agentsLoading || unavailableAgent || !ui.prompt.value.trim();
    ui["create-task"].textContent = state.creating ? "Creating…" : "Create task";
    ui["composer-help"].textContent = state.selected
      ? "This is a recorded task. Follow up chat is not connected yet. Choose New task to create a separate request."
      : state.creating ? "Creating one task. If the connection drops, inspect task history before submitting again."
      : unavailableAgent ? "The selected profile is unavailable. Explicitly choose another profile or Record only before submitting."
      : "Select a configured project agent for native work, or record a request only. Creating a task never starts execution or paid inference.";
    shell.update({project: state.project, selected: ui.agent.value, agents: state.agents,
      editable: editable && !state.agentsLoading});
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
      review.update(job);
      execution.update(job);
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
    if (data.event_type === "runtime.text") title.textContent = `${state.job?.runtime === "auto" ? "Agent" : label(state.job?.runtime)}${state.job?.model ? ` / ${state.job.model}` : ""}`;
    body.textContent = typeof text === "string" ? text : "";
    article.dataset.kind = typeof text === "string" ? "message" : "system";
    if (payload.source === "demo") title.textContent += " · Demo preview";
    article.append(title, time, body); ui.activity.append(article);
    if (typeof text !== "string" && Object.keys(payload).length) {
      const details = document.createElement("details"), summary = document.createElement("summary"), content = document.createElement("pre");
      summary.textContent = "Details"; content.textContent = JSON.stringify(payload, null, 2);
      details.append(summary, content); article.append(details);
    }
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
    showPanel("workspace");
    if (id === state.selected && state.job) { loadDetail(); return; }
    stopStream(); clearTimeout(detailTimer); state.taskEpoch += 1;
    state.selected = id; state.job = null; state.selections.set(state.project, id);
    selectTab("activity");
    review.select(null);
    execution.select(null);
    clearActivity(); tell(); setURL();
    ui.prompt.value = id ? "" : draft().text; ui.runtime.value = draft().runtime;
    ui.agent.value = draft().agent || "";
    renderTask(); renderTasks();
    if (id) loadDetail(); else if (focus && !state.creating) ui.prompt.focus();
  };
  const selectProject = async (id, initialTask) => {
    if (id === state.project && initialTask === undefined) return;
    state.projectEpoch += 1; state.jobs = new Map(); state.listed = new Set(); state.cursor = null;
    state.loadingOlder = false; state.pagesLoaded = 0;
    state.project = id; renderProjects();
    projectControl.select(id);
    loadAgents();
    resetControl("Loading project activity…"); loadControl();
    selectTask(initialTask === undefined ? state.selections.get(id) || null : initialTask);
    await loadTasks();
  };
  const createTask = async (event) => {
    event.preventDefault();
    if (!state.project || state.selected || state.creating || !ui.prompt.value.trim()) return;
    const project = state.project, text = ui.prompt.value, agent = ui.agent.value;
    if (state.agentsLoading || (agent && !state.agents.some((a) => a.id === agent))) return;
    const runtime = agent ? "auto" : ui.runtime.value;
    const epoch = state.projectEpoch, taskEpoch = state.taskEpoch;
    if (new TextEncoder().encode(text).length > 262144) { tell("Request is too large. Keep it within 256 KiB."); return; }
    state.creating = true; tell(); renderTask();
    try {
      const job = await request("/api/jobs", {project_id: project, request: text, runtime, ...(agent ? {agent_id: agent} : {})});
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
    if (action === "review") { showPanel("workspace"); selectTab("changes"); review.focus(); return; }
    if (action === "execution") { showPanel("workspace"); selectTab("checks"); execution.focus(); return; }
    state.actions.add(id); tell(); renderTask();
    try {
      await request(`/api/jobs/${encodeURIComponent(id)}/${action}`, {});
      if (epoch === state.taskEpoch) await loadDetail();
    } catch (error) { if (epoch === state.taskEpoch) tell(error.message); }
    finally { state.actions.delete(id); if (epoch === state.taskEpoch) renderTask(); }
  };
  const refresh = async () => {
    tell(); loadControl(); projectControl.refresh(); await loadTasks(); await loadDetail();
  };
  ui.prompt.addEventListener("input", () => { draft().text = ui.prompt.value; renderTask(); });
  ui.runtime.addEventListener("change", () => { draft().runtime = ui.runtime.value; });
  ui.agent.addEventListener("change", () => { draft().agent = ui.agent.value; renderTask(); });
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
  ui["task-search"].addEventListener("input", () => { renderProjects(); renderTasks(); });
  ui["attention-filter"].addEventListener("change", renderTasks);
  for (const tab of tabs) {
    ui[`tab-${tab}`].addEventListener("click", () => selectTab(tab));
    ui[`tab-${tab}`].addEventListener("keydown", (event) => {
      const index = tabs.indexOf(tab), next = event.key === "ArrowRight" ? (index + 1) % 3
        : event.key === "ArrowLeft" ? (index + 2) % 3 : event.key === "Home" ? 0 : event.key === "End" ? 2 : null;
      if (next !== null) { event.preventDefault(); selectTab(tabs[next]); ui[`tab-${tabs[next]}`].focus(); }
    });
  }
  for (const panel of ["projects", "workspace", "operations"]) {
    ui[`view-${panel}`].addEventListener("click", () => showPanel(panel));
  }
  window.addEventListener("pagehide", stopStream);
  const initialize = async () => {
    try {
      const data = await request("/api/bootstrap");
      state.projects = data.projects; state.ready = true; renderProjects();
      state.version = data.version; backendContact();
      const params = new URLSearchParams(location.search);
      const project = state.projects.find((item) => item.id === params.get("project")) || state.projects[0];
      if (project) await selectProject(project.id, params.get("task"));
      else renderTask();
    } catch (error) { ui.health.textContent = "Local backend unavailable"; tell(error.message); }
  };
  initialize();
  setInterval(() => { if (!document.hidden && state.ready && !state.loadingOlder) { loadTasks(); loadDetail(); loadControl(); projectControl.refresh(); } }, 10000);
})();
