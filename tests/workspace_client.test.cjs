"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");
const crypto = require("node:crypto");
const code = fs.readFileSync(path.join(__dirname, "../workspace_ui/workspace.js"), "utf8");
const pendingDigests = new Set();
const flush = async () => {
  for (let n = 0; n < 8; n++) {
    await new Promise(setImmediate);
    await Promise.allSettled([...pendingDigests]);
  }
};
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
};
const job = (id, project = "alpha") => ({
  id, project_id: project, title: id, request: `Request ${id}`, state: "created", runtime: "auto",
  created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z", worktree_ready: false,
});

function harness(options = {}) {
  const nodes = new Map(), streams = [], timers = new Map(), intervals = [], calls = [], jobs = new Map();
  let handler = options.handle || null, timerId = 0, elapsed = 0;
  let currentURL = options.url || "http://localhost/?project=alpha";
  for (const item of options.jobs || [job("A"), job("B"), job("C", "beta")]) jobs.set(item.id, item);
  const document = {hidden: false, activeElement: null};
  class Element {
    constructor(tag) { this.tag = tag; this.children = []; this.listeners = {}; this.dataset = {};
      this.value = ""; this.textContent = ""; this.hidden = false; this.disabled = false; }
    set innerHTML(_) { throw new Error("Untrusted markup must not be interpreted."); }
    setAttribute(key, value) { this[key] = value; }
    append(...items) { for (const item of items) { item.parent = this; this.children.push(item); } }
    replaceChildren() { this.children = []; }
    get firstElementChild() { return this.children[0]; }
    remove() { this.parent.children = this.parent.children.filter((item) => item !== this); }
    focus() { document.activeElement = this; }
    addEventListener(name, callback) { this.listeners[name] = callback; }
    fire(name, extra = {}) { return this.listeners[name]?.({preventDefault() {}, ...extra}); }
    requestSubmit() { return this.fire("submit"); }
  }
  document.getElementById = (id) => {
    if (!nodes.has(id)) nodes.set(id, new Element(id));
    return nodes.get(id);
  };
  document.createElement = (tag) => new Element(tag);
  document.createTextNode = (text) => ({textContent: text});
  class EventSource {
    constructor(url) { this.url = url; this.listeners = {}; this.closed = false; streams.push(this); }
    close() { this.closed = true; }
    addEventListener(name, callback) { this.listeners[name] = callback; }
    emit(id, task, payload = {text: "Recorded"}) {
      this.listeners["workbench.event"]({lastEventId: String(id), data: JSON.stringify({
        id: Number(id), job_id: task, event_type: "runtime.text", created_at: "2026-01-01T00:00:00Z", payload,
      })});
    }
  }
  const response = (data, status = 200) => ({ok: status < 400, status, json: async () => data});
  const fetch = async (url, init) => {
    calls.push({url, init});
    if (handler) {
      const result = handler(url, init);
      if (result !== undefined) return result;
    }
    if (url === "/api/bootstrap") return response({version: "test", projects: options.empty ? [] : [
      {id: "alpha", sensitivity: "private", cloud_allowed: false, permission_mode: "sandboxed-write"},
      {id: "beta", sensitivity: "public", cloud_allowed: true, permission_mode: "sandboxed-write"},
    ]});
    if (url.startsWith("/api/control?")) {
      const project = new URL(url, currentURL).searchParams.get("project_id");
      const selected = [...jobs.values()].filter((item) => item.project_id === project);
      const states = {};
      for (const item of selected) states[item.state] = (states[item.state] || 0) + 1;
      return response({project_id: project, basis: "recorded_job_states", total: selected.length,
        needs_attention: selected.filter((item) => ["waiting_input", "waiting_approval", "review_ready", "blocked", "failed"].includes(item.state)).length,
        states, runtimes: ["auto", "codex", "claude", "local"].map((runtime) => ({runtime,
          total: selected.filter((item) => item.runtime === runtime).length}))});
    }
    if (url.startsWith("/api/jobs?")) {
      const query = new URL(url, currentURL).searchParams;
      const values = [...jobs.values()].filter((item) => item.project_id === query.get("project_id"));
      return response({jobs: values, next_cursor: values.length === 50 ? values.at(-1).id : null});
    }
    if (url === "/api/jobs" && init.method === "POST") {
      const payload = JSON.parse(init.body), created = {...job("new", payload.project_id), ...payload};
      jobs.set(created.id, created); return response(created, 201);
    }
    const id = decodeURIComponent(url.split("/")[3]);
    if (url.endsWith("/approval") && jobs.has(id)) return response({job_id: id,
      project_id: jobs.get(id).project_id, job_state: jobs.get(id).state, bundle_hash: null,
      review_current: false, server_time: "2026-01-01T00:00:00Z", approval: null});
    return response(jobs.get(id) ? {...jobs.get(id)} : {detail: "Job does not exist."}, jobs.has(id) ? 200 : 404);
  };
  const reviewCrypto = {subtle: {digest(...args) {
    const pending = options.digest ? options.digest(...args) : crypto.webcrypto.subtle.digest(...args);
    pendingDigests.add(pending);
    pending.then(() => pendingDigests.delete(pending), () => pendingDigests.delete(pending));
    return pending;
  }}};
  const context = {
    document, EventSource, fetch, URL, URLSearchParams, TextEncoder, console, crypto: reviewCrypto,
    performance: {now: () => elapsed},
    location: {get href() { return currentURL; }, get search() { return new URL(currentURL).search; }},
    history: {replaceState(_, __, url) { currentURL = String(url); }},
    window: {addEventListener() {}},
    setTimeout(callback) { timers.set(++timerId, callback); return timerId; },
    clearTimeout(id) { timers.delete(id); }, setInterval(callback) { intervals.push(callback); },
  };
  document.getElementById("runtime").value = "auto";
  vm.runInNewContext(code, context, {filename: "workspace.js"});
  const node = (id) => nodes.get(id);
  const clickTask = async (id) => { await node("task-list").children.find((item) => item.dataset.job === id).fire("click"); await flush(); };
  const clickProject = async (id) => { await node("project-list").children.find((item) => item.children[0].textContent === id).fire("click"); await flush(); };
  return {node, streams, calls, jobs, response, clickTask, clickProject,
    handle(callback) { handler = callback; }, url() { return currentURL; },
    type(value) { node("prompt").value = value; node("prompt").fire("input"); },
    async tick() { for (const [id, callback] of [...timers]) { timers.delete(id); callback(); } await flush(); },
    async advance(ms) { elapsed += ms; for (const callback of intervals) callback(); await flush(); },
  };
}

test("operations show stored counts, unknown execution and attention first without a mutation", async () => {
  const h = harness({jobs: [job("new"), {...job("older"), state: "failed", runtime: "codex"}]});
  await flush();
  assert.equal(h.node("control-total").textContent, "2");
  assert.equal(h.node("control-attention").textContent, "1");
  assert.equal(h.node("control-queued").textContent, "0");
  assert.equal(h.node("task-list").children[0].dataset.job, "older");
  assert.equal(h.node("runtime-roster").children.length, 4);
  assert.match(h.node("runtime-roster").children[1].children[1].textContent, /1 recorded requests · Execution not connected/);
  assert.equal(h.calls.some((call) => call.init.method !== "GET"), false);
});

test("a delayed operations response cannot leak another project's counts", async () => {
  const late = deferred();
  const h = harness({handle: (url) => url === "/api/control?project_id=alpha" ? late.promise : undefined});
  await flush(); await h.clickProject("beta");
  assert.equal(h.node("control-total").textContent, "1");
  late.resolve(h.response({project_id: "alpha", basis: "recorded_job_states", total: 99}));
  await flush();
  assert.equal(h.node("control-total").textContent, "1");
  assert.equal(h.node("runtime-roster").children.length, 4);
});

test("unreadable operations clear old counts and phone panels preserve the draft", async () => {
  const h = harness(); await flush(); h.type("Keep this draft");
  await h.node("view-operations").fire("click");
  assert.equal(h.node("layout").dataset.view, "operations");
  assert.equal(h.node("view-operations")["aria-pressed"], "true");
  await h.node("view-projects").fire("click");
  assert.equal(h.node("layout").dataset.view, "projects");
  await h.node("view-workspace").fire("click");
  assert.equal(h.node("prompt").value, "Keep this draft");
  h.handle((url) => url.startsWith("/api/control?") ? h.response({total: "secret"}) : undefined);
  await h.advance(10000);
  assert.equal(h.node("control-total").textContent, "…");
  assert.equal(h.node("runtime-roster").children.length, 0);
  assert.match(h.node("control-status").textContent, /unavailable/);
  assert.equal(h.calls.some((call) => call.init.method !== "GET"), false);
});

test("new requests and selected task actions stay separate; drafts survive switches and refresh", async () => {
  const h = harness(); await flush();
  h.type("Local draft α"); await h.node("refresh").fire("click");
  assert.equal(h.node("prompt").value, "Local draft α");
  await h.clickTask("A"); assert.equal(h.node("prompt").disabled, true);
  assert.match(h.node("composer-help").textContent, /Follow up chat is not connected/);
  await h.clickProject("beta"); h.type("Draft β"); await h.clickProject("alpha");
  assert.equal(h.node("task-title").textContent, "A");
  h.node("new-task").fire("click"); assert.equal(h.node("prompt").value, "Local draft α");
  assert.equal(h.node("prompt").disabled, false);
});

test("old stream callbacks cannot contaminate a new task and exact 64 bit IDs deduplicate", async () => {
  const h = harness(); await flush(); await h.clickTask("A"); const old = h.streams.at(-1);
  old.emit(1, "A"); await h.clickTask("B"); const current = h.streams.at(-1);
  assert.equal(old.closed, true); old.emit(2, "A");
  assert.equal(h.node("activity").children.length, 0);
  for (const id of ["9007199254740992", "9007199254740993", "9223372036854775807"]) current.emit(id, "B");
  current.emit("9223372036854775807", "B"); current.emit(9, "A");
  assert.equal(h.node("activity").children.length, 3);
  assert.equal(h.node("task-title").textContent, "B");
});

test("a late task detail cannot overwrite the selected task", async () => {
  const h = harness(); await flush(); const delayed = deferred();
  h.handle((url) => url === "/api/jobs/A" ? delayed.promise : undefined);
  await h.clickTask("A"); await h.clickTask("B");
  delayed.resolve(h.response(job("A"))); await flush();
  assert.equal(h.node("request-text").textContent, "Request B");
  assert.equal(h.streams.at(-1).url, "/api/jobs/B/events");
});

test("a late project list cannot replace a newer project or erase its draft", async () => {
  const h = harness(); await flush(); const delayed = deferred();
  h.handle((url) => url.includes("project_id=alpha") ? delayed.promise : undefined);
  h.node("refresh").fire("click"); await h.clickProject("beta"); h.type("Keep β");
  delayed.resolve(h.response({jobs: [job("A")], next_cursor: null})); await flush();
  assert.equal(h.node("prompt").value, "Keep β");
  assert.deepEqual(h.node("task-list").children.map((item) => item.dataset.job), ["C"]);
});

test("uncertain submission is attempted once and retains the request", async () => {
  const h = harness(); await flush(); const delayed = deferred();
  h.handle((url, init) => url === "/api/jobs" && init.method === "POST" ? delayed.promise : undefined);
  h.type("Do not duplicate"); const pending = h.node("composer").fire("submit");
  h.node("composer").fire("submit"); assert.equal(h.node("create-task").disabled, true);
  await h.node("refresh").fire("click"); delayed.reject(new Error("Synthetic transport")); await pending;
  assert.equal(h.calls.filter((item) => item.url === "/api/jobs" && item.init.method === "POST").length, 1);
  assert.equal(h.node("prompt").value, "Do not duplicate");
  assert.match(h.node("notice").textContent, /may already have succeeded/);
});

test("submission keeps its project identity if navigation changes before success", async () => {
  const h = harness(); await flush(); const delayed = deferred();
  h.handle((url, init) => url === "/api/jobs" && init.method === "POST" ? delayed.promise : undefined);
  h.type("Create for alpha"); const pending = h.node("composer").fire("submit");
  await h.clickProject("beta"); delayed.resolve(h.response(job("created", "alpha"), 201));
  await pending; assert.equal(new URL(h.url()).searchParams.get("project"), "beta");
  assert.equal(h.node("prompt").disabled, false);
});

test("confirmed late creation clears an unchanged draft without stealing a new-task selection", async () => {
  const h = harness(); await flush(); const delayed = deferred();
  h.handle((url, init) => url === "/api/jobs" && init.method === "POST" ? delayed.promise : undefined);
  h.type("Confirmed once"); const pending = h.node("composer").fire("submit");
  h.node("new-task").fire("click"); delayed.resolve(h.response(job("created"), 201)); await pending;
  assert.equal(h.node("prompt").value, "");
  assert.equal(new URL(h.url()).searchParams.get("task"), null);
  assert.equal(h.node("create-task").disabled, true);
});

test("navigation during the post-submit history refresh is never overwritten", async () => {
  for (const target of ["project", "task"]) {
    const h = harness(); await flush(); const delayed = deferred();
    h.handle((url) => url.includes("project_id=alpha") ? delayed.promise : undefined);
    h.type("Create without stealing selection"); const pending = h.node("composer").fire("submit");
    await flush();
    if (target === "project") await h.clickProject("beta");
    else await h.clickTask("B");
    delayed.resolve(h.response({jobs: [job("new")], next_cursor: null})); await pending; await flush();
    const url = new URL(h.url());
    assert.equal(url.searchParams.get("project"), target === "project" ? "beta" : "alpha");
    assert.equal(url.searchParams.get("task"), target === "task" ? "B" : null);
    assert.equal(h.calls.filter((item) => item.url === "/api/jobs" && item.init.method === "POST").length, 1);
  }
});

test("ledger reconnect preserves records; a fresh task subscription replays; DOM is bounded", async () => {
  const h = harness(); await flush(); await h.clickTask("B"); const source = h.streams.at(-1);
  source.onopen(); source.emit(1, "B", {text: "<img src=x onerror=alert(1)>\nUnicode 日本"});
  assert.equal(h.node("activity").children[0].children[2].textContent, "<img src=x onerror=alert(1)>\nUnicode 日本");
  source.onerror(); source.emit(1, "B");
  assert.equal(h.node("activity").children.length, 1);
  for (let n = 2; n < 503; n++) source.emit(n, "B");
  assert.equal(h.node("activity").children.length, 500);
  assert.equal(h.node("event-limit").hidden, false);
  await h.clickTask("A"); await h.clickTask("B"); h.streams.at(-1).emit(1, "B");
  assert.equal(h.node("activity").children.length, 1);
  assert.equal(new URL(h.url()).searchParams.get("task"), "B");
});

test("empty configuration disables submission and makes no mutation", async () => {
  const h = harness({empty: true}); await flush();
  assert.equal(h.node("project-empty").hidden, false);
  assert.equal(h.node("prompt").disabled, true);
  h.node("composer").fire("submit");
  assert.equal(h.calls.some((item) => item.init.method === "POST"), false);
});

test("a full new page cannot leave intermediate task history unreachable", async () => {
  const h = harness(); await flush();
  const recent = Array.from({length: 50}, (_, n) => job(`new-${n}`));
  h.handle((url) => url.startsWith("/api/jobs?") ? h.response({jobs: recent, next_cursor: "new-49"}) : undefined);
  await h.node("refresh").fire("click");
  assert.equal(h.node("load-older").hidden, false);
  await h.node("load-older").fire("click");
  assert.match(h.calls.at(-1).url, /before_id=new-49/);
});

test("one detail-loaded new task cannot hide a gap in the paginated history", async () => {
  const h = harness({jobs: Array.from({length: 50}, (_, n) => job(`old-${n}`))}); await flush();
  let listFailed = true;
  const recent = Array.from({length: 50}, (_, n) => job(n === 0 ? "created" : `recent-${n}`));
  h.handle((url, init) => {
    if (url === "/api/jobs" && init.method === "POST") return h.response(job("created"), 201);
    if (url === "/api/jobs/created") return h.response(job("created"));
    if (url.startsWith("/api/jobs?")) return listFailed
      ? h.response({detail: "Task list is unavailable."}, 503)
      : h.response({jobs: recent, next_cursor: "recent-49"});
  });
  h.type("Create once"); await h.node("composer").fire("submit"); await flush();
  assert.equal(h.node("task-title").textContent, "created");
  listFailed = false; await h.node("refresh").fire("click");
  await h.node("load-older").fire("click");
  assert.match(h.calls.at(-1).url, /before_id=recent-49/);
});

test("task actions are sent once and a late result cannot change project selection", async () => {
  const h = harness(); await flush(); await h.clickTask("A"); const delayed = deferred();
  h.handle((url) => url === "/api/jobs/A/plan" ? delayed.promise : undefined);
  const pending = h.node("next-action").fire("click");
  h.node("next-action").fire("click");
  assert.equal(h.node("next-action").disabled, true);
  await h.clickProject("beta");
  delayed.resolve(h.response({state: "queued"})); await pending;
  assert.equal(new URL(h.url()).searchParams.get("project"), "beta");
  assert.equal(h.calls.filter((item) => item.url === "/api/jobs/A/plan").length, 1);
  assert.equal(h.node("task-title").textContent, "What would you like to work on?");
});

test("malformed ledger identities close the stream and refresh replays safely", async () => {
  for (const identity of ["0", "1.5", "9223372036854775808"]) {
    const h = harness(); await flush(); await h.clickTask("A"); const source = h.streams.at(-1);
    source.emit(identity, "A");
    assert.equal(source.closed, true);
    assert.equal(h.node("activity").children.length, 0);
    assert.match(h.node("stream-status").textContent, /Invalid ledger event/);
    await h.node("refresh").fire("click");
    const resumed = h.streams.at(-1); assert.notEqual(resumed, source);
    resumed.emit(1, "A"); resumed.emit(1, "A");
    assert.equal(h.node("activity").children.length, 1);
  }
});

test("oversized UTF8 requests stay local without a submission", async () => {
  const h = harness(); await flush(); h.type("日".repeat(87382));
  await h.node("composer").fire("submit");
  assert.equal(h.calls.some((item) => item.init.method === "POST"), false);
  assert.match(h.node("notice").textContent, /within 256 KiB/);
  assert.equal(h.node("prompt").value.length, 87382);
});

test("keyboard submission ignores composition and locks a single pending request", async () => {
  const h = harness(); await flush(); const delayed = deferred();
  h.handle((url, init) => url === "/api/jobs" && init.method === "POST" ? delayed.promise : undefined);
  h.type("Keyboard request");
  h.node("prompt").fire("keydown", {key: "Enter", ctrlKey: true, isComposing: true});
  assert.equal(h.calls.some((item) => item.init.method === "POST"), false);
  h.node("prompt").fire("keydown", {key: "Enter", metaKey: true});
  h.node("prompt").fire("keydown", {key: "Enter", ctrlKey: true});
  assert.equal(h.calls.filter((item) => item.url === "/api/jobs" && item.init.method === "POST").length, 1);
  delayed.resolve(h.response(job("new"), 201)); await flush();
  assert.equal(new URL(h.url()).searchParams.get("task"), "new");
});

const instant = "2026-01-01T00:00:00Z";
const canonical = (value) => Array.isArray(value) ? value.map(canonical) : value && typeof value === "object"
  ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])])) : value;
function reviewFixture(id = "A", project = "alpha") {
  const bundle = {schema_version: 1, job_id: id, project_id: project, scan_event_id: 5,
    diff: {base_commit: "a".repeat(40), truncated: false, files: [{path: "src/example.txt", previous_path: null,
      status: "modified", patch: "--- a/src/example.txt\n+++ b/src/example.txt\n@@ -1 +1 @@\n-old\n+new λ\n", summary: null}]},
    risks: ["No test run for this frozen snapshot."], verification: [{outcome: "passed", created_at: instant,
      command_args: ["python", "-m", "pytest"], exit_code: 0, duration_ms: 15}]};
  const bundle_hash = crypto.createHash("sha256").update(JSON.stringify(canonical(bundle))).digest("hex");
  const snapshot = {job_id: id, bundle_hash, bundle};
  const approval = {approval_id: `permission-${id}`, bundle_hash, expires_at: "2026-01-01T00:15:00Z",
    state: "pending", decision: null, decided_at: null, event_id: 6};
  const status = {job_id: id, project_id: project, job_state: "waiting_approval", bundle_hash,
    review_current: true, server_time: instant, approval};
  return {snapshot, approval, status};
}
async function reviewHarness({pending = true, url, id = "A", project = "alpha"} = {}) {
  const f = reviewFixture(id, project), records = new Map([[id, f]]);
  if (!pending) { f.status.approval = null; f.status.job_state = "review_ready"; }
  const h = harness({jobs: [{...job(id, project), state: f.status.job_state}, job("B"), job("C", "beta")], url});
  let custom = null;
  h.handle((url, init) => {
    const result = custom?.(url, init);
    if (result !== undefined) return result;
    const task = decodeURIComponent(url.split("/")[3]), item = records.get(task);
    if (item && url.endsWith("/approval") && init.method === "GET") return h.response(structuredClone(item.status));
    if (item && url.endsWith("/review-bundle") && init.method === "GET") return h.response(structuredClone(item.snapshot));
  });
  await flush(); if (!url) await h.clickTask(id);
  return {...h, ...f, records, intercept(callback) { custom = callback; }};
}
const writes = (h) => h.calls.filter((call) => call.init.method === "POST");
const decisionBody = (h) => JSON.parse(writes(h).at(-1).init.body);
const enableDecision = async (h) => { await h.node("review-view").fire("click"); };
const assertLocked = (h) => {
  for (const name of ["approve", "changes", "reject"]) assert.equal(h.node(`review-${name}`).disabled, true);
};

for (const failed of [false, true]) {
  test(`review waits for digest completion and ${failed ? "locks on failure" : "then enables viewed decisions"}`, async (t) => {
    const gate = deferred(), f = reviewFixture(); let started = false;
    t.after(() => gate.resolve());
    const h = harness({jobs: [{...job("A"), state: "waiting_approval"}], digest: async (...args) => {
      started = true; await gate.promise; return crypto.webcrypto.subtle.digest(...args);
    }});
    h.handle((url) => {
      if (url.endsWith("/approval")) return h.response(structuredClone(f.status));
      if (url.endsWith("/review-bundle")) return h.response(structuredClone(f.snapshot));
    });
    await flush();
    h.node("task-list").children.find((item) => item.dataset.job === "A").fire("click");
    await new Promise(setImmediate);
    assert.equal(started, true); assertLocked(h);
    assert.match(h.node("review-message").textContent, /Reading stored review state/);
    let settled = false;
    const draining = flush().then(() => { settled = true; });
    await new Promise(setImmediate);
    assert.equal(settled, false);
    if (failed) gate.reject(new Error("Synthetic digest failure."));
    else gate.resolve();
    await draining;
    if (failed) {
      assertLocked(h);
      assert.equal(h.node("review-content").hidden, true);
      assert.match(h.node("review-error").textContent, /Synthetic digest failure/);
    } else {
      assert.equal(h.node("review-id").textContent, f.approval.approval_id);
      await enableDecision(h);
      assert.equal(h.node("review-approve").disabled, false);
    }
    assert.equal(writes(h).length, 0);
    assert.equal(pendingDigests.size, 0);
  });
}

test("loading and URL reload are read only; original ID, scope and expiry precede every decision", async () => {
  const h = await reviewHarness({url: "http://localhost/?project=alpha&task=A"});
  assert.equal(writes(h).length, 0); assertLocked(h);
  assert.equal(h.node("review-id").textContent, "permission-A");
  assert.equal(h.node("review-hash").textContent, h.snapshot.bundle_hash);
  assert.match(h.node("review-scope").textContent, /Project alpha · Task A · 1 changed file/);
  assert.match(h.node("review-expiry").textContent, /Original decision deadline/);
  assert.equal(h.node("review-diff").hidden, true);
  await enableDecision(h);
  assert.equal(h.node("review-approve").disabled, false);
  assert.equal(h.node("review-view")["aria-expanded"], "true");
  await h.node("review-reload").fire("click"); assert.equal(writes(h).length, 0);
  assert.equal(h.node("review-id").textContent, "permission-A");
});

for (const [name, decision, target] of [["approve", "approved", "approved"],
  ["changes", "changes_requested", "running"], ["reject", "rejected", "rejected"]]) {
  test(`${decision} sends one exact decision and never promotes or starts a provider`, async () => {
    const h = await reviewHarness(); await enableDecision(h); const delayed = deferred();
    h.intercept((url, init) => url === "/api/approvals/permission-A" && init.method === "POST" ? delayed.promise : undefined);
    const pending = h.node(`review-${name}`).fire("click");
    h.node(`review-${name}`).fire("click"); h.node("review-request").fire("click"); assertLocked(h);
    assert.equal(writes(h).length, 1);
    assert.deepEqual(decisionBody(h), {decision, bundle_hash: h.snapshot.bundle_hash});
    h.approval.state = "decided"; h.approval.decision = decision; h.approval.decided_at = instant;
    h.status.job_state = target; h.jobs.get("A").state = target;
    delayed.resolve(h.response({job_id: "A", approval_id: "permission-A", bundle_hash: h.snapshot.bundle_hash, decision}));
    await pending; await flush(); assertLocked(h);
    assert.match(h.node("review-decision").textContent, new RegExp(decision.replaceAll("_", " ")));
    assert.equal(writes(h).length, 1);
    assert.equal(h.calls.some((call) => /promote|native|\/start|\/resume/.test(call.url)), false);
  });
}

test("decision preparation is explicit, requires viewed evidence and recovers its stored identity", async () => {
  const h = await reviewHarness({pending: false});
  await h.node("review-request").fire("click"); assert.equal(writes(h).length, 0);
  await enableDecision(h);
  h.intercept((url, init) => {
    if (url.endsWith("/approval-request") && init.method === "POST") {
      h.status.approval = h.approval; h.status.job_state = "waiting_approval";
      h.jobs.get("A").state = "waiting_approval";
      return h.response({job_id: "A", ...h.approval});
    }
  });
  await h.node("review-request").fire("click"); await flush();
  assert.deepEqual(decisionBody(h), {bundle_hash: h.snapshot.bundle_hash});
  assert.equal(h.node("review-id").textContent, "permission-A");
  assert.equal(h.node("review-approve").disabled, false);
  await h.node("review-reload").fire("click"); assert.equal(writes(h).length, 1);
});

test("pending approval expires at its original deadline without waiting for a refresh", async () => {
  const h = await reviewHarness(); await enableDecision(h);
  await h.advance(900000); assertLocked(h);
  assert.match(h.node("review-message").textContent, /expired/);
  await h.node("review-approve").fire("click"); assert.equal(writes(h).length, 0);
});

test("slow review reads cannot extend the approval deadline", async () => {
  const h = await reviewHarness(), delayed = deferred();
  h.intercept((url) => url.endsWith("/review-bundle") ? delayed.promise : undefined);
  const loading = h.node("review-reload").fire("click"); await flush(); await h.advance(900000);
  delayed.resolve(h.response(h.snapshot)); await loading; await enableDecision(h); assertLocked(h);
  assert.match(h.node("review-message").textContent, /expired/);
});

for (const unreadable of [false, true]) {
  test(`a committed decision with ${unreadable ? "unreadable" : "lost"} response reconciles through GET without retry`, async () => {
    const h = await reviewHarness(); await enableDecision(h);
    h.intercept((url, init) => {
      if (url === "/api/approvals/permission-A" && init.method === "POST") {
        h.approval.state = "decided"; h.approval.decision = "approved"; h.approval.decided_at = instant;
        h.status.job_state = "approved"; h.jobs.get("A").state = "approved";
        return unreadable ? {ok: true, json: async () => { throw new Error("broken JSON"); }}
          : Promise.reject(new Error("transport failed"));
      }
    });
    await h.node("review-approve").fire("click"); await flush(); assertLocked(h);
    assert.match(h.node("review-decision").textContent, /approved/);
    assert.equal(writes(h).length, 1);
    assert.equal(h.node("review-error").hidden, true);
    assert.equal(h.calls.at(-1).init.method, "GET");
  });
}

test("unconfirmed writes stay locked across navigation, reload and alternate decision clicks", async () => {
  const h = await reviewHarness(); await enableDecision(h);
  h.intercept((url, init) => init.method === "POST" ? Promise.reject(new Error("transport failed")) : undefined);
  await h.node("review-approve").fire("click"); await flush();
  await h.clickTask("B"); await h.clickTask("A"); await enableDecision(h);
  await h.node("review-reload").fire("click"); assertLocked(h);
  await h.node("review-reject").fire("click"); assert.equal(writes(h).length, 1);
  assert.match(h.node("review-error").textContent, /may already have succeeded/);
});

test("stale worktree conflict does not regenerate a snapshot or reenable the same approval", async () => {
  const h = await reviewHarness(); await enableDecision(h);
  h.intercept((url, init) => init.method === "POST" ? h.response({detail: "Worktree content changed after review."}, 409) : undefined);
  await h.node("review-approve").fire("click"); await h.node("review-reload").fire("click"); assertLocked(h);
  assert.match(h.node("review-error").textContent, /changed after review/);
  assert.equal(writes(h).length, 1);
  assert.equal(writes(h)[0].url, "/api/approvals/permission-A");
});

test("historical review requires explicit new snapshot preparation and viewing before another decision", async () => {
  const h = await reviewHarness(), fresh = reviewFixture();
  fresh.snapshot.bundle.scan_event_id = 20;
  fresh.snapshot.bundle_hash = crypto.createHash("sha256").update(JSON.stringify(canonical(fresh.snapshot.bundle))).digest("hex");
  fresh.status.bundle_hash = fresh.snapshot.bundle_hash; fresh.status.approval = null; fresh.status.job_state = "review_ready";
  h.status.review_current = false; h.status.job_state = "review_ready";
  h.approval.state = "decided"; h.approval.decision = "changes_requested"; h.approval.decided_at = instant;
  await h.node("review-reload").fire("click"); await enableDecision(h); assertLocked(h);
  assert.match(h.node("review-message").textContent, /Historical review/);
  assert.equal(h.node("review-prepare").hidden, false);
  h.intercept((url, init) => {
    if (url.endsWith("/review-bundle") && init.method === "POST") {
      h.records.set("A", fresh); return h.response(fresh.snapshot);
    }
  });
  await h.node("review-prepare").fire("click"); await flush();
  assert.equal(writes(h).length, 1); assert.deepEqual(decisionBody(h), {});
  assert.equal(h.node("review-id").textContent, "Not requested");
  assert.equal(h.node("review-diff").hidden, true); assert.equal(h.node("review-request").disabled, true);
  await enableDecision(h); assert.equal(h.node("review-request").disabled, false);
});

test("late A to B to A approval reads cannot overwrite fresh review identity", async () => {
  const h = await reviewHarness(), old = deferred(); let intercepted = false;
  h.intercept((url) => {
    if (url.endsWith("/approval") && !intercepted) { intercepted = true; return old.promise; }
  });
  const loading = h.node("review-reload").fire("click"); await flush();
  await h.clickTask("B"); h.approval.approval_id = "fresh-A"; await h.clickTask("A"); await enableDecision(h);
  old.resolve(h.response({...h.status, approval: {...h.approval, approval_id: "obsolete-A"}}));
  await loading; await flush();
  assert.equal(h.node("review-id").textContent, "fresh-A");
  assert.equal(h.node("review-approve").disabled, false);
});

test("a pending write stays task scoped while navigation changes and reconciles before reenabling", async () => {
  const h = await reviewHarness(); await enableDecision(h); const delayed = deferred();
  h.intercept((url, init) => init.method === "POST" ? delayed.promise : undefined);
  const action = h.node("review-approve").fire("click");
  await h.clickTask("B"); await h.clickTask("A"); assertLocked(h);
  await h.node("review-reject").fire("click"); assert.equal(writes(h).length, 1);
  h.approval.state = "decided"; h.approval.decision = "approved"; h.approval.decided_at = instant;
  h.status.job_state = "approved";
  delayed.resolve(h.response({job_id: "A", bundle_hash: h.snapshot.bundle_hash, approval_id: "permission-A", decision: "approved"}));
  await action; await flush(); assertLocked(h);
  assert.equal(h.node("review-id").textContent, "permission-A");
  assert.match(h.node("review-decision").textContent, /approved/);
});

for (const corruption of ["task", "project", "hash", "content", "status", "expiry", "decision"]) {
  test(`malformed ${corruption} evidence fails closed without a mutation`, async () => {
    const h = await reviewHarness(); await enableDecision(h);
    h.intercept((url) => {
      if (url.endsWith("/approval") && ["status", "expiry", "decision"].includes(corruption)) {
        const data = structuredClone(h.status);
        if (corruption === "status") data.project_id = "beta";
        if (corruption === "expiry") data.approval.expires_at = "not a date";
        if (corruption === "decision") data.approval.decision = "approved";
        return h.response(data);
      }
      if (url.endsWith("/review-bundle")) {
        const data = structuredClone(h.snapshot);
        if (corruption === "task") data.job_id = "B";
        if (corruption === "project") data.bundle.project_id = "beta";
        if (corruption === "hash") data.bundle_hash = "0".repeat(64);
        if (corruption === "content") data.bundle.diff.files[0].patch = "+unexpected change";
        return h.response(data);
      }
    });
    await h.node("review-reload").fire("click"); assertLocked(h);
    await h.node("review-approve").fire("click"); assert.equal(writes(h).length, 0);
    assert.equal(h.node("review-content").hidden, true);
    assert.equal(h.node("review-error").hidden, false);
  });
}

test("diff, paths, risks and verification arguments render as text, including incomplete content", async () => {
  const h = await reviewHarness(), marker = '<img src=x onerror="unsafe()">';
  h.snapshot.bundle.diff.files[0].path = marker;
  h.snapshot.bundle.diff.files[0].patch = marker;
  h.snapshot.bundle.diff.files.push({path: "image.bin", previous_path: null, status: "added", patch: null, summary: "Binary content not displayed."});
  h.snapshot.bundle.diff.truncated = true;
  h.snapshot.bundle.risks = [marker]; h.snapshot.bundle.verification[0].command_args = [marker];
  h.snapshot.bundle_hash = crypto.createHash("sha256").update(JSON.stringify(canonical(h.snapshot.bundle))).digest("hex");
  h.status.bundle_hash = h.approval.bundle_hash = h.snapshot.bundle_hash;
  h.approval.approval_id = "permission-safe-rendering";
  await h.node("review-reload").fire("click"); await enableDecision(h);
  assert.equal(h.node("review-patches").children[0].children[1].textContent, marker);
  assert.match(h.node("review-files").children[0].textContent, /<img/);
  assert.equal(h.node("review-risks").children[0].textContent, marker);
  assert.match(h.node("review-risks").children[1].textContent, /incomplete/);
  assert.equal(h.node("review-checks").children[0].children[2].textContent, JSON.stringify([marker]));
  assert.match(h.node("review-patches").children[1].children[1].textContent, /Binary content/);
  assert.equal(writes(h).length, 0);
});

test("empty review is prepared only by an explicit click, never by opening or refreshing", async () => {
  const h = await reviewHarness({pending: false});
  h.status.bundle_hash = null; h.status.review_current = false;
  await h.node("review-reload").fire("click");
  assert.equal(writes(h).length, 0); assertLocked(h);
  assert.equal(h.node("review-prepare").disabled, false);
  h.intercept((url, init) => {
    if (url.endsWith("/review-bundle") && init.method === "POST") {
      h.status.bundle_hash = h.snapshot.bundle_hash; h.status.review_current = true;
      return h.response(h.snapshot);
    }
  });
  await h.node("review-prepare").fire("click"); await flush();
  assert.equal(writes(h).length, 1);
  assert.equal(h.node("review-request").disabled, true);
  assert.equal(h.node("review-id").textContent, "Not requested");
});

test("lost approval preparation response recovers one original ID without resubmission", async () => {
  const h = await reviewHarness({pending: false}); await enableDecision(h);
  h.intercept((url, init) => {
    if (url.endsWith("/approval-request") && init.method === "POST") {
      h.status.approval = h.approval; h.status.job_state = "waiting_approval";
      h.jobs.get("A").state = "waiting_approval";
      return Promise.reject(new Error("transport failed"));
    }
  });
  await h.node("review-request").fire("click"); await flush();
  assert.equal(writes(h).length, 1);
  assert.equal(h.node("review-id").textContent, "permission-A");
  assert.equal(h.node("review-approve").disabled, false);
  assert.equal(h.node("review-error").hidden, true);
});

test("immutable approval expiry cannot be silently extended in a later response", async () => {
  const h = await reviewHarness(); await enableDecision(h);
  h.approval.expires_at = "2026-01-01T00:45:00Z";
  await h.node("review-reload").fire("click"); assertLocked(h);
  assert.match(h.node("review-error").textContent, /identity changed/);
  assert.equal(writes(h).length, 0);
});

test("expired authority and missing verification are displayed honestly without creating new requests", async () => {
  const h = await reviewHarness();
  h.approval.state = "expired"; h.status.server_time = h.approval.expires_at;
  const fresh = reviewFixture(); fresh.snapshot.bundle.verification = [];
  fresh.snapshot.bundle_hash = crypto.createHash("sha256").update(JSON.stringify(canonical(fresh.snapshot.bundle))).digest("hex");
  fresh.status.bundle_hash = fresh.approval.bundle_hash = fresh.snapshot.bundle_hash;
  fresh.approval.approval_id = "permission-expired"; fresh.approval.state = "expired";
  fresh.status.server_time = fresh.approval.expires_at; h.records.set("A", fresh);
  await h.node("review-reload").fire("click"); await enableDecision(h); assertLocked(h);
  assert.match(h.node("review-message").textContent, /does not extend/);
  assert.match(h.node("review-checks").children[0].textContent, /not proof that tests passed/);
  assert.equal(h.node("review-request").hidden, true);
  assert.equal(writes(h).length, 0);
});

test("refresh preserves frozen diff nodes and an explicitly collapsed diff", async () => {
  const h = await reviewHarness(); await enableDecision(h);
  const patch = h.node("review-patches").children[0];
  await h.node("review-view").fire("click");
  assert.equal(h.node("review-diff").hidden, true);
  await h.node("review-reload").fire("click");
  assert.equal(h.node("review-diff").hidden, true);
  assert.equal(h.node("review-patches").children[0], patch);
  assert.equal(h.node("review-view")["aria-expanded"], "false");
});

test("switching to a task without review clears the prior disclosure and evidence", async () => {
  const h = await reviewHarness(); await enableDecision(h);
  await h.clickTask("B"); assertLocked(h);
  assert.equal(h.node("review-content").hidden, true);
  assert.equal(h.node("review-view").textContent, "View unified diff");
  assert.equal(h.node("review-view")["aria-expanded"], "false");
  assert.equal(h.node("review-patches").children.length, 0);
});
