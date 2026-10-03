"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");
const code = fs.readFileSync(path.join(__dirname, "../workspace_ui/workspace.js"), "utf8");
const flush = async () => { for (let n = 0; n < 8; n++) await new Promise(setImmediate); };
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
  const nodes = new Map(), streams = [], timers = new Map(), calls = [], jobs = new Map();
  let handler = null, timerId = 0, currentURL = "http://localhost/?project=alpha";
  for (const item of options.jobs || [job("A"), job("B"), job("C", "beta")]) jobs.set(item.id, item);
  const document = {hidden: false, activeElement: null};
  class Element {
    constructor(tag) { this.tag = tag; this.children = []; this.listeners = {}; this.dataset = {};
      this.value = ""; this.textContent = ""; this.hidden = false; this.disabled = false; }
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
  const response = (data, status = 200) => ({ok: status < 400, json: async () => data});
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
    return response(jobs.get(id) ? {...jobs.get(id)} : {detail: "Job does not exist."}, jobs.has(id) ? 200 : 404);
  };
  const context = {
    document, EventSource, fetch, URL, URLSearchParams, TextEncoder, console,
    location: {get href() { return currentURL; }, get search() { return new URL(currentURL).search; }},
    history: {replaceState(_, __, url) { currentURL = String(url); }},
    window: {addEventListener() {}},
    setTimeout(callback) { timers.set(++timerId, callback); return timerId; },
    clearTimeout(id) { timers.delete(id); }, setInterval() {},
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
  };
}

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
