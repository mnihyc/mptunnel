"use strict";

// node --test tests/dashboard_peer_refresh.test.cjs
// Executes the complete production closure, replacing only startup. No helper
// implementation is copied. The clock, DOM and HTTP responses are controlled.
// DASHBOARD_SOURCE can point at the unpatched dashboard for a red/green run.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const { test } = require("node:test");
const source = fs.readFileSync(process.env.DASHBOARD_SOURCE || "assets/dashboard/dashboard.js", "utf8");

class Element {
  constructor() {
    this.children = [];
    this.selectors = new Map();
    this.dataset = {};
    this.attributes = {};
    this.hidden = false;
    this.value = "";
    this.classList = { add() {}, remove() {}, toggle() {} };
  }
  set textContent(value) { this.text = String(value); this.children = []; }
  get textContent() { return (this.text || "") + this.children.map((node) => node.textContent || "").join(" "); }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.text = ""; this.children = nodes; }
  setAttribute(key, value) { this.attributes[key] = value; }
  removeAttribute(key) { delete this.attributes[key]; }
  getAttribute(key) { return this.attributes[key]; }
  showModal() { this.open = true; }
  close() { this.open = false; }
  focus() {}
  addEventListener() {}
  removeEventListener() {}
  querySelectorAll() { return []; }
  querySelector(selector) {
    if (!this.selectors.has(selector)) this.selectors.set(selector, new Element());
    return this.selectors.get(selector);
  }
  cloneNode() { return new Element(); }
  closest() { return this.querySelector("table"); }
}

const session = { service: "mpp_outbound", service_index: 0, service_name: "test-peer", session_id: "123" };
function result(id, code = "ok", bytes = 0, atUs = 1_000_000, owner = session) {
  return {
    ...owner, request_id: String(id), received_unix_ms: 100_000 + Number(id) * 1000, code,
    paths: code === "ok" ? [{
      path: "path-1", path_id: "0", underlay: "tcp", state: "active",
      direction: "server_to_client", usage: "available", usage_direction: "client_to_server",
      native_delivery: { epoch: "7", direction: "server_to_client", acked_bytes: String(bytes), sampled_at_us: String(atUs) }
    }] : []
  };
}

function harness() {
  const clock = { wall: 100_000, mono: 0 };
  const ids = new Map();
  const scheduled = new Map();
  const requests = [];
  let nextTimer = 0;
  let respond = () => { throw new Error("unexpected request"); };
  const window = {
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    sessionStorage: { removeItem() {} },
    setTimeout(fn, delay) { const id = ++nextTimer; scheduled.set(id, { fn, delay }); return id; },
    clearTimeout(id) { scheduled.delete(id); },
    requestAnimationFrame() {},
    fetch: async (url, options) => {
      requests.push({ url, options, at: clock.mono });
      const answer = await respond(url, options);
      if (answer instanceof Error) throw answer;
      const status = answer.httpStatus || 200;
      return { ok: status >= 200 && status < 300, status, text: async () => JSON.stringify(answer.body || answer) };
    }
  };
  const context = vm.createContext({
    window, document: {
      getElementById(id) {
        if (!ids.has(id)) {
          const element = new Element();
          if (id === "overview-peer-path-group-template") element.content = { firstElementChild: new Element() };
          ids.set(id, element);
        }
        return ids.get(id);
      },
      createElement: () => new Element(), querySelectorAll: () => [], hidden: false
    },
    Node: Element, Headers, AbortController,
    Date: { now: () => clock.wall }, performance: { now: () => clock.mono }
  });
  const startup = "  initialize();\n})();";
  assert.ok(source.includes(startup));
  vm.runInContext(source.replace(startup, `  globalThis.app = {
    state, elements, requestPeerStatus, requestPeerStatuses, renderSelectedPeerResult, renderDiagnostics,
    pathQualities, peerResultResidenceMs, peerSessionKey, clearToken,
    ingestPeerResult: typeof ingestPeerResult === "function" ? ingestPeerResult : null,
    reconcilePeerDiagnostics: typeof reconcilePeerDiagnostics === "function" ? reconcilePeerDiagnostics : null
  };\n})();`), context);
  const app = context.app;
  app.state.bearerToken = "test-only";
  app.state.refreshIntervalMs = 0;
  const h = {
    ...app, clock, requests, scheduled,
    advance(ms) { clock.wall += ms; clock.mono += ms; },
    respond(fn) { respond = fn; },
    setStatus(sessions = [session], cached = [], started = 1) {
      const previous = app.state.status && app.state.status.started_unix_ms;
      app.state.status = {
        started_unix_ms: started, generated_unix_ms: clock.wall,
        outbounds: [...new Set(sessions.filter(s => s.service === "mpp_outbound").map(s => s.service_name))]
          .map(name => ({ name, protocol: "mpp" })),
        diagnostics: { peer_sessions: sessions, peer_results: cached },
        controls: { peer_diagnostics: { supported: true } }
      };
      app.state.lastReceivedAt = clock.wall;
      if (app.reconcilePeerDiagnostics) app.reconcilePeerDiagnostics(previous);
      app.renderDiagnostics();
    },
    async deliver(answer, advance = 1100) {
      h.advance(advance);
      respond = () => answer;
      await app.requestPeerStatus("manual", false);
    },
    select(owner) { app.state.selectedPeerSessionKey = app.peerSessionKey(owner); app.renderSelectedPeerResult(); },
    rows() { return app.elements.peerPathsBody.children; },
    overviewRows(index = 0) {
      return app.elements.overviewPeerPathsList.children[index].querySelector("[data-peer-outbound-paths]").children;
    },
    rate() {
      const row = h.rows()[0];
      if (!row) return null;
      return row.children.find((cell) => cell.dataset.label === "Rate").children[0].children[0].textContent;
    },
    peer(owner = session) { return app.state.peerDiagnostics && app.state.peerDiagnostics.get(app.peerSessionKey(owner)); },
    snapshot(owner = session) { return h.peer(owner).snapshot; }
  };
  h.setStatus();
  return h;
}

async function establish(h) {
  await h.deliver(result(1));
  await h.deliver(result(2, "ok", 1000, 2_000_000));
  assert.equal(h.rate(), "↓ 8 Kbps");
}

test("transient unavailable preserves rows and rate; recovery needs no second warm-up", async () => {
  const h = harness();
  await establish(h);
  await h.deliver(result(3, "unavailable"));
  assert.equal(h.rows().length, 1, "a failed acquisition must not replace the last successful inventory");
  assert.equal(h.rate(), "↓ 8 Kbps");
  assert.match(h.elements.overviewPeerState.textContent, /Unavailable/);
  assert.match(h.elements.overviewPeerContext.textContent, /last successful sample/);
  await h.deliver(result(4, "ok", 3000, 4_000_000));
  assert.equal(h.rate(), "↓ 8 Kbps", "the prior native baseline survived the refusal");
});

test("rapid sequential clicks do not self-rate-limit or enqueue delayed work", async () => {
  const h = harness();
  await h.deliver(result(1), 0);
  const timers = h.scheduled.size;
  for (let click = 0; click < 50; click += 1) await h.deliver(result(2, "unavailable"), 0);
  assert.equal(h.requests.length, 1);
  assert.equal(h.scheduled.size, timers, "cooldown must not schedule a retry");
  assert.match(h.elements.peerRequestState.textContent, /sample retained/);
  await h.deliver(result(2, "ok", 1000, 2_000_000), 999);
  assert.equal(h.requests.length, 1);
  await h.deliver(result(2, "ok", 1000, 2_000_000), 1);
  assert.equal(h.requests.length, 2);
  assert.equal(h.rate(), "↓ 8 Kbps");
});

test("admission uses monotonic response completion, not dispatch or wall time", async () => {
  const h = harness();
  let complete;
  h.respond(() => new Promise((resolve) => { complete = resolve; }));
  const pending = h.requestPeerStatus("manual", false);
  await Promise.resolve();
  h.advance(3000); // Long RPC: dispatch-based spacing would allow an immediate second request.
  complete(result(1));
  await pending;
  h.clock.wall += 60_000; // Clock adjustment cannot expire the cooldown.
  await h.deliver(result(2, "unavailable"), 0);
  assert.equal(h.requests.length, 1);
  await h.deliver(result(2, "ok", 1000, 2_000_000), 1000);
  assert.equal(h.requests.length, 2);
});

test("concurrent clicks remain single-flight", async () => {
  const h = harness();
  let complete;
  h.respond(() => new Promise((resolve) => { complete = resolve; }));
  const pending = h.requestPeerStatus("manual", false);
  await Promise.resolve();
  for (let i = 0; i < 10; i += 1) await h.requestPeerStatus("manual", false);
  assert.equal(h.requests.length, 1);
  complete(result(1));
  await pending;
});

test("HTTP failures retain observations but remain visible as failed attempts", async () => {
  const h = harness();
  await establish(h);
  for (const failure of [new Error("Management API request timed out"), { httpStatus: 409, body: { error: "already in progress" } }]) {
    await h.deliver(failure);
    assert.equal(h.rows().length, 1);
    assert.equal(h.rate(), "↓ 8 Kbps");
    assert.match(h.elements.overviewPeerState.textContent, /Refresh failed/);
  }
});

test("repeated render, sorting and two table surfaces do not mutate peer measurements", async () => {
  const h = harness();
  await establish(h);
  const snapshot = h.snapshot();
  const point = [...snapshot.points.values()][0];
  const other = { ...session, session_id: "456" };
  h.setStatus([session, other]);
  h.select(other); // Empty UI surface must not delete the first session's interval.
  h.select(session);
  for (const table of ["peer-path-status", "overview-peer-paths", "another-view"]) {
    for (let i = 0; i < 5; i += 1) {
      h.state.tableSorts.set(table, { column: "rate", direction: "desc" });
      const quality = h.pathQualities(snapshot.result.paths, snapshot.result, table)[0];
      assert.equal(quality.rate, 8000);
      h.renderSelectedPeerResult();
    }
  }
  assert.equal(h.snapshot(), snapshot);
  assert.equal([...snapshot.points.values()][0], point);
  assert.equal(h.state.nativeDeliveryCursors.size, 0, "peer render must not own a per-table cursor");
});

test("cached copies and duplicate samples cannot renew evidence age", async () => {
  const h = harness();
  await establish(h);
  const snapshot = h.snapshot();
  h.advance(5000);
  const age = h.peerResultResidenceMs(snapshot.result);
  h.setStatus([session], [structuredClone(snapshot.result)]);
  assert.equal(h.snapshot(), snapshot);
  assert.equal(h.peerResultResidenceMs(snapshot.result), age);
  await h.deliver(result(3, "ok", 1000, 2_000_000), 2000); // New RPC, unchanged source sample.
  assert.match(h.rate(), /^↓ ~/);
  assert.equal([...h.snapshot().points.values()][0].observedAt, [...snapshot.points.values()][0].observedAt);
});

test("retained rows age naturally and long intervals remain explicitly stale", async () => {
  const h = harness();
  await establish(h);
  await h.deliver(result(3, "unavailable"), 7000);
  assert.equal(h.rows().length, 1);
  assert.match(h.rate(), /^↓ ~/);
  await h.deliver(result(4, "ok", 3000, 20_000_000));
  const snapshot = h.snapshot();
  const quality = h.pathQualities(snapshot.result.paths, snapshot.result, "unused")[0];
  assert.equal(quality.stale, true);
  assert.equal(quality.sharePpm, null);
});

test("a newer cached result wins, an older result cannot roll back observations", async () => {
  const h = harness();
  await establish(h);
  h.advance(1100);
  const newer = result(4, "ok", 3000, 4_000_000);
  newer.received_unix_ms = h.clock.wall;
  h.setStatus([session], [newer]);
  assert.equal(h.snapshot().result.request_id, "4");
  h.setStatus([session], [result(3, "unavailable")]);
  assert.equal(h.snapshot().result.request_id, "4");
  assert.equal(h.peer().attempt.result.request_id, "4");
  assert.equal(h.rate(), "↓ 8 Kbps");
});

test("successful empty inventory and disabled are not transient failures", async () => {
  for (const code of ["ok", "disabled"]) {
    const h = harness();
    await establish(h);
    const empty = result(3, code);
    empty.paths = [];
    await h.deliver(empty);
    assert.equal(h.rows().length, 0);
    await h.deliver(result(4, "ok", 3000, 4_000_000));
    assert.equal(h.rate(), "↓ -", "authoritative removal/denial retires the old measurement");
  }
});

test("native epoch/direction reset and zero delta retain their distinct meanings", async () => {
  const h = harness();
  await establish(h);
  await h.deliver(result(3, "ok", 1000, 3_000_000));
  assert.equal(h.rate(), "↓ 0 bps");
  const reset = result(4, "ok", 2000, 4_000_000);
  reset.paths[0].native_delivery.epoch = "8";
  await h.deliver(reset);
  assert.equal(h.rate(), "↓ -");
  const direction = result(5, "ok", 3000, 5_000_000);
  direction.paths[0].native_delivery.epoch = "8";
  direction.paths[0].native_delivery.direction = "client_to_server";
  await h.deliver(direction);
  assert.equal(h.rate(), "↑ -");
});

test("peer state is bounded by live sessions and cleared on runtime restart", async () => {
  const h = harness();
  await establish(h);
  h.setStatus([]);
  assert.equal(h.state.peerDiagnostics.size, 0);
  assert.equal(h.rows().length, 0);
  h.setStatus();
  await h.deliver(result(3, "ok", 2000, 3_000_000));
  assert.equal(h.rate(), "↓ -");
  h.setStatus([session], [], 2);
  assert.equal(h.peer().snapshot, null);
  await h.deliver(result(1));
  assert.equal(h.snapshot().result.request_id, "1", "request sequence restarts with the runtime");
});

test("request IDs remain lossless and ordered across u64 wrap and clock reversal", async () => {
  const h = harness();
  const nearWrap = result(1);
  nearWrap.request_id = "18446744073709551615";
  await h.deliver(nearWrap);
  const wrapped = result(1, "ok", 1000, 2_000_000);
  wrapped.received_unix_ms = 1; // Endpoint clock moved backwards.
  await h.deliver(wrapped);
  assert.equal(h.rate(), "↓ 8 Kbps");
  h.setStatus([session], [nearWrap]);
  assert.equal(h.snapshot().result.request_id, "1");
});

test("session selection neither transfers cooldown nor loses another session's interval", async () => {
  const h = harness();
  await establish(h);
  const other = { ...session, session_id: "456" };
  h.setStatus([session, other]);
  h.select(other);
  await h.deliver(result(3, "ok", 0, 1_000_000, other), 0);
  assert.equal(h.requests.length, 3);
  assert.equal(h.rate(), "↓ -");
  h.select(session);
  assert.equal(h.rate(), "↓ 8 Kbps");
  await h.deliver(result(4, "unavailable"), 0);
  assert.equal(h.requests.length, 3);
});

test("old authentication completion cannot restore erased peer observations", async () => {
  const h = harness();
  await establish(h);
  h.advance(1100);
  let complete;
  h.respond(() => new Promise((resolve) => { complete = resolve; }));
  const pending = h.requestPeerStatus("manual", false);
  await Promise.resolve();
  h.clearToken();
  assert.equal(h.state.peerDiagnostics.size, 0);
  complete(result(3, "ok", 2000, 3_000_000));
  await pending;
  assert.equal(h.state.peerDiagnostics.size, 0);
  assert.equal(h.rows().length, 0);
});

test("mismatched response identity is rejected without poisoning another session", async () => {
  const h = harness();
  await establish(h);
  await h.deliver(result(3, "ok", 2000, 3_000_000, { ...session, session_id: "999" }));
  assert.equal(h.snapshot().result.request_id, "2");
  assert.match(h.elements.peerRequestState.textContent, /does not match/);
});

test("first accepted snapshot after unavailable uses the surviving native interval", async () => {
  const h = harness();
  await establish(h);
  await h.deliver(result(3, "unavailable"));
  await h.deliver(result(4, "ok", 3000, 4_000_000));
  assert.equal(h.rate(), "↓ 8 Kbps", "unavailable must not turn recovery into a new first sample");
});

test("cached residence uses endpoint-relative age rather than cross-host wall clocks", () => {
  const h = harness();
  const cached = result(1);
  cached.received_unix_ms = 15_000;
  h.state.status.generated_unix_ms = 20_000;
  h.state.status.diagnostics.peer_results = [cached];
  h.reconcilePeerDiagnostics(1);
  assert.equal(h.peerResultResidenceMs(h.snapshot().result), 5000);
  h.advance(600);
  assert.equal(h.peerResultResidenceMs(h.snapshot().result), 5600);
});

test("a complete OK inventory prunes removed paths without resetting surviving paths", async () => {
  const h = harness();
  const first = result(1);
  first.paths.push({ ...structuredClone(first.paths[0]), path_id: "1" });
  await h.deliver(first);
  const second = result(2, "ok", 1000, 2_000_000);
  second.paths.push({ ...structuredClone(second.paths[0]), path_id: "1" });
  await h.deliver(second);
  assert.equal(h.snapshot().points.size, 2);
  await h.deliver(result(3, "ok", 2000, 3_000_000));
  assert.equal(h.snapshot().points.size, 1);
  assert.equal(h.rate(), "↓ 8 Kbps");
});

test("outbound refresh skips a cooling session without skipping its sibling", async () => {
  const h = harness();
  const other = { ...session, service_index: 1, service_name: "second-peer", session_id: "456" };
  h.setStatus([session, other]);
  await h.deliver(result(1), 0);
  h.respond((_url, options) => {
    assert.equal(JSON.parse(options.body).session_id, other.session_id);
    return result(2, "ok", 0, 1_000_000, other);
  });
  await h.requestPeerStatuses("manual", false);
  assert.equal(h.requests.length, 2);
  assert.equal(h.overviewRows(0).length, 1);
  assert.equal(h.overviewRows(1).length, 1);
  await h.requestPeerStatuses("manual", false);
  assert.equal(h.requests.length, 2, "each outbound owns its cooldown");
  h.advance(1100);
  h.respond((_url, options) => JSON.parse(options.body).session_id === session.session_id
    ? result(3, "unavailable") : result(4, "ok", 1000, 2_000_000, other));
  await h.requestPeerStatuses("manual", false);
  assert.equal(h.overviewRows(0).length, 1, "failed outbound retains its own sample");
  assert.equal(h.overviewRows(1).length, 1);
  assert.equal(h.snapshot(other).result.request_id, "4");
  assert.equal(h.peer().attempt.result.code, "unavailable");
});

test("late replies cannot populate a retired or restarted session with the same key", async () => {
  for (const restart of [false, true]) {
    const h = harness();
    let complete;
    h.respond(() => new Promise(resolve => { complete = resolve; }));
    const pending = h.requestPeerStatus("manual", false);
    await Promise.resolve();
    const old = h.peer();
    if (restart) h.setStatus([session], [], 2);
    else { h.setStatus([]); h.setStatus(); }
    assert.notEqual(h.peer(), old);
    complete(result(10));
    await pending;
    assert.equal(h.snapshot(), null);
    assert.equal(h.peer().attempt, null);
    assert.equal(h.peer().nextRequestAt, 0);
    await h.deliver(result(1), 0);
    assert.equal(h.snapshot().result.request_id, "1");
  }
});

test("a late request failure cannot poison a replacement session", async () => {
  const h = harness();
  let fail;
  h.respond(() => new Promise((_resolve, reject) => { fail = reject; }));
  const pending = h.requestPeerStatus("manual", false);
  await Promise.resolve();
  h.setStatus([session], [], 2);
  fail(new Error("old runtime closed"));
  await pending;
  assert.equal(h.peer().error, null);
  assert.equal(h.peer().nextRequestAt, 0);
  assert.equal(h.rows().length, 0);
});
