"use strict";

// Run with: node --test tests/dashboard_quality.test.cjs
// The harness executes the exact production helpers from dashboard.js.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const { test } = require("node:test");

const dashboard = fs.readFileSync("assets/dashboard/dashboard.js", "utf8");

function extractFunction(name) {
  const marker = `function ${name}(`;
  const start = dashboard.indexOf(marker);
  assert.notEqual(start, -1, `missing ${marker}`);
  const open = dashboard.indexOf("{", start);
  let depth = 0;
  let quote = null;
  let lineComment = false;
  let blockComment = false;
  for (let index = open; index < dashboard.length; index += 1) {
    const character = dashboard[index];
    const next = dashboard[index + 1];
    if (lineComment) {
      if (character === "\n") lineComment = false;
      continue;
    }
    if (blockComment) {
      if (character === "*" && next === "/") {
        blockComment = false;
        index += 1;
      }
      continue;
    }
    if (quote !== null) {
      if (character === "\\") index += 1;
      else if (character === quote) quote = null;
      continue;
    }
    if (character === "/" && next === "/") {
      lineComment = true;
      index += 1;
      continue;
    }
    if (character === "/" && next === "*") {
      blockComment = true;
      index += 1;
      continue;
    }
    if (character === "\"" || character === "'" || character === "`") {
      quote = character;
      continue;
    }
    if (character === "{") depth += 1;
    if (character === "}") {
      depth -= 1;
      if (depth === 0) return dashboard.slice(start, index + 1);
    }
  }
  throw new Error(`unterminated function ${name}`);
}

function makeHarness() {
  const clock = { ms: 0 };
  const state = {
    nativeDeliveryCursors: new Map(),
    refreshIntervalMs: 5_000,
    peerResult: null,
    peerResultReceivedAt: 0,
    lastReceivedAt: 0,
    status: {}
  };
  const context = vm.createContext({
    state,
    MIN_STALE_AFTER_MS: 6_500,
    QUALITY_PAYLOAD_BYTES: 64 * 1024,
    Date: { now: () => clock.ms }
  });
  const names = [
    "asObject",
    "finiteNumber",
    "metricAvailable",
    "statusResidenceMs",
    "effectivePathMetricAgeMs",
    "peerResultResidenceMs",
    "effectivePeerMetricAgeMs",
    "metricIsStale",
    "deliveryRateIsStale",
    "qualityGroupKey",
    "pathQualities",
    "staleAfterMs"
  ];
  vm.runInContext(names.map(extractFunction).join("\n"), context);
  return {
    clock,
    state,
    run(paths, result, tableKey = "quality-test", receivedAtMs = clock.ms) {
      if (result) {
        state.peerResult = result;
        state.peerResultReceivedAt = receivedAtMs;
      } else {
        state.lastReceivedAt = clock.ms;
      }
      return Array.from(context.pathQualities(paths, result, tableKey), (quality) => ({
        rate: quality.rate,
        rateFresh: quality.rateFresh,
        stale: quality.stale,
        sharePpm: quality.sharePpm,
        peers: quality.peers,
        coverageFresh: quality.coverageFresh,
        coverageTotal: quality.coverageTotal
      }));
    }
  };
}

function nativePath(id, group = "g") {
  return {
    service: "outbound",
    service_index: 0,
    session_id: group,
    underlay: "tcp",
    path_id: String(id),
    path_instance_id: String(id + 100),
    direction: "server_to_client",
    native_delivery: {
      epoch: 7,
      direction: "server_to_client",
      acked_bytes: "0",
      sampled_at_us: "1000000"
    }
  };
}

function setSample(path, bytes, sampledAtUs, epoch = 7) {
  path.native_delivery.acked_bytes = String(bytes);
  path.native_delivery.sampled_at_us = String(sampledAtUs);
  path.native_delivery.epoch = epoch;
}

function setDirection(path, direction) {
  path.direction = direction;
  if (path.native_delivery) path.native_delivery.direction = direction;
}

test("a stale member is excluded while a fresh sibling gets a subset share", () => {
  const harness = makeHarness();
  const stale = nativePath(1);
  const fresh = nativePath(2);

  harness.run([stale, fresh], null);
  harness.clock.ms = 1_000;
  setSample(stale, 99_000_000, 2_000_000);
  setSample(fresh, 1_000_000, 2_000_000);
  const established = harness.run([stale, fresh], null);
  assert.equal(established[0].rate, 792_000_000);
  assert.equal(established[1].rate, 8_000_000);
  assert.equal(established[0].sharePpm + established[1].sharePpm, 1_000_000);

  harness.clock.ms = 12_000;
  setSample(fresh, 2_000_000, 3_000_000);
  const mixed = harness.run([stale, fresh], null);
  assert.equal(mixed[0].rate, 792_000_000, "historical rate remains visible");
  assert.equal(mixed[0].stale, true);
  assert.equal(mixed[0].sharePpm, null);
  assert.equal(mixed[1].rateFresh, true);
  assert.equal(mixed[1].sharePpm, 1_000_000);
  assert.equal(mixed[1].coverageFresh, 1);
  assert.equal(mixed[1].coverageTotal, 2);

  harness.clock.ms = 13_000;
  setSample(stale, 100_000_000, 3_000_000);
  setSample(fresh, 3_000_000, 4_000_000);
  const recovered = harness.run([stale, fresh], null);
  assert.equal(recovered[0].stale, false);
  assert.equal(recovered[0].rateFresh, true);
  assert.equal(recovered[0].coverageFresh, 2);
  assert.equal(recovered[0].sharePpm, 500_000);
  assert.equal(recovered[1].sharePpm, 500_000);
});

test("all stale and all missing observations have no current share", () => {
  const harness = makeHarness();
  const first = nativePath(1);
  const second = nativePath(2);
  harness.run([first, second], null);
  harness.clock.ms = 1_000;
  setSample(first, 1_000, 2_000_000);
  setSample(second, 2_000, 2_000_000);
  harness.run([first, second], null);

  harness.clock.ms = 12_000;
  const stale = harness.run([first, second], null);
  assert.deepEqual(stale.map((quality) => quality.sharePpm), [null, null]);
  assert.deepEqual(stale.map((quality) => quality.rateFresh), [false, false]);
  assert.deepEqual(stale.map((quality) => [quality.coverageFresh, quality.coverageTotal]), [
    [0, 2], [0, 2]
  ]);
  assert.ok(stale.every((quality) => quality.rate !== null && quality.stale));

  const missingHarness = makeHarness();
  const missing = [
    { ...nativePath(3), native_delivery: null },
    { ...nativePath(4), native_delivery: null }
  ];
  const unavailable = missingHarness.run(missing, null);
  assert.deepEqual(unavailable.map((quality) => quality.sharePpm), [null, null]);
  assert.deepEqual(unavailable.map((quality) => [quality.coverageFresh, quality.coverageTotal]), [
    [0, 2], [0, 2]
  ]);
});

test("fresh zero progress is counted, epoch reset is not a fabricated rate", () => {
  const harness = makeHarness();
  const idle = nativePath(1);
  const active = nativePath(2);
  harness.run([idle, active], null);
  harness.clock.ms = 1_000;
  setSample(idle, 100, 2_000_000);
  setSample(active, 1_000, 2_000_000);
  harness.run([idle, active], null);

  harness.clock.ms = 2_000;
  setSample(idle, 100, 3_000_000, 8); // New epoch: establish baseline only.
  setSample(active, 2_000, 3_000_000);
  const reset = harness.run([idle, active], null);
  assert.equal(reset[0].rate, null);
  assert.equal(reset[0].rateFresh, false);
  assert.equal(reset[0].sharePpm, null);
  assert.equal(reset[1].coverageFresh, 1);
  assert.equal(reset[1].coverageTotal, 2);
  assert.equal(reset[1].sharePpm, 1_000_000);

  harness.clock.ms = 3_000;
  setSample(idle, 100, 4_000_000, 8); // Fresh zero-byte interval.
  setSample(active, 3_000, 4_000_000);
  const idleFresh = harness.run([idle, active], null);
  assert.equal(idleFresh[0].rate, 0);
  assert.equal(idleFresh[0].rateFresh, true);
  assert.equal(idleFresh[0].sharePpm, 0);
  assert.equal(idleFresh[0].coverageFresh, 2);
  assert.equal(idleFresh[0].coverageTotal, 2);

  const zeroOnlyHarness = makeHarness();
  const zeroOnly = nativePath(3);
  zeroOnlyHarness.run([zeroOnly], null);
  zeroOnlyHarness.clock.ms = 1_000;
  setSample(zeroOnly, 0, 2_000_000);
  const noCapacity = zeroOnlyHarness.run([zeroOnly], null);
  assert.equal(noCapacity[0].rate, 0);
  assert.equal(noCapacity[0].rateFresh, true);
  assert.equal(noCapacity[0].sharePpm, null, "zero total rate has no defined share");
  assert.deepEqual([noCapacity[0].coverageFresh, noCapacity[0].coverageTotal], [1, 1]);
});

test("session and direction boundaries isolate stale paths and separate counter resets", () => {
  const harness = makeHarness();
  const stale = nativePath(1, "one-session");
  const sibling = nativePath(2, "one-session");
  const otherDirection = nativePath(3, "one-session");
  const otherSession = nativePath(4, "other-session");
  setDirection(otherDirection, "client_to_server");
  harness.run([stale, sibling, otherDirection, otherSession], null);

  harness.clock.ms = 1_000;
  setSample(stale, 99_000_000, 2_000_000);
  setSample(sibling, 1_000_000, 2_000_000);
  setSample(otherDirection, 2_000_000, 2_000_000);
  setSample(otherSession, 3_000_000, 2_000_000);
  harness.run([stale, sibling, otherDirection, otherSession], null);

  harness.clock.ms = 12_000;
  setSample(sibling, 2_000_000, 3_000_000);
  setSample(otherDirection, 3_000_000, 3_000_000);
  setSample(otherSession, 4_000_000, 3_000_000);
  const quality = harness.run([stale, sibling, otherDirection, otherSession], null);
  assert.equal(quality[0].sharePpm, null);
  assert.equal(quality[0].stale, true);
  assert.deepEqual([quality[1].coverageFresh, quality[1].coverageTotal], [1, 2]);
  assert.equal(quality[1].sharePpm, 1_000_000);
  assert.deepEqual([quality[2].coverageFresh, quality[2].coverageTotal], [1, 1]);
  assert.equal(quality[2].sharePpm, 1_000_000);
  assert.deepEqual([quality[3].coverageFresh, quality[3].coverageTotal], [1, 1]);
  assert.equal(quality[3].sharePpm, 1_000_000);

  const resetHarness = makeHarness();
  const reset = nativePath(5);
  resetHarness.run([reset], null);
  resetHarness.clock.ms = 1_000;
  setSample(reset, 1_000, 2_000_000);
  resetHarness.run([reset], null);
  resetHarness.clock.ms = 2_000;
  setSample(reset, 900, 3_000_000); // Decreasing in-epoch counter invalidates the interval.
  const decreased = resetHarness.run([reset], null);
  assert.equal(decreased[0].rate, null);
  assert.equal(decreased[0].rateFresh, false);
  assert.equal(decreased[0].sharePpm, null);
});

test("an overlong native sample interval is stale, not current capacity", () => {
  const harness = makeHarness();
  const path = nativePath(1);
  harness.run([path], null);
  harness.clock.ms = 1_000;
  setSample(path, 1_000, 2_000_000);
  harness.run([path], null);

  harness.clock.ms = 12_000;
  setSample(path, 2_000, 20_000_000); // 18 s observation interval exceeds the 10 s age bound.
  const aged = harness.run([path], null);
  assert.ok(aged[0].rate > 0, "historical rate remains diagnostic");
  assert.equal(aged[0].stale, true);
  assert.equal(aged[0].rateFresh, false);
  assert.equal(aged[0].sharePpm, null);
  assert.deepEqual([aged[0].coverageFresh, aged[0].coverageTotal], [0, 1]);
});

test("approximate fallback uses local and peer metric age plus horizon", () => {
  const localHarness = makeHarness();
  const expired = {
    ...nativePath(1), native_delivery: null,
    delivery_rate_bps: "200", delivery_rate_approximate: true,
    last_delivery_age_ms: 11_000, freshness_horizon_ms: 10_000
  };
  const fresh = {
    ...nativePath(2), native_delivery: null,
    delivery_rate_bps: "100", delivery_rate_approximate: true,
    last_delivery_age_ms: 500, freshness_horizon_ms: 10_000
  };
  const unaged = {
    ...nativePath(3), native_delivery: null,
    delivery_rate_bps: "50", delivery_rate_approximate: true,
    last_delivery_age_ms: 0, freshness_horizon_ms: null
  };
  const local = localHarness.run([expired, fresh, unaged], null);
  assert.equal(local[0].rate, 200);
  assert.equal(local[0].stale, true);
  assert.equal(local[0].sharePpm, null);
  assert.equal(local[1].rateFresh, true);
  assert.equal(local[1].sharePpm, 1_000_000);
  assert.equal(local[2].rate, 50);
  assert.equal(local[2].stale, true);
  assert.equal(local[2].sharePpm, null);
  assert.deepEqual([local[1].coverageFresh, local[1].coverageTotal], [1, 3]);

  const peerHarness = makeHarness();
  const result = {
    service: "inbound", service_index: 0, session_id: "peer", received_unix_ms: "1000"
  };
  const peer = {
    ...nativePath(3), native_delivery: null,
    metric_age_us: 5_000_000, freshness_horizon_ms: 10_000,
    delivery_rate_bps: "300", delivery_rate_approximate: true
  };
  peerHarness.state.status.generated_unix_ms = "6000";
  peerHarness.clock.ms = 6_000;
  peerHarness.run([peer], result, "quality-test", 0); // 5 s metric age + 5 s residence.
  const aged = peerHarness.run([peer], result, "quality-test", 0);
  assert.equal(aged[0].rate, 300);
  assert.equal(aged[0].stale, true);
  assert.equal(aged[0].sharePpm, null);
  assert.deepEqual([aged[0].coverageFresh, aged[0].coverageTotal], [0, 1]);
});

test("quality tooltip names fresh-subset semantics and coverage", () => {
  assert.match(dashboard, /Share among fresh observed paths/);
  assert.match(dashboard, /Fresh rate coverage:/);
  assert.match(dashboard, /stale or missing observations are excluded, not treated as zero/);
  assert.match(dashboard, /A \* on serialization marks an approximate fallback rate; ~ marks a stale input rate/);
});

test("approximate fallback marks the derived serialization estimate", () => {
  const context = vm.createContext({});
  const names = [
    "finiteNumber",
    "metricAvailable",
    "formatOptionalMetric",
    "formatDiagnosticMetric",
    "formatDuration",
    "formatRtt",
    "formatQualitySerialization"
  ];
  vm.runInContext(names.map(extractFunction).join("\n"), context);

  assert.equal(context.formatQualitySerialization({
    etaMs: 1_500, stale: false, approximate: true
  }), "1.5 s*");
  assert.equal(context.formatQualitySerialization({
    etaMs: 1_500, stale: false, approximate: false
  }), "1.5 s");
  assert.equal(context.formatQualitySerialization({
    etaMs: 1_500, stale: true, approximate: true
  }), "~1.5 s*");
  assert.equal(context.formatQualitySerialization({
    etaMs: null, stale: false, approximate: true
  }), "-");
});
