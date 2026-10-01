"use strict";

// Run with: node --test tests/dashboard_peer_outbounds.test.cjs
// This exercises the production grouping helper without a browser or runtime.
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

function peerContext(status, liveResults = []) {
  const context = vm.createContext({ state: {
    status, lastReceivedAt: 1, peerDiagnostics: new Map()
  } });
  const names = ["asArray", "asObject", "finiteNumber", "peerSessionKey", "qualityGroupKey",
    "nativeDeliveryKey", "sampleNativeDelivery", "ingestPeerResult", "reconcilePeerDiagnostics",
    "newestCachedPeerResult", "peerOutboundMatches", "overviewPeerOutboundGroups"];
  vm.runInContext(names.map(extractFunction).join("\n"), context);
  context.reconcilePeerDiagnostics(undefined);
  for (const { result } of liveResults) context.ingestPeerResult(result, 1, 0);
  return context;
}

function groupedOutbounds(status, liveResults = []) {
  return JSON.parse(JSON.stringify(peerContext(status, liveResults).overviewPeerOutboundGroups()));
}

function selectedPeerResult(status, session, live) {
  return JSON.parse(JSON.stringify(peerContext(status, live ? [{ result: live }] : [])
    .newestCachedPeerResult(session)));
}

test("peer paths group by MPP name and MPP context index across interleaved native outbounds", () => {
  const north = "mpp-north";
  const south = "mpp-south";
  const status = {
    outbounds: [
      { name: north, protocol: "mpp" },
      { name: "local-direct", protocol: "direct" },
      { name: south, protocol: "mpp" },
      { name: "mpp-offline", protocol: "mpp" }
    ],
    diagnostics: {
      peer_sessions: [
        { service: "mpp_outbound", service_index: 0, service_name: north, session_id: "north-a", carrier_count: 2 },
        { service: "mpp_outbound", service_index: 0, service_name: north, session_id: "north-b", carrier_count: 1 },
        { service: "mpp_outbound", service_index: 1, session_id: "south-a", carrier_count: 1 },
        { service: "mpp_inbound", service_index: 0, service_name: "server-inbound", session_id: "inbound-a", carrier_count: 1 }
      ],
      peer_results: [
        { service: "mpp_outbound", service_index: 0, service_name: north, session_id: "north-a", received_unix_ms: 10, code: "ok", request_id: "1", paths: [] },
        { service: "mpp_outbound", service_index: 0, service_name: north, session_id: "north-b", received_unix_ms: 20, code: "ok", request_id: "2", paths: [] },
        { service: "mpp_outbound", service_index: 1, session_id: "south-a", received_unix_ms: 30, code: "ok", request_id: "3", paths: [] },
        { service: "mpp_inbound", service_index: 0, service_name: "server-inbound", session_id: "inbound-a", received_unix_ms: 40, code: "ok", request_id: "4", paths: [] }
      ]
    }
  };

  const groups = groupedOutbounds(status);
  assert.deepEqual(groups.map(group => group.name), [north, south, "mpp-offline"]);
  assert.deepEqual(groups.map(group => group.sessions.map(session => session.session_id)), [
    ["north-a", "north-b"], ["south-a"], []
  ]);
  assert.deepEqual(groups.map(group => group.peers.map(peer => peer.session.session_id)), [
    ["north-a", "north-b"], ["south-a"], []
  ]);
  assert.deepEqual(groups.map(group => group.peers.filter(peer => peer.result).length), [2, 1, 0]);
  assert.equal(groups[2].sessions.length, 0, "configured offline MPP outbound keeps an empty table group");
});

test("inbound-only peers do not create outbound tables", () => {
  const groups = groupedOutbounds({
    outbounds: [{ name: "local-direct", protocol: "direct" }],
    diagnostics: {
      peer_sessions: [{ service: "mpp_inbound", service_index: 0, service_name: "server-inbound", session_id: "peer" }],
      peer_results: [{ service: "mpp_inbound", service_index: 0, service_name: "server-inbound", session_id: "peer", paths: [] }]
    }
  });
  assert.deepEqual(groups, []);
});

test("empty or stale session results cannot invent a peer row, while a live exact-key result wins", () => {
  const session = { service: "mpp_outbound", service_index: 0, service_name: "north", session_id: "101" };
  const stale = { ...session, session_id: "removed", received_unix_ms: 90, paths: [{ path: "stale" }] };
  const fresh = { ...session, received_unix_ms: 100, request_id: "1", code: "ok", paths: [{ path: "fresh" }] };
  const status = {
    outbounds: [{ name: "north", protocol: "mpp" }],
    diagnostics: { peer_sessions: [session], peer_results: [stale] }
  };
  const [withoutLive] = groupedOutbounds(status);
  assert.equal(withoutLive.peers.length, 1);
  assert.equal(withoutLive.peers[0].result, null, "removed cached sessions are ignored");

  const [withLive] = groupedOutbounds(status, [{ session, result: fresh }]);
  assert.equal(withLive.peers.length, 1);
  assert.equal(withLive.peers[0].result.request_id, "1", "fresh queried result overrides status cache");

  const newerCacheStatus = structuredClone(status);
  newerCacheStatus.diagnostics.peer_results = [{
    ...session,
    received_unix_ms: 120,
    request_id: "2", code: "ok",
    paths: [{ path: "newer-cache" }]
  }];
  const [withNewerCache] = groupedOutbounds(newerCacheStatus, [{ session, result: fresh }]);
  assert.equal(withNewerCache.peers[0].result.request_id, "2", "newer same-key status result wins over old live cache");

  const [withWrongIdentity] = groupedOutbounds(status, [{
    session,
    result: { ...fresh, service_name: "other-outbound" }
  }]);
  assert.equal(withWrongIdentity.peers[0].result, null, "same session ID on a different outbound cannot leak across groups");

  const selected = selectedPeerResult(newerCacheStatus, session, fresh);
  assert.equal(selected.request_id, "2", "selected diagnostics view also uses the newest exact-key result");
});
