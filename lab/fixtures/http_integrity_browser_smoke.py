#!/usr/bin/env python3
"""Finite local HTTP integrity fixture for the reflection browser smoke."""
from __future__ import annotations

import argparse
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

DOWNLOAD_BYTES = 1_048_576
SMALL_BYTES = 4_096
UPLOAD_BYTES = 262_144
DOWNLOAD_SHA256 = "631b84027d6b9e52b539c4e8373622d23032dfadc64d60af87339c9037e4f769"
SMALL_SHA256 = "0d356260eaf09e3b3dc81a65b2ad2399aa7c4921c0274bd2cbb54c2a21c46e3b"
UPLOAD_SHA256 = "d3780f8ebeb903c651ccd09e745f61b40b15b76e2e7a95d61bf9a3986a7f34bc"
REQUEST_LIMIT = 7
REQUEST_COUNTS: dict[str, int] = {}
REQUEST_LOCK = threading.Lock()


def download_byte(index: int) -> int:
    return index % 251


def small_byte(index: int) -> int:
    return (index * 7 + 3) % 251


def upload_byte(index: int) -> int:
    return (index * 13 + 5) % 251


def body(size: int, pattern) -> bytes:
    return bytes(pattern(index) for index in range(size))


DOWNLOAD_BODY = body(DOWNLOAD_BYTES, download_byte)
SMALL_BODY = body(SMALL_BYTES, small_byte)

PAGE = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="data:,">
<title>MPTunnel local browser smoke</title>
<style>body{{font:16px system-ui,sans-serif;margin:2rem;max-width:60rem}}pre{{white-space:pre-wrap;background:#f3f4f6;padding:1rem}}[data-state=pass]{{color:#075b2a}}[data-state=fail]{{color:#9b1c1c}}</style>
<h1>Local tunnel browser smoke</h1>
<p>One bounded download, four concurrent small GETs, and one fixed-size upload.</p>
<p id="state" data-state="running">Running checks…</p>
<pre id="result">{{}}</pre>
<script>
(() => {{
  const expected = {{
    download: {{size: {DOWNLOAD_BYTES}, sha256: "{DOWNLOAD_SHA256}"}},
    small: {{size: {SMALL_BYTES}, sha256: "{SMALL_SHA256}"}},
    upload: {{size: {UPLOAD_BYTES}, sha256: "{UPLOAD_SHA256}"}}
  }};
  const state = document.querySelector("#state");
  const result = document.querySelector("#result");
  function assert(condition, message) {{ if (!condition) throw new Error(message); }}
  async function getAndCheck(path, specification, byteAt) {{
    const response = await fetch(path, {{cache: "no-store", signal: AbortSignal.timeout(10000)}});
    assert(response.ok, `${{path}} HTTP ${{response.status}}`);
    const announced = Number(response.headers.get("content-length"));
    const announcedHash = response.headers.get("x-fixture-sha256");
    const bytes = new Uint8Array(await response.arrayBuffer());
    assert(announced === specification.size, `${{path}} Content-Length ${{announced}}`);
    assert(bytes.length === specification.size, `${{path}} received ${{bytes.length}} bytes`);
    assert(announcedHash === specification.sha256, `${{path}} server digest mismatch`);
    for (let i = 0; i < bytes.length; i++) {{
      if (bytes[i] !== byteAt(i)) throw new Error(`${{path}} byte mismatch at ${{i}}`);
    }}
    return {{path, status: response.status, bytes: bytes.length,
             expected_sha256_header: announcedHash, pattern_ok: true}};
  }}
  async function uploadAndCheck() {{
    const bytes = new Uint8Array(expected.upload.size);
    for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 13 + 5) % 251;
    const response = await fetch("/upload", {{
      method: "POST", cache: "no-store", signal: AbortSignal.timeout(10000),
      headers: {{"content-type": "application/octet-stream"}}, body: bytes
    }});
    const received = await response.json();
    assert(response.ok, `upload HTTP ${{response.status}}`);
    assert(received.bytes === expected.upload.size, `upload byte count ${{received.bytes}}`);
    assert(received.sha256 === expected.upload.sha256, "upload SHA-256 mismatch");
    assert(received.pattern_ok === true, "upload payload pattern mismatch");
    return {{path: "/upload", status: response.status, bytes: received.bytes,
             sha256: received.sha256, pattern_ok: received.pattern_ok}};
  }}
  (async () => {{
    try {{
      const [download, small, upload] = await Promise.all([
        getAndCheck("/download.bin", expected.download, i => i % 251),
        Promise.all(Array.from({{length: 4}}, () =>
          getAndCheck("/small.bin", expected.small, i => (i * 7 + 3) % 251))),
        uploadAndCheck()
      ]);
      const report = {{state: "pass", requests: 7, download, concurrent_small_gets: small, upload}};
      state.dataset.state = "pass";
      state.textContent = "PASS — all fixed-size HTTP integrity checks completed";
      result.textContent = JSON.stringify(report, null, 2);
      console.log(JSON.stringify(report));
    }} catch (error) {{
      const report = {{state: "fail", error: String(error)}};
      state.dataset.state = "fail";
      state.textContent = "FAIL — see retained error and browser logs";
      result.textContent = JSON.stringify(report, null, 2);
      console.error(JSON.stringify(report));
    }}
  }})();
}})();
</script>
"""


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MPTunnelLocalFixture/1"
    sys_version = ""

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(10.0)

    def _send(self, status: int, content_type: str, payload: bytes, extra=None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)
        self.close_connection = True

    def _admit(self) -> bool:
        key = f"{self.command} {self.path}"
        per_path_limits = {
            "GET /": 1,
            "GET /download.bin": 1,
            "GET /small.bin": 4,
            "POST /upload": 1,
        }
        with REQUEST_LOCK:
            total = sum(REQUEST_COUNTS.values())
            seen = REQUEST_COUNTS.get(key, 0)
            if total >= REQUEST_LIMIT or seen >= per_path_limits.get(key, 0):
                return False
            REQUEST_COUNTS[key] = seen + 1
            return True

    def do_HEAD(self) -> None:
        if not self._admit():
            self._send(429, "text/plain; charset=utf-8", b"fixed request budget exhausted\n")
            return
        routes = {
            "/": ("text/html; charset=utf-8", PAGE.encode("utf-8"), {}),
            "/download.bin": ("application/octet-stream", DOWNLOAD_BODY, {"X-Fixture-SHA256": DOWNLOAD_SHA256}),
            "/small.bin": ("application/octet-stream", SMALL_BODY, {"X-Fixture-SHA256": SMALL_SHA256}),
        }
        route = routes.get(self.path)
        if route is None:
            self._send(404, "text/plain; charset=utf-8", b"not found\n")
        else:
            self._send(200, *route)

    def do_GET(self) -> None:
        self.do_HEAD()

    def do_POST(self) -> None:
        if not self._admit():
            self._send(429, "text/plain; charset=utf-8", b"fixed request budget exhausted\n")
            return
        if self.path != "/upload":
            self._send(404, "text/plain; charset=utf-8", b"not found\n")
            return
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._send(411, "text/plain; charset=utf-8", b"Content-Length required\n")
            return
        if content_length != UPLOAD_BYTES:
            self._send(413, "text/plain; charset=utf-8", b"fixed upload size required\n")
            return

        digest = hashlib.sha256()
        received = 0
        pattern_ok = True
        while received < UPLOAD_BYTES:
            chunk = self.rfile.read(min(64 * 1024, UPLOAD_BYTES - received))
            if not chunk:
                self._send(400, "text/plain; charset=utf-8", b"truncated upload\n")
                return
            digest.update(chunk)
            for offset, value in enumerate(chunk):
                if value != upload_byte(received + offset):
                    pattern_ok = False
            received += len(chunk)
        payload_hash = digest.hexdigest()
        status = 200 if pattern_ok and payload_hash == UPLOAD_SHA256 else 422
        payload = json.dumps({
            "bytes": received,
            "sha256": payload_hash,
            "pattern_ok": pattern_ok,
        }, sort_keys=True, separators=(",", ":")).encode("ascii")
        self._send(status, "application/json; charset=utf-8", payload)

    def log_message(self, fmt, *args) -> None:
        print("http-fixture " + (fmt % args), flush=True)


def self_check() -> dict:
    checks = {
        "download": (DOWNLOAD_BODY, DOWNLOAD_BYTES, DOWNLOAD_SHA256),
        "small": (SMALL_BODY, SMALL_BYTES, SMALL_SHA256),
        "upload": (body(UPLOAD_BYTES, upload_byte), UPLOAD_BYTES, UPLOAD_SHA256),
    }
    for name, (payload, expected_size, expected_hash) in checks.items():
        assert len(payload) == expected_size, (name, len(payload), expected_size)
        assert hashlib.sha256(payload).hexdigest() == expected_hash, name
    assert sum((1, 1, 4, 1)) == REQUEST_LIMIT
    assert '<link rel="icon" href="data:,">' in PAGE
    assert "crypto.subtle" not in PAGE
    return {"status": "ok", "fixed_sizes_bytes": {name: size for name, (_, size, _) in checks.items()},
            "sha256": {name: digest for name, (_, _, digest) in checks.items()},
            "page_request_count_including_document": REQUEST_LIMIT,
            "fixed_request_budget": {"GET /": 1, "GET /download.bin": 1,
                                     "GET /small.bin": 4, "POST /upload": 1}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="10.238.46.20")
    parser.add_argument("--port", default=18080, type=int)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        print(json.dumps(self_check(), sort_keys=True))
        return
    server = ThreadingHTTPServer((args.bind, args.port), FixtureHandler)
    server.daemon_threads = True
    print(json.dumps({"ready": True, "bind": args.bind, "port": args.port,
                      "self_check": self_check()}), flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
