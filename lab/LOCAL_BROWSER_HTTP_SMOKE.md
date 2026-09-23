# Local browser HTTP integrity smoke

This is a small functional check for a browser using the configured MPTUNNEL proxy to reach a temporary HTTP test origin. It is not a throughput benchmark and makes no Internet or Cloudflare claim.

The reusable fixture is at `lab/fixtures/http_integrity_browser_smoke.py`. It uses only Python's standard library, embeds its test page, and offers a no-socket `--self-check`. One page load performs exactly one document request, one 1 MiB download, four concurrent 4 KiB GETs, and one 256 KiB POST. It checks each GET byte against a fixed pattern. The page preconstructs a fixed-size 256 KiB upload body; the server reads it in 64 KiB chunks and returns its measured byte count and SHA-256. This does not test a browser streaming-request API. The server limits accepted routes to the fixed request set. No TLS, external assets, CAPTCHA, WebCrypto, retries, or performance threshold is required.

## Run against the reflection proxy

Use a fresh test-origin process on the server side of the reflection pair and configure Chromium to use the client-side mixed listener as a SOCKS5 proxy. This fixture uses plain HTTP, so the browser must speak SOCKS5; the mixed listener accepts SOCKS5 or HTTP CONNECT, not plain HTTP forward-proxy GET. In the existing topology these addresses are:

- Test origin: `10.238.46.20:18080`
- Browser proxy: `socks5://10.238.46.10:1080`
- Page: `http://10.238.46.20:18080/`

First run the fixture's no-network validation:

```sh
python3 -B lab/fixtures/http_integrity_browser_smoke.py --self-check
```

After the exact server/container and proxy configuration have been reviewed, start the HTTP fixture in a dedicated foreground terminal inside the reflection server container:

```sh
docker exec --interactive mptunnel-reflection-server-1   python3 -B /workspace/lab/fixtures/http_integrity_browser_smoke.py   --bind 10.238.46.20 --port 18080
```

Use a fresh isolated Chromium context with the proxy URL above. A minimal Playwright CLI config is:

```json
{
  "browser": {
    "browserName": "chromium",
    "isolated": true,
    "launchOptions": {
      "channel": "chrome",
      "headless": true,
      "proxy": {"server": "socks5://10.238.46.10:1080"},
      "args": ["--disable-quic"]
    },
    "contextOptions": {"viewport": {"width": 1440, "height": 1000}}
  },
  "outputDir": ".tmp/local-browser-smoke/run-01/output",
  "outputMode": "file"
}
```

Create a fresh isolated session and output directory, open the page once, then save its initial snapshot, result, final snapshot, screenshot, console output and `requests` log. The fixture records all seven HTTP requests, including the document request. The checks run on page load; do not reload or retry a failure. If the first snapshot already shows a completed result, no wait is needed. Otherwise, a Playwright CLI `run-code` wait can use this callback (the options object is the third `waitForFunction` argument):

```js
async (page) => {
  await page.waitForFunction(
    () => {
      const state = document.querySelector("#state")?.dataset.state;
      return state === "pass" || state === "fail";
    },
    null,
    { timeout: 15000 }
  );
}
```

A pass means only that these fixed local HTTP operations completed through the selected browser proxy and the response bytes matched the fixture's expectations. It does not establish speed, general server compatibility, large-file correctness, sustained behavior, or a performance advantage.

## Narrow local fixture authorization

The local HTTP origin is private, so the unchanged public-browser route intentionally rejects it. For this fixture only, add an `allow-restricted` rule before `default-browser` on both nodes. On the client, match inbound `local`, principal `anonymous`, destination `10.238.46.20/32`, TCP port `18080`, and the existing MPP outbound `remote`. On the server, match inbound `mpp`, principal `diagnostic`, the same destination and port, and the existing direct outbound. Preserve the existing `127.0.0.1/32` owned-target rule and the defaults after these narrow fixture rules; keep all other restricted destinations denied. The reflection configs use target resolution `as-is`, so no DNS route is needed for this numeric address. The fixture binds `10.238.46.20:18080` inside the server container.

This local-only rule pair exercises restricted-target authorization on both nodes for one fixture endpoint. It is not a general private-network setting and must not be copied into a public browser profile.

## Cleanup and evidence

Close the Playwright session and stop the fixture in its foreground terminal with Ctrl-C. If a coordinator launched the processes, use its exact owned-process cleanup instead and verify the browser, fixture, client, server and coordinator PIDs are absent. Confirm port 18080 is clear, the reflection containers are unchanged, and prior shaping is restored. Retain the single page result, screenshot, console/`requests` logs, fixture request log and cleanup record together. Never leave an extra fixture service running.

For a reproduction that uses a product binary, bind its source/artifact digest and current reflection container identities before starting endpoints. Keep fixture and browser artifacts in a fresh output directory. These request sizes and counts are fixed semantic checks; do not use them to compare product performance.
