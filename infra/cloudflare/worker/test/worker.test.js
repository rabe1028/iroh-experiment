import assert from "node:assert/strict";
import test from "node:test";
import worker from "../src/index.js";

function request(path = "/observe", { method = "GET", cf = {}, headers = {} } = {}) {
  const req = new Request(`https://observe.example.com${path}`, {
    method, headers: { "cf-connecting-ip": "203.0.113.10", ...headers },
  });
  Object.defineProperty(req, "cf", { value: cf });
  return req;
}

test("H3 metadata matches the Rust client header contract; port stays absent", async () => {
  for (const httpProtocol of ["HTTP/3", "HTTP/3.0"]) {
    const response = await worker.fetch(request("/observe", {
      cf: { httpProtocol, clientQuicRtt: 12.5, colo: "NRT" },
    }));
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("x-observed-ip"), "203.0.113.10");
    assert.equal(response.headers.get("x-observed-port"), null);
    assert.equal(response.headers.get("x-observed-rtt-ms"), "12.5");
    assert.equal(response.headers.get("x-observed-colo"), "NRT");
    assert.equal(response.headers.get("x-observed-is-tcp"), "false");
    assert.equal(response.headers.get("x-observed-http-version"), httpProtocol);
    assert.equal(response.headers.get("cache-control"), "no-store");
    assert.equal(response.headers.get("cloudflare-cdn-cache-control"), "no-store");
    assert.equal((await response.json()).observed_port, null);
  }
});

test("IPv6 edge addresses are preserved", async () => {
  const response = await worker.fetch(request("/observe", {
    headers: { "cf-connecting-ip": "2001:db8::1" },
  }));
  assert.equal(response.headers.get("x-observed-ip"), "2001:db8::1");
});

test("query/forwarded/observation headers cannot supply a fake observation", async () => {
  const response = await worker.fetch(request("/observe?ip=192.0.2.99&port=9999", {
    headers: { "x-forwarded-for": "192.0.2.99", "x-observed-ip": "192.0.2.99",
      "x-observed-port": "9999", "x-observed-rtt-ms": "99" },
  }));
  assert.equal(response.headers.get("x-observed-ip"), "203.0.113.10");
  assert.equal(response.headers.get("x-observed-port"), null);
  assert.equal(response.headers.get("x-observed-rtt-ms"), null);
});

test("TCP is identified and never reports QUIC RTT even if a stale value is present", async () => {
  const response = await worker.fetch(request("/observe", {
    cf: { httpProtocol: "HTTP/2", clientQuicRtt: 0, clientTcpRtt: 15 },
  }));
  assert.equal(response.headers.get("x-observed-is-tcp"), "true");
  assert.equal(response.headers.get("x-observed-rtt-ms"), null);
});

test("missing and invalid RTT are missing rather than coerced to zero", async () => {
  for (const clientQuicRtt of [undefined, null, "10", -1, NaN, Infinity]) {
    const response = await worker.fetch(request("/observe", {
      cf: { httpProtocol: "HTTP/3", clientQuicRtt },
    }));
    assert.equal(response.headers.get("x-observed-rtt-ms"), null);
    assert.equal((await response.json()).rtt_ms, null);
  }
  const zero = await worker.fetch(request("/observe", {
    cf: { httpProtocol: "HTTP/3", clientQuicRtt: 0 },
  }));
  assert.equal(zero.headers.get("x-observed-rtt-ms"), "0");
});

test("no edge IP fails rather than returning a successful invented observation", async () => {
  const response = await worker.fetch(new Request("https://observe.example.com/observe"));
  assert.equal(response.status, 503);
});

test("only the exact observation path and GET/HEAD are accepted", async () => {
  for (const path of ["/", "/observe/", "/observe-other"]) {
    assert.equal((await worker.fetch(request(path))).status, 404);
  }
  const post = await worker.fetch(request("/observe", { method: "POST" }));
  assert.equal(post.status, 405);
  assert.equal(post.headers.get("allow"), "GET, HEAD");
  const head = await worker.fetch(request("/observe", { method: "HEAD" }));
  assert.equal(head.status, 200);
  assert.equal(await head.text(), "");
});
