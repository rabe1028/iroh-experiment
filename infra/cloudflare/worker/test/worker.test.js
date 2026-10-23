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

test("unknown routes and non-read methods are rejected; observation HEAD has no body", async () => {
  for (const path of ["/observe/", "/observe-other", "/other", "/test/"]) {
    assert.equal((await worker.fetch(request(path))).status, 404);
  }
  const post = await worker.fetch(request("/observe", { method: "POST" }));
  assert.equal(post.status, 405);
  assert.equal(post.headers.get("allow"), "GET, HEAD");
  const head = await worker.fetch(request("/observe", { method: "HEAD" }));
  assert.equal(head.status, 200);
  assert.equal(await head.text(), "");
});

test("mobile page and its assets are served without requiring observation headers", async () => {
  const paths = [];
  const env = { ASSETS: { async fetch(req) {
    paths.push(new URL(req.url).pathname);
    return new Response(req.method === "HEAD" ? null : "asset", {
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  } } };
  for (const path of ["/", "/test", "/browser.js", "/browser.css", "/peer", "/peer.js", "/peer-proof.js"]) {
    const response = await worker.fetch(new Request("https://observe.example.com" + path), env);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "asset");
    assert.equal(response.headers.get("cache-control"), "no-store");
    assert.equal(response.headers.get("x-content-type-options"), "nosniff");
    assert.match(response.headers.get("content-security-policy"), /script-src 'self'/);
    assert.equal(response.headers.get("x-observed-ip"), null);
    const post = await worker.fetch(request(path, { method: "POST" }), env);
    assert.equal(post.status, 405);
    assert.equal(post.headers.get("allow"), "GET, HEAD");
  }
  assert.deepEqual(paths, ["/", "/", "/browser.js", "/browser.css", "/peer", "/peer.js", "/peer-proof.js"]);
  const head = await worker.fetch(request("/test", { method: "HEAD" }), env);
  assert.equal(await head.text(), "");
});

test("missing static asset binding fails explicitly without changing the observer", async () => {
  assert.equal((await worker.fetch(request("/test"))).status, 503);
  assert.equal((await worker.fetch(request("/observe"))).status, 200);
});

test("iroh Wasm assets are allowlisted and relay CSP applies only to the iroh page", async () => {
  const env = { ASSETS: { fetch: async req => new Response(new URL(req.url).pathname) } };
  for (const path of ["/iroh", "/iroh.js", "/iroh-invite.js", "/iroh-wasm/browser_iroh.js", "/iroh-wasm/browser_iroh_bg.wasm"]) {
    const response = await worker.fetch(request(path), env);
    assert.equal(response.status, 200);
    assert.equal(await response.text(), path);
    const csp = response.headers.get("content-security-policy");
    assert.equal(csp.includes("'wasm-unsafe-eval'"), path === "/iroh");
    assert.equal(csp.includes("wss://aps1-1.relay.n0.iroh.link"), path === "/iroh");
    assert.equal(csp.includes("'unsafe-eval'"), false);
    assert.equal((await worker.fetch(request(path, { method: "POST" }), env)).status, 405);
  }
  assert.equal((await worker.fetch(request("/iroh-wasm/private.wasm"), env)).status, 404);
  const oldCsp = (await worker.fetch(request("/test"), env)).headers.get("content-security-policy");
  assert.equal(oldCsp.includes("iroh.link"), false);
  assert.equal(oldCsp.includes("wasm-unsafe-eval"), false);
});

test("signaling rejects cross-origin, invalid rooms and missing bindings before forwarding", async()=>{
  const path="/signal/"+Date.now()+"-12345678-1234-4321-8321-123456789abc";
  assert.equal((await worker.fetch(request(path))).status,403);
  assert.equal((await worker.fetch(request(path,{headers:{origin:"https://evil.example"}}))).status,403);
  assert.equal((await worker.fetch(request("/signal/invalid",{headers:{origin:"https://observe.example.com"}}))).status,410);
  assert.equal((await worker.fetch(request(path,{headers:{origin:"https://observe.example.com"}}))).status,503);
  let forwarded=false;
  const env={SIGNAL_ROOMS:{idFromName:name=>name,get:()=>({fetch:async()=>{forwarded=true;return new Response("upgrade");}})}};
  assert.equal((await worker.fetch(request(path,{headers:{origin:"https://observe.example.com"}}),env)).status,200);
  assert.equal(forwarded,true);
});
