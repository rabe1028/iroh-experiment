// The edge supplies CF-Connecting-IP and request.cf. Never accept observation
// values from query parameters or client-supplied x-observed-* headers.
export default {
  async fetch(request, env = {}) {
    const headers = new Headers({
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "cloudflare-cdn-cache-control": "no-store",
    });
    const reply = (status, body) => new Response(
      request.method === "HEAD" ? null : JSON.stringify(body),
      { status, headers },
    );
    const path = new URL(request.url).pathname;
    const browserAsset = ["/", "/test", "/browser.js", "/browser.css"].includes(path);
    if (path !== "/observe" && !browserAsset) {
      return reply(404, { error: "not found" });
    }
    if (!["GET", "HEAD"].includes(request.method)) {
      headers.set("allow", "GET, HEAD");
      return reply(405, { error: "method not allowed" });
    }
    if (browserAsset) {
      if (!env.ASSETS) return reply(503, { error: "browser assets unavailable" });
      const url = new URL(request.url);
      if (path === "/test") url.pathname = "/";
      const asset = await env.ASSETS.fetch(new Request(url, request));
      const response = new Response(asset.body, asset);
      response.headers.set("cache-control", "no-store");
      response.headers.set("cloudflare-cdn-cache-control", "no-store");
      response.headers.set("x-content-type-options", "nosniff");
      response.headers.set("referrer-policy", "no-referrer");
      response.headers.set("permissions-policy", "camera=(), microphone=(), geolocation=()");
      response.headers.set("content-security-policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'");
      return response;
    }
    const ip = request.headers.get("cf-connecting-ip");
    if (!ip) return reply(503, { error: "edge client address unavailable" });

    const cf = request.cf ?? {};
    const protocol = cf.httpProtocol;
    const isQuic = ["HTTP/3", "HTTP/3.0"].includes(protocol);
    const rtt = isQuic && Number.isFinite(cf.clientQuicRtt)
      && cf.clientQuicRtt >= 0 ? cf.clientQuicRtt : null;
    headers.set("x-observed-ip", ip);
    if (rtt !== null) headers.set("x-observed-rtt-ms", String(rtt));
    if (cf.colo) headers.set("x-observed-colo", cf.colo);
    if (protocol) {
      headers.set("x-observed-http-version", protocol);
      headers.set("x-observed-is-tcp", String(!isQuic));
    }
    // The source port is supplied by a Response Header Transform Rule, never
    // invented here. The client reads headers; the body describes Worker data.
    return reply(200, {
      observed_ip: ip,
      observed_port: null,
      rtt_ms: rtt,
      http_protocol: protocol ?? null,
      colo: cf.colo ?? null,
    });
  },
};
