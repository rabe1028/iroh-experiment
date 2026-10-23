// The edge supplies CF-Connecting-IP and request.cf. Never accept observation
// values from query parameters or client-supplied x-observed-* headers.
export default {
  async fetch(request) {
    const headers = new Headers({
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "cloudflare-cdn-cache-control": "no-store",
    });
    const reply = (status, body) => new Response(
      request.method === "HEAD" ? null : JSON.stringify(body),
      { status, headers },
    );
    if (new URL(request.url).pathname !== "/observe") {
      return reply(404, { error: "not found" });
    }
    if (!["GET", "HEAD"].includes(request.method)) {
      headers.set("allow", "GET, HEAD");
      return reply(405, { error: "method not allowed" });
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
