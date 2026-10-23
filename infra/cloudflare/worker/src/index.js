import { roomExpiry } from "./signal-room.js";
import { relayRoom } from "./relay-protocol.js";
export { SignalingRoom } from "./signal-room.js";
export { IrohRelay } from "./iroh-relay.js";

const browserPaths = new Set(["/", "/test", "/browser.js", "/browser.css", "/peer", "/peer.js", "/peer-proof.js",
  "/iroh", "/iroh.js", "/iroh-invite.js", "/iroh-wasm/browser_iroh.js", "/iroh-wasm/browser_iroh_bg.wasm", "/relay-lab", "/relay-lab.js"]);
function jsonReply(request, status, body, extra = {}) {
  return new Response(request.method === "HEAD" ? null : JSON.stringify(body), {
    status, headers: { "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store", "cloudflare-cdn-cache-control": "no-store", ...extra },
  });
}
function signaling(request, env, path) {
  if (request.headers.get("origin") !== new URL(request.url).origin) return jsonReply(request, 403, { error: "same origin required" });
  if (request.method !== "GET") return jsonReply(request, 405, { error: "GET required" }, { allow: "GET" });
  if (!roomExpiry(path.slice(8))) return jsonReply(request, 410, { error: "room expired or invalid" });
  if (!env.SIGNAL_ROOMS) return jsonReply(request, 503, { error: "signaling unavailable" });
  return env.SIGNAL_ROOMS.get(env.SIGNAL_ROOMS.idFromName(path.slice(8))).fetch(request);
}
function irohRelay(request, env) {
  const url = new URL(request.url), origin = request.headers.get("origin");
  if (origin && origin !== url.origin) return jsonReply(request, 403, { error: "same origin required" });
  if (request.method !== "GET") return jsonReply(request, 405, { error: "GET required" }, { allow: "GET" });
  const room = relayRoom(url, Date.now(), url.pathname === "/relay-stats");
  if (!room) return jsonReply(request, 410, { error: "relay room expired or invalid" });
  if (!env.IROH_RELAYS) return jsonReply(request, 503, { error: "iroh relay unavailable" });
  return env.IROH_RELAYS.get(env.IROH_RELAYS.idFromName(room.token)).fetch(request);
}
function infrastructureRoute(request, env, path) {
  if (path === "/relay" || path === "/relay-stats") return irohRelay(request, env);
  if (path === "/ping" && ["GET", "HEAD"].includes(request.method)) return new Response(null, { status: 200, headers: { "cache-control": "no-store" } });
  if (path.startsWith("/signal/")) return signaling(request, env, path);
  return null;
}
async function browserAsset(request, env, path) {
  if (!env.ASSETS) return jsonReply(request, 503, { error: "browser assets unavailable" });
  const url = new URL(request.url);
  if (path === "/test") url.pathname = "/";
  const asset = await env.ASSETS.fetch(new Request(url, request));
  const response = new Response(asset.body, asset);
  response.headers.set("cache-control", "no-store");
  response.headers.set("cloudflare-cdn-cache-control", "no-store");
  response.headers.set("x-content-type-options", "nosniff");
  response.headers.set("referrer-policy", "no-referrer");
  response.headers.set("permissions-policy", "camera=(), microphone=(), geolocation=()");
  const irohPage = path === "/iroh" || path === "/relay-lab";
  response.headers.set("content-security-policy", `default-src 'none'; script-src 'self'${irohPage ? " 'wasm-unsafe-eval'" : ""}; style-src 'self'; connect-src 'self' ${url.protocol === "https:" ? "wss:" : "ws:"}//${url.host}; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`);
  return response;
}

// Observation values always come from the Cloudflare edge.
export default {
  async fetch(request, env = {}) {
    const path = new URL(request.url).pathname;
    const infrastructure = infrastructureRoute(request, env, path);
    if (infrastructure) return infrastructure;
    if (path !== "/observe" && !browserPaths.has(path)) return jsonReply(request, 404, { error: "not found" });
    if (!["GET", "HEAD"].includes(request.method)) return jsonReply(request, 405, { error: "method not allowed" }, { allow: "GET, HEAD" });
    if (browserPaths.has(path)) return browserAsset(request, env, path);
    const headers = new Headers();
    const reply = (status, body) => {
      const response = jsonReply(request, status, body);
      for (const [name, value] of headers) response.headers.set(name, value);
      return response;
    };
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
