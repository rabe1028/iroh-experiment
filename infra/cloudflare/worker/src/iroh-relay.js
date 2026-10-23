import { authenticate, challengeFrame, clientFrame, forwardedFrame, hex, protocol, relayRoom, unhex } from "./relay-protocol.js";
import { counters, addCounters, attachment, send } from "./relay-metrics.js";

// A bounded two-identity experimental relay. No global/public relay directory.
export class IrohRelay {
  constructor(ctx) {
    this.ctx = ctx;
    this.ready = ctx.blockConcurrencyWhile(async () => {
      const boots = (await ctx.storage.get("boots")) ?? 0;
      await ctx.storage.put("boots", boots + 1);
    });
  }
  async fetch(request) {
    await this.ready;
    const url = new URL(request.url), stats = url.pathname === "/relay-stats";
    const room = relayRoom(url, Date.now(), stats);
    if (!room) return new Response("invalid or expired room", { status: 410 });
    if (stats) return Response.json(await this.snapshot(), { headers: { "cache-control": "no-store" } });
    if (!protocol(request)) return new Response("iroh-relay-v2 required", { status: 400 });
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("WebSocket required", { status: 426 });
    const accepted = await this.admit(room);
    if (!accepted) return new Response("room admission limit or configuration mismatch", { status: 409 });
    const [client, server] = Object.values(new WebSocketPair());
    const challenge = crypto.getRandomValues(new Uint8Array(16));
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ phase: "challenge", challenge: hex(challenge), deadline: Date.now() + 10_000,
      expires: room.expires, id: null, archived: false, counts: counters(), bucket: Date.now(), bucket_messages: 0 });
    send(server, challengeFrame(challenge));
    await this.schedule();
    return new Response(null, { status: 101, webSocket: client, headers: { "sec-websocket-protocol": "iroh-relay-v2" } });
  }
  async admit(room) {
    return this.ctx.storage.transaction(async txn => {
      const meta = await txn.get("meta");
      const attempts = (await txn.get("attempts")) ?? 0;
      // Cloudflare includes CLOSING sockets in getWebSockets(). Retired sockets
      // cannot occupy a live admission slot; the lifetime upgrade cap still bounds them.
      const live = this.ctx.getWebSockets().filter(ws => !attachment(ws)?.archived && ws.readyState !== 2 && ws.readyState !== 3);
      if ((meta && meta.ping !== room.ping) || attempts >= 20 || live.length >= 4) return false;
      if (!meta) await txn.put("meta", { expires: room.expires, ping: room.ping, created_at: Date.now(), alarm_events: 0 });
      await txn.put("attempts", attempts + 1);
      return true;
    });
  }
  async webSocketMessage(ws, message) {
    await this.ready;
    try {
      let info = attachment(ws);
      if (!info || info.archived || Date.now() >= info.expires) throw new Error("expired");
      if (!(message instanceof ArrayBuffer)) throw new Error("binary frames required");
      if (Date.now() - info.bucket >= 10_000) { info.bucket = Date.now(); info.bucket_messages = 0; }
      info.bucket_messages++;
      info.counts.messages++; info.counts.inbound_bytes += message.byteLength;
      ws.serializeAttachment(info);
      if (info.bucket_messages > 512 || info.counts.messages > 10_000 || info.counts.inbound_bytes > 2 * 1024 * 1024) throw new Error("relay budget");
      if (info.phase === "challenge") { await this.authorize(ws, new Uint8Array(message)); return; }
      if (info.phase !== "authenticated") throw new Error("authentication in progress");
      const data = clientFrame(message);
      info = attachment(ws); info.counts[data.type]++; ws.serializeAttachment(info);
      if (data.type === "ping") { const pong = data.frame.slice(); pong[0] = 10; send(ws, pong, "server_pong"); }
      else if (data.type === "pong") {
        if (hex(data.frame.slice(1)) !== info.pending_ping) throw new Error("unexpected pong");
        info.pending_ping = null; info.deadline = null; ws.serializeAttachment(info);
        await this.schedule();
      } else await this.route(ws, data);
    } catch { await this.finish(ws, 1008, "invalid, expired or over-budget relay session"); }
  }
  async authorize(ws, frame) {
    let info = attachment(ws);
    if (Date.now() >= info.deadline) throw new Error("auth timeout");
    info.phase = "verifying"; ws.serializeAttachment(info);
    const id = await authenticate(frame, unhex(info.challenge));
    const allowed = await this.ctx.storage.transaction(async txn => {
      const ids = (await txn.get("identities")) ?? [];
      if (!ids.includes(id)) {
        if (ids.length >= 2) return false;
        await txn.put("identities", [...ids, id]);
      }
      return true;
    });
    info = attachment(ws);
    if (!allowed || info.archived || Date.now() >= info.deadline || Date.now() >= info.expires) throw new Error("identity limit or expired auth");
    info = attachment(ws); info.id = id; info.phase = "authenticated"; info.challenge = null;
    info.deadline = null; info.counts.auth++; ws.serializeAttachment(info);
    // Claim synchronously before any await; simultaneous reconnects must not
    // leave two authenticated sockets routing the same EndpointId.
    for (const old of this.ctx.getWebSockets()) {
      const other = attachment(old);
      if (old !== ws && other?.id === id && !other.archived) await this.finish(old, 1000, "identity reconnected");
    }
    if (attachment(ws).archived) throw new Error("superseded authentication");
    send(ws, Uint8Array.of(2));
    const meta = await this.ctx.storage.get("meta");
    if (!meta.next_ping) { meta.next_ping = Date.now() + meta.ping * 1000; await this.ctx.storage.put("meta", meta); }
    await this.schedule();
  }
  async route(ws, data) {
    const info = attachment(ws);
    const target = this.ctx.getWebSockets().find(peer => {
      const other = attachment(peer);
      return peer !== ws && other?.phase === "authenticated" && !other.archived && other.id === data.destination;
    });
    if (!target) { info.counts.dropped++; ws.serializeAttachment(info); return; }
    try {
      send(target, forwardedFrame(data.frame, info.id));
      info.counts.forwarded++; ws.serializeAttachment(info);
    } catch {
      await this.finish(target, 1011, "datagram delivery failed");
      const current = attachment(ws);
      if (!current.archived) { current.counts.dropped++; ws.serializeAttachment(current); }
    }
  }
  async schedule() {
    const meta = await this.ctx.storage.get("meta");
    if (!meta) return;
    const active = this.ctx.getWebSockets().map(attachment).filter(info => info && !info.archived);
    let next = meta.expires + 3_600_000;
    if (active.length) {
      next = meta.expires;
      if (meta.next_ping) next = Math.min(next, meta.next_ping);
      for (const info of active) if (info.deadline) next = Math.min(next, info.deadline);
    }
    await this.ctx.storage.setAlarm(Math.max(Date.now() + 1, next));
  }
  async alarm() {
    await this.ready;
    const meta = await this.ctx.storage.get("meta");
    if (!meta) return;
    const now = Date.now(); meta.alarm_events++;
    if (now >= meta.expires + 3_600_000) { await this.ctx.storage.deleteAll(); return; }
    const pingDue = meta.next_ping && now >= meta.next_ping;
    for (const ws of this.ctx.getWebSockets()) {
      const info = attachment(ws);
      if (!info || info.archived) continue;
      if (now >= info.expires || (info.deadline && now >= info.deadline)) {
        await this.finish(ws, 1000, "room or handshake/pong timeout"); continue;
      }
      if (pingDue && info.phase === "authenticated" && !info.pending_ping) {
        const ping = crypto.getRandomValues(new Uint8Array(8));
        info.pending_ping = hex(ping); info.deadline = now + 5_000; ws.serializeAttachment(info);
        try { send(ws, Uint8Array.of(9, ...ping), "server_ping"); }
        catch { await this.finish(ws, 1011, "ping delivery failed"); }
      }
    }
    if (pingDue) meta.next_ping = now + meta.ping * 1000;
    await this.ctx.storage.put("meta", meta);
    await this.schedule();
  }
  async finish(ws, code, reason) {
    const info = attachment(ws);
    if (!info || info.archived) return;
    info.archived = true; ws.serializeAttachment(info);
    try { ws.close([1005, 1006, 1015].includes(code) ? 1000 : code, reason); } catch { /* Already closed. */ }
    await this.ctx.storage.transaction(async txn => {
      const closed = (await txn.get("closed")) ?? { connections: 0, counts: counters(), recent: [] };
      closed.connections++; addCounters(closed.counts, info.counts);
      closed.recent = [...(closed.recent ?? []).slice(-15), { at: Date.now(), code, reason: reason.slice(0, 120), id: info.id }];
      await txn.put("closed", closed);
    });
    if (info.id) for (const peer of this.ctx.getWebSockets()) {
      const other = attachment(peer);
      if (peer !== ws && other?.phase === "authenticated" && !other.archived && other.id !== info.id) {
        try { send(peer, Uint8Array.of(8, ...unhex(info.id)), "endpoint_gone"); } catch { /* Closing peer. */ }
      }
    }
    await this.schedule();
  }
  async webSocketClose(ws, code = 1000, reason = "peer closed") { await this.finish(ws, code, reason); }
  async webSocketError(ws) { await this.finish(ws, 1011, "socket error"); }
  async snapshot() {
    const meta = await this.ctx.storage.get("meta");
    const closed = (await this.ctx.storage.get("closed")) ?? { connections: 0, counts: counters() };
    const counts = { ...closed.counts };
    const peers = [];
    for (const ws of this.ctx.getWebSockets()) {
      const info = attachment(ws);
      if (info && !info.archived) { addCounters(counts, info.counts); peers.push({ id: info.id, phase: info.phase }); }
    }
    return { protocol: "iroh-relay-v2", implementation: "cloudflare-durable-object", object_id: this.ctx.id?.toString() ?? null, ...meta,
      sampled_at: Date.now(), constructor_runs: await this.ctx.storage.get("boots"),
      websocket_upgrades: (await this.ctx.storage.get("attempts")) ?? 0,
      closed_connections: closed.connections, recent_closes: closed.recent ?? [], active_connections: peers.length, peers, counts,
      platform_billing_requests: null, platform_active_gb_s: null };
  }
}
