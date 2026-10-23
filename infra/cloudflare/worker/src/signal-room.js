// The room forwards signaling only. Echo payloads have no server message type.
export const ROOM_TTL_MS = 10 * 60 * 1000;
export function roomExpiry(token, now = Date.now()) {
  if (!/^\d{13}-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(token)) return null;
  const created = Number(token.slice(0, 13));
  return created <= now + 30000 && now < created + ROOM_TTL_MS ? created + ROOM_TTL_MS : null;
}

function validateSignal(data, info) {
  if (data.type === "candidate") {
    if (typeof data.candidate?.candidate !== "string" || data.candidate.candidate.length > 2048) throw new Error("invalid candidate");
    return;
  }
  const expected = info.role === "offerer" ? "offer" : "answer";
  if (data.type !== expected || info.sdpSent) throw new Error("invalid SDP order");
  if (typeof data.sdp !== "string" || !data.sdp.startsWith("v=0\r\n")) throw new Error("invalid SDP");
  info.sdpSent = true;
}

export class SignalingRoom {
  constructor(ctx) { this.ctx = ctx; }

  async fetch(request) {
    const token = new URL(request.url).pathname.split("/").at(-1);
    const expires = roomExpiry(token);
    if (!expires) return new Response("expired or invalid room", { status: 410 });
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket required", { status: 426 });
    }
    const admission = await this.ctx.storage.transaction(async txn => {
      const joined = (await txn.get("joined")) ?? 0;
      if (joined >= 2) return null;
      await txn.put("joined", joined + 1);
      await txn.setAlarm(expires);
      return joined === 0 ? "offerer" : "answerer";
    });
    if (!admission) return new Response("room already used; create a new room", { status: 409 });
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ role: admission, expires, messages: 0, bytes: 0, sdpSent: false });
    server.send(JSON.stringify({ type: "hello", role: admission, expires_at: expires }));
    if (this.ctx.getWebSockets().length === 2) this.broadcast({ type: "ready" });
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, message) {
    try {
      const info = ws.deserializeAttachment();
      if (!info || Date.now() >= info.expires) throw new Error("expired");
      if (typeof message !== "string" || message.length > 24576) throw new Error("message too large");
      info.messages++;
      info.bytes += new TextEncoder().encode(message).length;
      if (info.messages > 32 || info.bytes > 65536) throw new Error("signaling limit");
      const data = JSON.parse(message);
      validateSignal(data, info);
      ws.serializeAttachment(info);
      this.broadcast(data, ws);
    } catch {
      ws.close(1008, "invalid or expired signaling");
    }
  }

  broadcast(data, except = null) {
    const encoded = JSON.stringify(data);
    for (const ws of this.ctx.getWebSockets()) {
      if (ws !== except) {
        try { ws.send(encoded); } catch { /* A closing peer needs no retry. */ }
      }
    }
  }

  webSocketClose(ws) { this.broadcast({ type: "peer-left" }, ws); }
  webSocketError(ws) { ws.close(1011, "signaling error"); }
  async alarm() {
    for (const ws of this.ctx.getWebSockets()) ws.close(1000, "room expired");
    await this.ctx.storage.deleteAll();
  }
}
