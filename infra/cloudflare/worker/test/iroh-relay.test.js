import assert from "node:assert/strict";
import test from "node:test";
import { IrohRelay } from "../src/iroh-relay.js";
import { authenticate, challengeMessage, clientFrame, forwardedFrame, hex, relayRoom } from "../src/relay-protocol.js";
import { counters } from "../src/relay-metrics.js";

async function identity() {
  const keys = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]);
  const raw = new Uint8Array(await crypto.subtle.exportKey("raw", keys.publicKey));
  return { id: hex(raw), async auth(challenge) {
    const sig = new Uint8Array(await crypto.subtle.sign("Ed25519", keys.privateKey, challengeMessage(challenge)));
    return Uint8Array.of(1, ...raw, 64, ...sig);
  } };
}
function socket(info) {
  return { info, sent: [], closed: null, serializeAttachment(value) { this.info = structuredClone(value); },
    deserializeAttachment() { return structuredClone(this.info); }, send(data) { this.sent.push(new Uint8Array(data).slice()); },
    close(code, reason) { this.closed = { code, reason }; } };
}
async function room() {
  const values = new Map([["meta", { expires: Date.now() + 60_000, ping: 15, created_at: Date.now(), alarm_events: 0 }]]);
  const storage = { async get(key) { return structuredClone(values.get(key)); }, async put(key, v) { values.set(key, structuredClone(v)); },
    async transaction(fn) { return fn(storage); }, async setAlarm(when) { storage.nextAlarm = when; }, async deleteAll() { values.clear(); } };
  const sockets = [];
  const ctx = { storage, getWebSockets: () => sockets, blockConcurrencyWhile: fn => fn() };
  const relay = new IrohRelay(ctx); await relay.ready;
  return { relay, ctx, sockets, values, add() {
    const challenge = crypto.getRandomValues(new Uint8Array(16));
    const ws = socket({ phase: "challenge", challenge: hex(challenge), deadline: Date.now() + 10_000,
      expires: Date.now() + 60_000, archived: false, id: null, counts: counters(), bucket: Date.now(), bucket_messages: 0 });
    sockets.push(ws); return { ws, challenge };
  } };
}

test("iroh relay authentication verifies ownership, rejects a tampered signature and challenge replay", async () => {
  const key = await identity(), challenge = crypto.getRandomValues(new Uint8Array(16));
  const frame = await key.auth(challenge);
  assert.equal(await authenticate(frame, challenge), key.id);
  const tampered = frame.slice(); tampered[50] ^= 1;
  await assert.rejects(authenticate(tampered, challenge));
  await assert.rejects(authenticate(frame, crypto.getRandomValues(new Uint8Array(16))));
  await assert.rejects(authenticate(frame.slice(1), challenge));
});
test("binary framing cannot spoof source identity or bypass type and size bounds", () => {
  const destination = new Uint8Array(32).fill(7), source = "08".repeat(32);
  for (const tag of [4, 5]) {
    const frame = Uint8Array.of(tag, ...destination, 0, ...(tag === 5 ? [4, 176] : []), 1, 2, 3);
    const parsed = clientFrame(frame.buffer), forwarded = forwardedFrame(parsed.frame, source);
    assert.equal(parsed.destination, "07".repeat(32));
    assert.equal(forwarded[0], tag + 2);
    assert.equal(hex(forwarded.slice(1, 33)), source);
    assert.deepEqual(forwarded.slice(33), frame.slice(33));
  }
  for (const frame of ["text", new ArrayBuffer(0), new ArrayBuffer(65_538), Uint8Array.of(6).buffer,
    Uint8Array.of(9, 1).buffer, Uint8Array.of(5, ...destination, 0).buffer]) assert.throws(() => clientFrame(frame));
});
test("authenticated routing survives DO reconstruction; reconnects cannot consume a third identity", async () => {
  const r = await room(), a = await identity(), b = await identity(), c = await identity();
  const A = r.add(), B = r.add();
  await r.relay.webSocketMessage(A.ws, (await a.auth(A.challenge)).buffer);
  await r.relay.webSocketMessage(B.ws, (await b.auth(B.challenge)).buffer);
  const resumed = new IrohRelay(r.ctx); await resumed.ready;
  const frame = Uint8Array.of(4, ...new Uint8Array((await b.auth(B.challenge)).slice(1, 33)), 0, 42);
  await resumed.webSocketMessage(A.ws, frame.buffer);
  assert.equal(B.ws.sent.at(-1)[0], 6);
  assert.equal(hex(B.ws.sent.at(-1).slice(1, 33)), a.id);
  const C = r.add(); await resumed.webSocketMessage(C.ws, (await c.auth(C.challenge)).buffer);
  assert.equal(C.ws.closed.code, 1008);
  const snapshot = await resumed.snapshot();
  assert.equal(snapshot.counts.auth, 2); assert.equal(snapshot.counts.forwarded, 1);
  assert.equal(snapshot.constructor_runs, 2);
  await resumed.webSocketClose(A.ws); await resumed.webSocketClose(A.ws);
  assert.equal((await resumed.snapshot()).counts.auth, 2);
  assert.equal(B.ws.sent.at(-1)[0], 8);
});
test("unauthenticated forwarding, expired sessions and quota overflow fail closed", async () => {
  const r = await room(), A = r.add();
  await r.relay.webSocketMessage(A.ws, Uint8Array.of(4, ...new Uint8Array(32), 0, 1).buffer);
  assert.equal(A.ws.closed.code, 1008);
  const B = r.add(); B.ws.info.expires = Date.now() - 1;
  await r.relay.webSocketMessage(B.ws, Uint8Array.of(9, ...new Uint8Array(8)).buffer);
  assert.equal(B.ws.closed.code, 1008);
  const C = r.add(); C.ws.info.counts.inbound_bytes = 2 * 1024 * 1024;
  await r.relay.webSocketMessage(C.ws, Uint8Array.of(1).buffer);
  assert.equal(C.ws.closed.code, 1008);
});
test("simultaneous reconnects retain one authenticated route and preserve cumulative counters", async () => {
  const r = await room(), key = await identity(), sockets = [r.add(), r.add(), r.add()];
  const frames = await Promise.all(sockets.map(s => key.auth(s.challenge)));
  await Promise.all(sockets.map((s, i) => r.relay.webSocketMessage(s.ws, frames[i].buffer)));
  const snapshot = await r.relay.snapshot();
  assert.equal(snapshot.peers.filter(p => p.phase === "authenticated").length, 1);
  assert.equal(snapshot.counts.auth, 3);
});
test("a closing receiver cannot force the authenticated sender to disconnect", async () => {
  const r = await room(), a = await identity(), b = await identity(), A = r.add(), B = r.add();
  await r.relay.webSocketMessage(A.ws, (await a.auth(A.challenge)).buffer);
  const bAuth = await b.auth(B.challenge);
  await r.relay.webSocketMessage(B.ws, bAuth.buffer);
  B.ws.send = () => { throw new Error("receiver disconnected"); };
  await r.relay.webSocketMessage(A.ws, Uint8Array.of(4, ...bAuth.slice(1, 33), 0, 42).buffer);
  assert.equal(A.ws.closed, null);
  assert.equal(B.ws.info.archived, true);
  assert.equal(A.ws.info.counts.dropped, 1);
});
test("retired CLOSING sockets cannot exhaust admission slots; live and lifetime bounds still apply", async () => {
  const r = await room();
  for (let i = 0; i < 4; i++) { const { ws } = r.add(); ws.info.archived = true; ws.readyState = 2; }
  const config = { ping: 15, expires: Date.now() + 60_000 };
  assert.equal(await r.relay.admit(config), true);
  for (let i = 0; i < 4; i++) r.add();
  assert.equal(await r.relay.admit(config), false);
  for (const ws of r.sockets) ws.info.archived = true;
  r.values.set("attempts", 20);
  assert.equal(await r.relay.admit(config), false);
});
test("alarm sends binary protocol pings; a valid pong cancels the deadline", async () => {
  const r = await room(), key = await identity(), A = r.add();
  await r.relay.webSocketMessage(A.ws, (await key.auth(A.challenge)).buffer);
  r.values.get("meta").next_ping = Date.now() - 1;
  await r.relay.alarm();
  const ping = A.ws.sent.at(-1); assert.equal(ping[0], 9); assert.equal(ping.length, 9);
  const pong = ping.slice(); pong[0] = 10;
  await r.relay.webSocketMessage(A.ws, pong.buffer);
  assert.equal(A.ws.info.pending_ping, null); assert.equal(A.ws.info.deadline, null);
  assert.equal((await r.relay.snapshot()).counts.server_ping, 1);
});
test("relay capabilities expire and only the five experiment ping values are valid", () => {
  const now = 1_790_000_000_000, token = `${now}-12345678-1234-4321-8321-123456789abc`;
  assert.ok(relayRoom(new URL(`https://relay.example/relay?room=${token}&ping=300`), now));
  assert.equal(relayRoom(new URL(`https://relay.example/relay?room=${token}&ping=1`), now), null);
  assert.equal(relayRoom(new URL(`https://relay.example/relay?room=${token}`), now + 1_800_000), null);
  assert.ok(relayRoom(new URL(`https://relay.example/relay-stats?room=${token}`), now + 1_800_000, true));
});
