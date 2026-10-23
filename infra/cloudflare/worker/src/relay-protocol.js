// Wire format pinned to iroh-relay 1.0.3 (protos/{common,handshake,relay}.rs).
import { blake3 } from "@noble/hashes/blake3.js";

export const PING_SECONDS = [15, 30, 60, 120, 300];
export const RELAY_TTL = 30 * 60_000;
export function relayRoom(url, now = Date.now(), history = false) {
  const token = url.searchParams.get("room") ?? "";
  if (!/^\d{13}-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(token)) return null;
  const created = Number(token.slice(0, 13)), expires = created + RELAY_TTL;
  const ping = Number(url.searchParams.get("ping") ?? 15);
  if (created > now + 30_000 || now >= expires + (history ? 3_600_000 : 0) || !PING_SECONDS.includes(ping)) return null;
  return { token, expires, ping };
}
export const hex = bytes => Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
export const unhex = text => Uint8Array.from(text.match(/../g), byte => Number.parseInt(byte, 16));
export function challengeMessage(challenge) {
  return blake3(challenge, { context: new TextEncoder().encode("iroh-relay handshake v1 challenge signature") });
}
export function challengeFrame(challenge) { return Uint8Array.of(0, ...challenge); }
export async function authenticate(frame, challenge) {
  // PublicKey is a fixed [u8;32]; serde_bytes signature is length-prefixed by 64.
  if (frame.length !== 98 || frame[0] !== 1 || frame[33] !== 64) throw new Error("invalid client auth");
  const publicKey = frame.slice(1, 33);
  const key = await crypto.subtle.importKey("raw", publicKey, "Ed25519", false, ["verify"]);
  if (!await crypto.subtle.verify("Ed25519", key, frame.slice(34), challengeMessage(challenge))) throw new Error("signature invalid");
  return hex(publicKey);
}
export function clientFrame(message) {
  if (!(message instanceof ArrayBuffer) || message.byteLength > 65_537 || !message.byteLength) throw new Error("invalid binary frame");
  const frame = new Uint8Array(message);
  if (frame[0] === 9 || frame[0] === 10) {
    if (frame.length !== 9) throw new Error("invalid ping/pong");
    return { type: frame[0] === 9 ? "ping" : "pong", frame };
  }
  if (frame[0] !== 4 && frame[0] !== 5) throw new Error("unexpected client frame");
  if (frame.length < (frame[0] === 4 ? 34 : 36) || frame[33] > 3) throw new Error("invalid datagram frame");
  return { type: "datagram", destination: hex(frame.slice(1, 33)), frame };
}
export function forwardedFrame(frame, source) {
  const output = frame.slice(); output[0] += 2; output.set(unhex(source), 1); return output;
}
export function protocol(request) {
  return request.headers.get("sec-websocket-protocol")?.split(",").map(x => x.trim()).includes("iroh-relay-v2") ? "iroh-relay-v2" : null;
}
