import assert from "node:assert/strict";
import test from "node:test";
import { readInvitation, invitationLink, ROOM_MS } from "../public/iroh-invite.js";

const now = 1_790_000_000_000;
const invite = { id: "a".repeat(64), cap: "b".repeat(32), expires: now + ROOM_MS, room: `${now}-12345678-1234-4321-8321-123456789abc`, ping: 15 };
test("iroh invitation carries its capability only in the fragment", () => {
  const url = new URL(invitationLink("https://observe.example.com/iroh", invite));
  assert.equal(url.search, "");
  assert.equal(url.pathname, "/iroh");
  assert.deepEqual(readInvitation(url.hash, now), invite);
  assert.equal(readInvitation("", now), null);
});
test("iroh invitation rejects expired, future, malformed identities and capabilities", () => {
  for (const patch of [{ expires: now }, { expires: now + ROOM_MS + 30_001 }, { expires: Infinity },
    { expires: 0 }, { expires: 1.1 }, { id: "x".repeat(64) }, { id: "a".repeat(63) },
    { cap: "wss://evil.example/" }, { cap: "a".repeat(31) }, { room: "invalid" }, { ping: 1 }, { ping: 301 }]) {
    assert.throws(() => readInvitation(new URL(invitationLink("https://observe.example.com/iroh", { ...invite, ...patch })).hash, now));
  }
});
