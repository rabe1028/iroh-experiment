export const ROOM_MS = 10 * 60_000;
export function readInvitation(hash, now = Date.now()) {
  if (!hash) return null;
  const fields = new URLSearchParams(hash.replace(/^#/, ""));
  const id = fields.get("id"), cap = fields.get("cap"), expires = Number(fields.get("expires"));
  const room = fields.get("room"), ping = Number(fields.get("ping") ?? 15);
  if (!/^[a-f0-9]{64}$/.test(id ?? "") || !/^[a-f0-9]{32}$/.test(cap ?? "")
      || !Number.isSafeInteger(expires) || expires <= now || expires > now + ROOM_MS + 30_000
      || !validRelayRoom(room, now) || ![15, 30, 60, 120, 300].includes(ping)) {
    throw new Error("招待リンクが無効、または期限切れです。PCで新しいリンクを作ってください。");
  }
  return { id, cap, expires, room, ping };
}
export function validRelayRoom(room, now = Date.now()) {
  return /^\d{13}-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(room ?? "")
    && Number(room.slice(0, 13)) <= now + 30_000 && now < Number(room.slice(0, 13)) + 30 * 60_000;
}
export function relayAddress(base, room, ping) {
  const url = new URL("/", base); url.search = new URLSearchParams({ room, ping }); return url.href;
}
export function invitationLink(base, invitation) {
  const url = new URL(base);
  url.hash = new URLSearchParams(invitation).toString();
  return url.href;
}
