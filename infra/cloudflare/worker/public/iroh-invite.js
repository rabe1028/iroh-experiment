export const ROOM_MS = 10 * 60_000;
export function readInvitation(hash, now = Date.now()) {
  if (!hash) return null;
  const fields = new URLSearchParams(hash.replace(/^#/, ""));
  const id = fields.get("id"), cap = fields.get("cap"), expires = Number(fields.get("expires"));
  if (!/^[a-f0-9]{64}$/.test(id ?? "") || !/^[a-f0-9]{32}$/.test(cap ?? "")
      || !Number.isSafeInteger(expires) || expires <= now || expires > now + ROOM_MS + 30_000) {
    throw new Error("招待リンクが無効、または期限切れです。PCで新しいリンクを作ってください。");
  }
  return { id, cap, expires };
}
export function invitationLink(base, invitation) {
  const url = new URL(base);
  url.hash = new URLSearchParams(invitation).toString();
  return url.href;
}
