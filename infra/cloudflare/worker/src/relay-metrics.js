export function counters() {
  return { messages: 0, inbound_bytes: 0, outbound_bytes: 0, auth: 0, ping: 0, pong: 0,
    datagram: 0, forwarded: 0, dropped: 0, server_ping: 0, server_pong: 0, endpoint_gone: 0 };
}
export function addCounters(total, values) {
  for (const key of Object.keys(total)) total[key] += values[key] ?? 0;
  return total;
}
export function attachment(ws) { return ws.deserializeAttachment(); }
export function send(ws, bytes, category) {
  const info = attachment(ws);
  ws.send(bytes);
  info.counts.outbound_bytes += bytes.byteLength;
  if (category) info.counts[category]++;
  ws.serializeAttachment(info);
}
