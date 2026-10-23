import { ROOM_MS, readInvitation, invitationLink } from "/iroh-invite.js";

const $ = id => document.getElementById(id);
let node, report, active = false, generation = 0, expiryTimer, wasm;
const scope = { engine: "iroh-wasm", iroh_version: "1.0.3", iroh_peer_tested: false,
  direct_connection_tested: false, nat_traversal_tested: false,
  nat_traversal_success: null, same_socket_stun_h3_tested: false };
function state(message, badge = "実行中", tone = "neutral") {
  $("status").textContent = message; $("state-badge").textContent = badge;
  $("state-badge").dataset.tone = tone;
}
function showReport() {
  $("json-output").textContent = JSON.stringify(report, null, 2);
  $("json-details").hidden = false; $("download").disabled = false;
}
function invitationUI() {
  if (active) return;
  try {
    const invite = readInvitation(location.hash);
    $("start").hidden = Boolean(invite); $("join").hidden = !invite;
    state(invite ? "招待リンクを確認しました。参加すると接続を開始します。" : "まず片方の端末でリンクを作ってください。", "待機中");
  } catch (error) {
    $("start").hidden = false; $("join").hidden = true;
    state(error.message, "リンク無効", "error");
  }
}
async function stop(message = "中止しました。再実行すると新しいiroh IDを作ります。") {
  generation++; active = false; clearTimeout(expiryTimer);
  const old = node; node = null;
  $("stop").disabled = true; $("start").disabled = false; $("join").disabled = false;
  $("invitation").hidden = true;
  if (report && !report.finished_at) {
    report.finished_at = new Date().toISOString(); report.error = message; showReport();
  }
  state(message, "切断");
  if (old) await old.close();
}
async function start(join) {
  if (active) return;
  const run = ++generation;
  active = true; $("start").disabled = true; $("join").disabled = true; $("stop").disabled = false;
  $("invitation").hidden = true; $("json-details").hidden = true; $("download").disabled = true;
  $("echo").textContent = "—"; $("path").textContent = "—";
  $("local-id").textContent = "—"; $("remote-id").textContent = "—";
  report = { schema: "iroh-browser-relay/v1", ...scope, started_at: new Date().toISOString(),
    network: document.querySelector('input[name="network"]:checked').value,
    role: join ? "dial" : "listen", echo_success: null, measurement: null, error: null };
  try {
    const invite = join ? readInvitation(location.hash) : null;
    if (join && !invite) throw new Error("招待リンクがありません。");
    const cap = invite?.cap ?? Array.from(crypto.getRandomValues(new Uint8Array(16)), v => v.toString(16).padStart(2, "0")).join("");
    const expires = invite?.expires ?? Date.now() + ROOM_MS;
    expiryTimer = setTimeout(() => { if (run === generation) void stop("リンクの有効期限になりました。新しいリンクで再実行してください。"); }, expires - Date.now());
    state("Wasmを読み込み、irohをrelayに接続しています…");
    wasm ??= import("/iroh-wasm/browser_iroh.js").then(async module => { await module.default(); return module; }).catch(error => { wasm = undefined; throw error; });
    const module = await wasm;
    if (run !== generation) return;
    const created = await module.BrowserNode.start(cap);
    if (run !== generation) { await created.close(); return; }
    node = created;
    $("local-id").textContent = node.endpoint_id();
    report.endpoint_id = node.endpoint_id();
    if (!join) {
      $("room-link").value = invitationLink(location.href, { id: node.endpoint_id(), cap, expires });
      $("invitation").hidden = false; $("copy-status").textContent = "もう1台が参加するまで、この画面を開いたままにしてください。";
    }
    state(join ? "相手のiroh IDを認証し、64KiBを往復させています…" : "iroh起動済み。もう1台でリンクを開いて参加してください。");
    report.iroh_peer_tested = true;
    const measurement = JSON.parse(await (join ? node.connect_echo(invite.id) : node.listen_once()));
    if (run !== generation) return;
    report.measurement = measurement; report.iroh_peer_tested = true;
    report.echo_success = measurement.echo_success; report.finished_at = new Date().toISOString();
    $("remote-id").textContent = measurement.remote_id;
    $("echo").textContent = "64 KiB ✓"; $("path").textContent = "relay";
    showReport(); clearTimeout(expiryTimer);
    state("irohで64KiBの往復・内容一致を確認しました。経路はrelayです。NAT越えは未検証です。", "往復成功", "good");
    // Leave the endpoint alive briefly so the peer can finish consuming its ACK.
    expiryTimer = setTimeout(() => { if (run === generation) void stop("測定済みのiroh接続を閉じました。結果は保存できます。"); }, 30_000);
  } catch (error) {
    if (run !== generation) return;
    report.error = String(error?.message ?? error); report.finished_at = new Date().toISOString(); showReport();
    const old = node; node = null; clearTimeout(expiryTimer); active = false;
    $("stop").disabled = true; $("start").disabled = false; $("join").disabled = false;
    $("invitation").hidden = true;
    state(report.error, "未完了", "error");
    if (old) await old.close();
  }
}
$("start").addEventListener("click", () => { void start(false); });
$("join").addEventListener("click", () => { void start(true); });
$("stop").addEventListener("click", () => { void stop(); });
$("copy").addEventListener("click", async () => {
  try { await navigator.clipboard.writeText($("room-link").value); $("copy-status").textContent = "コピーしました。もう1台で開いてください。"; }
  catch { $("room-link").select(); $("copy-status").textContent = "リンクを長押ししてコピーしてください。"; }
});
$("download").addEventListener("click", () => {
  const url = URL.createObjectURL(new Blob([JSON.stringify(report, null, 2)], { type: "application/json" }));
  const a = document.createElement("a"); a.href = url; a.download = `iroh-browser-${Date.now()}.json`;
  a.click(); setTimeout(() => URL.revokeObjectURL(url), 10_000);
  $("export-status").textContent = "保存できない場合は「JSONを表示」から取り出せます。";
});
addEventListener("hashchange", invitationUI);
addEventListener("pagehide", () => { if (node) void node.close(); });
invitationUI();
