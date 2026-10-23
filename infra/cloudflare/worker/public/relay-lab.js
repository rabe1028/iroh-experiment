import init, { BrowserNode } from "/iroh-wasm/browser_iroh.js";
import { relayAddress } from "/iroh-invite.js";

const $ = id => document.getElementById(id), pings = [15, 30, 60, 120, 300];
let generation = 0, session, report, finishWaiting;
const initialized = init();
function row(record, state) {
  const tr = document.querySelector(`[data-key="${record.key}"]`);
  const delta = record.delta;
  [record.mode === "endpoint-only" ? "Endpoint待機" : "QUIC接続維持", `${record.ping}s`, state,
    delta?.messages ?? "—", delta ? `${delta.ping} / ${delta.pong}` : "—", delta?.datagram ?? "—",
    record.constructor_runs_delta ?? "—"].forEach((value, i) => { tr.children[i].textContent = value; });
}
function output() {
  $("lab-output").textContent = serialized(); $("lab-json").hidden = false; $("download").disabled = false;
}
async function statistics(url) {
  const endpoint = new URL(url); endpoint.pathname = "/relay-stats";
  const response = await fetch(endpoint, { cache: "no-store", signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`relay statistics HTTP ${response.status}`);
  return response.json();
}
async function newNode(current, cap, url, ping) {
  const node = await BrowserNode.start(cap, url, ping);
  if (current.generation !== generation) { await node.close(); throw new Error("cancelled"); }
  current.nodes.push(node); return node;
}
async function prepare(current, record) {
  const cap = crypto.randomUUID().replaceAll("-", ""), token = `${Date.now()}-${crypto.randomUUID()}`;
  const url = relayAddress(location.href, token, record.ping);
  // The capability URL stays in closure memory and is never put in the exported report.
  record.statistics = () => statistics(url);
  const a = await newNode(current, cap, url, record.ping);
  record.runtime_handles = [a];
  if (record.mode === "quic-idle") {
    const b = await newNode(current, cap, url, record.ping); record.runtime_handles.push(b);
    const [listen, dial] = await Promise.all([a.listen_once(), b.connect_echo(a.endpoint_id())]);
    record.echo = [JSON.parse(listen), JSON.parse(dial)];
  }
  row(record, "準備完了");
}
async function closeNodes(current) {
  await Promise.allSettled(current.nodes.map(node => node.close()));
}
async function stopExperiment() {
  generation++; finishWaiting?.(); finishWaiting = null;
  if (session) await closeNodes(session);
  if (report && !report.finished_at) { report.cancelled = true; report.finished_at = new Date().toISOString(); output(); }
  $("start").disabled = false; $("stop").disabled = true;
  $("lab-status").textContent = "測定を中止しました。取得済みの結果を保存できます。";
}
async function sample(current) {
  await Promise.all(report.conditions.map(async record => {
    if (record.error) return;
    try { record.baseline = await record.statistics(); }
    catch (error) { record.error = String(error); row(record, "集計失敗"); }
  }));
  if (current.generation !== generation) return;
  if (report.conditions.every(record => record.error)) throw new Error("すべての条件で準備に失敗しました。");
  report.sample_started_at = new Date().toISOString(); const start = performance.now();
  function captureHealth() {
    for (const record of report.conditions) {
      if (record.error) continue;
      const states = record.runtime_handles.map(node => JSON.parse(node.health()));
      const fingerprint = JSON.stringify(states);
      if (fingerprint !== record.last_health) {
        record.health_events ??= []; record.health_events.push({ elapsed_ms: performance.now() - start, nodes: states });
        record.last_health = fingerprint;
      }
    }
  }
  captureHealth();
  await new Promise(resolve => {
    const timer = setInterval(() => {
      captureHealth();
      $("lab-status").textContent = `観測中：${Math.min(330, Math.floor((performance.now() - start) / 1000))} / 330秒。アプリデータとstatus pollingは停止中です。`;
    }, 1000);
    const done = () => { clearInterval(timer); clearTimeout(end); captureHealth(); resolve(); };
    const end = setTimeout(done, 330_000); finishWaiting = done;
  });
  finishWaiting = null;
  if (current.generation !== generation) return;
  report.sample_elapsed_ms = performance.now() - start;
  await Promise.all(report.conditions.map(async record => {
    if (record.error) return;
    try {
      record.relay_connected = record.runtime_handles.every(node => node.relay_connected());
      record.quic_connected = record.mode === "quic-idle" ? record.runtime_handles.every(node => node.connected()) : null;
      record.final = await record.statistics();
      record.sample_elapsed_ms = record.final.sampled_at - record.baseline.sampled_at;
      record.delta = Object.fromEntries(Object.keys(record.final.counts).map(key => [key, record.final.counts[key] - record.baseline.counts[key]]));
      record.constructor_runs_delta = record.final.constructor_runs - record.baseline.constructor_runs;
      record.reconnects = record.final.websocket_upgrades - record.baseline.websocket_upgrades;
      record.survived = record.relay_connected && record.quic_connected !== false && record.reconnects === 0
        && record.final.closed_connections === record.baseline.closed_connections;
      row(record, record.survived ? "維持 ✓" : "切断 / 再接続");
    } catch (error) { record.error = String(error); row(record, "集計失敗"); }
  }));
}
async function runExperiment() {
  const current = { generation: ++generation, nodes: [] }; session = current;
  $("start").disabled = true; $("stop").disabled = false; $("download").disabled = true; $("lab-json").hidden = true;
  $("lab-rows").replaceChildren();
  report = { schema: "cloudflare-iroh-relay-ping/v1", started_at: new Date().toISOString(),
    transport: "iroh-wasm-cloudflare-do", separate_devices_tested: false, nat_traversal_tested: false,
    platform_billing_requests: null, platform_active_gb_s: null, conditions: [] };
  for (const mode of ["endpoint-only", "quic-idle"]) for (const ping of pings) {
    const record = { key: `${mode}-${ping}`, mode, ping, error: null };
    report.conditions.push(record);
    const tr = document.createElement("tr"); tr.dataset.key = record.key;
    for (let i = 0; i < 7; i++) tr.append(document.createElement("td"));
    $("lab-rows").append(tr); row(record, "準備中");
  }
  // Runtime handles and functions are excluded from JSON by the replacer below.
  try {
    $("lab-status").textContent = "iroh Endpointを起動し、DO relayへの接続と64KiB往復を準備しています…";
    await initialized;
    await Promise.all(report.conditions.map(async record => {
      try { await prepare(current, record); }
      catch (error) { record.error = String(error); row(record, "準備失敗"); }
    }));
    if (current.generation !== generation) return;
    await sample(current);
    if (current.generation !== generation) return;
    report.finished_at = new Date().toISOString();
    report.all_conditions_survived = report.conditions.every(record => record.survived && !record.error);
    $("lab-status").textContent = "330秒の観測が完了しました。結果には準備失敗・再接続も含めて記録しています。";
    await closeNodes(current); output();
  } catch (error) {
    if (current.generation !== generation) return;
    report.error = String(error); report.finished_at = new Date().toISOString(); await closeNodes(current); output();
    $("lab-status").textContent = report.error;
  } finally {
    if (current.generation === generation) { $("start").disabled = false; $("stop").disabled = true; }
  }
}
function serialized() { return JSON.stringify(report, (key, value) => key === "runtime_handles" || key === "statistics" || key === "last_health" ? undefined : value, 2); }
$("start").addEventListener("click", () => { void runExperiment(); });
$("stop").addEventListener("click", () => { void stopExperiment(); });
$("download").addEventListener("click", () => {
  const url = URL.createObjectURL(new Blob([serialized()], { type: "application/json" }));
  const link = document.createElement("a"); link.href = url; link.download = `cloudflare-relay-${Date.now()}.json`; link.click(); setTimeout(() => URL.revokeObjectURL(url), 10_000);
});
addEventListener("pagehide", () => { generation++; finishWaiting?.(); if (session) void closeNodes(session); });
