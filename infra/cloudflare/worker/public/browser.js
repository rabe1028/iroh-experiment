/* Browser preflight only. HTTP and WebRTC never claim a shared socket. */
"use strict";
const el = id => document.getElementById(id);
let report = null;
const badge = (id, text, tone = "neutral") => {
  el(id).textContent = text;
  el(id).dataset.tone = tone;
};
const portNumber = value => typeof value === "string" && /^\d+$/.test(value)
  && Number(value) >= 1 && Number(value) <= 65535 ? Number(value) : null;
const isH3 = value => ["3", "3.0", "HTTP/3", "HTTP/3.0", "h3"].includes(value);

async function observeHttp(current) {
  for (let i = 0; i < 5; i++) {
    badge("h3-badge", "測定中 " + (i + 1) + "/5");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 6000);
    const started = performance.now();
    let sample;
    try {
      const response = await fetch("/observe?browser=" + crypto.randomUUID(), {
        cache: "no-store", credentials: "omit", signal: controller.signal,
      });
      if (!response.ok) throw new Error("HTTP " + response.status);
      const data = await response.json();
      sample = {
        success: true,
        http_protocol: response.headers.get("x-observed-http-version") || data.http_protocol || null,
        observed_ip: response.headers.get("x-observed-ip") || data.observed_ip || null,
        observed_port: portNumber(response.headers.get("x-observed-port")),
        colo: response.headers.get("x-observed-colo") || data.colo || null,
        fetch_elapsed_ms: Math.round(performance.now() - started),
      };
      el("protocol").textContent = sample.http_protocol || "不明";
      el("ip").textContent = sample.observed_ip || "取得できず";
      el("port").textContent = sample.observed_port ?? "欠測";
      el("colo").textContent = sample.colo || "不明";
    } catch (error) {
      sample = { success: false, error: error.name === "AbortError" ? "timeout" : String(error.message) };
    } finally {
      clearTimeout(timer);
    }
    current.http_observations.push(sample);
    const item = document.createElement("li");
    const label = document.createElement("span");
    label.textContent = String(i + 1).padStart(2, "0") + " / "
      + (sample.success ? sample.http_protocol || "方式不明" : "取得できず");
    const detail = document.createElement("span");
    detail.textContent = sample.success ? (sample.colo || "—") + " · " + sample.fetch_elapsed_ms + " ms" : sample.error;
    item.append(label, detail);
    el("attempts").append(item);
    if (i < 4) await new Promise(resolve => setTimeout(resolve, 300));
  }
  const successes = current.http_observations.filter(s => s.success);
  const h3Count = successes.filter(s => isH3(s.http_protocol)).length;
  current.http3_observed = h3Count > 0;
  badge("h3-badge", h3Count ? "H3 " + h3Count + "/5" : successes.length ? "H3未確認" : "取得できず", h3Count ? "good" : "neutral");
  el("h3-status").textContent = h3Count ? "HTTP/3の接続を確認しました。" : successes.length
    ? "HTTPSは応答しましたが、今回はHTTP/3を確認できませんでした。"
    : "今回はCloudflareの観測結果を取得できませんでした。";
  el("h3-detail").textContent = "5回中 " + successes.length + " 回応答。表示のmsはfetch全体の経過時間です。"
    + (h3Count ? " これはCloudflareへの接続で、端末間のNAT越えではありません。"
      : " 接続方式はブラウザが選びます。少し待って再測定できます。UDP遮断とは断定しません。");
}

function gatherStun() {
  return new Promise(resolve => {
    let pc;
    let timer;
    let finished = false;
    const candidates = [];
    function finish(outcome, error = null) {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      if (pc) {
        pc.onicecandidate = null;
        pc.onicecandidateerror = null;
        pc.close();
      }
      resolve({ outcome, candidates, error, server: "stun:stun.cloudflare.com:3478", timeout_ms: 8000 });
    }
    if (typeof RTCPeerConnection === "undefined") { finish("unsupported"); return; }
    try {
      pc = new RTCPeerConnection({ iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }], iceCandidatePoolSize: 0 });
      pc.createDataChannel("preflight");
      pc.onicecandidate = event => {
        if (!event.candidate) { finish(candidates.length ? "observed" : "no-candidate"); return; }
        const candidate = event.candidate;
        const fields = candidate.candidate.split(/\s+/);
        const type = candidate.type || fields[7];
        const protocol = (candidate.protocol || fields[2] || "").toLowerCase();
        if (type !== "srflx" || protocol !== "udp") return;
        const address = candidate.address || fields[4];
        const port = portNumber(String(candidate.port ?? fields[5]));
        if (!address || !port) return;
        if (!candidates.some(c => c.address === address && c.port === port)) candidates.push({ type, protocol, address, port });
      };
      // ICE errors can be per-interface (e.g. IPv6 unavailable). Collect until
      // completion/deadline rather than treating the first error as failure.
      timer = setTimeout(() => finish(candidates.length ? "observed" : "timeout"), 8000);
      pc.createOffer().then(offer => {
        if (!finished) return pc.setLocalDescription(offer);
      }).catch(error => finish("error", String(error.message)));
    } catch (error) { finish("error", String(error.message)); }
  });
}

async function observeStun(current) {
  badge("stun-badge", "収集中");
  el("stun-status").textContent = "外部UDP候補を収集しています（最大8秒）。";
  current.stun = await gatherStun();
  const observed = current.stun.candidates.length > 0;
  badge("stun-badge", observed ? "UDP候補あり" : "候補未取得", observed ? "good" : "neutral");
  el("stun-status").textContent = observed ? "STUNによる外部UDP候補を取得しました。"
    : current.stun.outcome === "unsupported" ? "このブラウザではWebRTCを利用できません。"
      : "今回は外部UDP候補を取得できませんでした。";
  for (const candidate of current.stun.candidates) {
    const item = document.createElement("li");
    item.textContent = (candidate.address.includes(":") ? "[" + candidate.address + "]" : candidate.address)
      + ":" + candidate.port + " / UDP · srflx";
    el("candidates").append(item);
  }
  el("stun-detail").textContent = observed
    ? "HTTP接続とは別socketです。相手端末との接続・irohのNAT越えは未検証です。"
    : "Safari等のプライバシー設定・回線の制限などでも候補は出ません。UDP遮断やNAT越え失敗とは判定しません。カメラ・マイクの許可は求めません。";
}

el("start").addEventListener("click", async () => {
  el("start").disabled = true;
  el("download").disabled = true;
  el("network").disabled = true;
  el("start").textContent = "観測しています…";
  el("attempts").replaceChildren();
  el("candidates").replaceChildren();
  el("export-status").textContent = "";
  el("json-details").hidden = true;
  el("json-details").open = false;
  el("json-output").textContent = "";
  for (const id of ["protocol", "ip", "port", "colo"]) el(id).textContent = "—";
  badge("stun-badge", "待機中");
  el("h3-status").textContent = "HTTPの接続方式を観測しています。";
  el("h3-detail").textContent = "接続方式はブラウザが選びます。各試行は最大6秒です。";
  el("stun-status").textContent = "HTTPの観測後に、外部UDP候補を収集します。";
  el("stun-detail").textContent = "カメラ・マイクの許可は求めません。";
  report = {
    schema: "browser-network-preflight-v1", started_at: new Date().toISOString(),
    network_profile_self_reported: document.querySelector('input[name="network"]:checked').value,
    user_agent: navigator.userAgent, http_observations: [], stun: null,
    same_socket: false, iroh_peer_tested: false, nat_traversal_tested: false,
  };
  try {
    await observeHttp(report);
    await observeStun(report);
  } catch (error) {
    report.unexpected_error = String(error.message);
    el("export-status").textContent = "測定中にエラーが発生しました。取得済みの結果はJSONで保存できます。";
  } finally {
    report.completed_at = new Date().toISOString();
    el("json-output").textContent = JSON.stringify(report, null, 2);
    el("json-details").hidden = false;
    el("run-time").textContent = new Date(report.completed_at).toLocaleString("ja-JP");
    el("start").disabled = false;
    el("download").disabled = false;
    el("network").disabled = false;
    const arrow = document.createElement("span");
    arrow.textContent = "↗";
    arrow.setAttribute("aria-hidden", "true");
    el("start").replaceChildren("もう一度測る", arrow);
  }
});
el("download").addEventListener("click", () => {
  if (!report) return;
  const url = URL.createObjectURL(new Blob([JSON.stringify(report, null, 2) + "\n"], { type: "application/json" }));
  const link = document.createElement("a");
  link.href = url;
  link.download = "network-preflight-" + report.started_at.replace(/[:.]/g, "-") + ".json";
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
  el("export-status").textContent = "保存するJSONにはIPアドレスが含まれます。";
});
