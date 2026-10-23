import { candidateInfo, srflxCandidate, signalingSdp, selectedPath, pathEvidence } from "./peer-proof.js";
const el=id=>document.getElementById(id);
const CHUNK=8192, BLOCKS=8;
let current=null;
const bodyFor=index=>String.fromCharCode(65+index).repeat(CHUNK);
function event(text) {
  const item=document.createElement("li");
  item.textContent=text;
  el("events").append(item);
}
function status(text,badge) {
  el("peer-status").textContent=text;
  el("state-badge").textContent=badge;
}
function showResult(report) {
  el("json-output").textContent=JSON.stringify(report,null,2);
  el("json-details").hidden=false;
  el("download").disabled=false;
}

class Experiment {
  constructor(token) {
    this.token=token;
    this.diagnostic=["127.0.0.1","localhost","[::1]"].includes(location.hostname)
      && new URLSearchParams(location.search).get("diagnostic")==="local";
    this.local=[];this.remote=[];this.pendingCandidates=[];
    this.received=0;this.ended=false;this.dataQueue=Promise.resolve();this.signalQueue=Promise.resolve();
    this.report={schema:"browser-peer-nat-v1",transport:"webrtc-datachannel",run_id:token,
      started_at:new Date().toISOString(),
      network_profile_self_reported:document.querySelector('input[name="network"]:checked').value,
      user_agent:navigator.userAgent,turn_configured:false,ipv4_srflx_only:!this.diagnostic,diagnostic_private_peers:this.diagnostic,iroh_peer_tested:false,
      connection_success:null,echo_success:null,payload_bytes:0,nat_traversal_success:null,
      local_stun_candidates:this.local,remote_stun_candidates:this.remote,path_before:null,path_after:null};
  }
  start() {
    this.pc=new RTCPeerConnection({iceServers:[{urls:"stun:stun.cloudflare.com:3478"}],iceCandidatePoolSize:0});
    this.pc.onicecandidate=e=>{
      if(this.ended || !e.candidate)return;
      const candidate=this.acceptedCandidate(e.candidate);
      if(candidate) {
        if(!this.local.some(c=>c.address===candidate.address && c.port===candidate.port))this.local.push(candidate);
        try { this.signal({type:"candidate",candidate:e.candidate.toJSON()}); }
        catch(error) { this.fail(error.message,"signaling"); }
      }
    };
    this.pc.ondatachannel=e=>this.attach(e.channel);
    this.pc.onconnectionstatechange=()=>{
      if(this.pc.connectionState==="failed")this.fail("WebRTC接続が成立しませんでした。", "connect");
    };
    this.ws=new WebSocket(location.origin.replace(/^http/,"ws")+"/signal/"+this.token);
    this.ws.onmessage=e=>{
      this.signalQueue=this.signalQueue.then(()=>this.onSignal(JSON.parse(e.data))).catch(error=>this.fail(error.message,"signaling"));
    };
    this.ws.onerror=()=>this.fail("ルームに接続できません。期限切れ・使用済みの場合は新しく作ってください。","signaling");
    this.ws.onclose=()=>{
      if(!this.ended && this.channel?.readyState!=="open")this.fail("接続情報の交換が終了しました。新しいルームで試してください。","signaling");
    };
    this.deadline=setTimeout(()=>this.fail("ルームの有効時間が終了しました。新しく作ってください。","peer-wait"),Math.max(0,Number(this.token.slice(0,13))+600000-Date.now()));
    event("ルームに接続中 · 相手の参加待ちは最大10分");
  }
  signal(message) {
    if(this.ws.readyState!==WebSocket.OPEN)throw new Error("接続情報の交換が途切れました。");
    this.ws.send(JSON.stringify(message));
  }
  acceptedCandidate(candidate) {
    if(!this.diagnostic)return srflxCandidate(candidate);
    const parsed=candidateInfo(candidate);
    return parsed?.protocol==="udp" && ["host","srflx"].includes(parsed.type) ? parsed : null;
  }
  async onSignal(data) {
    if(this.ended)return;
    switch(data.type) {
      case "hello": this.hello(data);break;
      case "ready": await this.connect();break;
      case "offer": case "answer": await this.remoteDescription(data);break;
      case "candidate": await this.remoteCandidate(data);break;
      case "peer-left":
        if(this.channel?.readyState!=="open")this.fail("相手がルームから切断しました。新しいルームで試してください。","signaling");
        break;
      default: throw new Error("未知の接続情報です。");
    }
  }
  hello(data) {
    this.role=data.role;this.report.role=data.role;
    status("相手の端末が参加するのを待っています。","相手待ち");event("ルーム参加済み · "+data.role);
  }
  async connect() {
    clearTimeout(this.deadline);
    this.deadline=setTimeout(()=>this.fail("時間内に接続・データ往復が完了しませんでした。","deadline"),90000);
    status("STUN候補を交換し、端末間を接続しています。","接続中");
    event(this.diagnostic ? "ローカル診断 · NAT越え成功には数えません" : "2台が参加 · 公開IPv4のSTUN候補のみ交換");
    this.report.connection_success=false;
    if(this.role==="offerer") {
      this.attach(this.pc.createDataChannel("nat-echo"));
      await this.description(await this.pc.createOffer());
    }
  }
  async remoteDescription(data) {
    const expected=this.role==="answerer" ? "offer" : "answer";
    if(data.type!==expected)throw new Error("接続順序が不正です。");
    await this.pc.setRemoteDescription({type:data.type,sdp:signalingSdp(data.sdp)});
    for(const candidate of this.pendingCandidates)await this.pc.addIceCandidate(candidate);
    this.pendingCandidates=[];
    if(data.type==="offer")await this.description(await this.pc.createAnswer());
  }
  async remoteCandidate(data) {
    const parsed=this.acceptedCandidate(data.candidate);
    if(!parsed)return;
    if(!this.remote.some(c=>c.address===parsed.address && c.port===parsed.port))this.remote.push(parsed);
    if(this.pc.remoteDescription)await this.pc.addIceCandidate(data.candidate);
    else this.pendingCandidates.push(data.candidate);
  }
  async description(description) {
    await this.pc.setLocalDescription(description);
    this.signal({type:description.type,sdp:signalingSdp(this.pc.localDescription.sdp)});
  }
  attach(channel) {
    if(this.channel) { channel.close();return; }
    this.channel=channel;
    channel.onopen=()=>this.open().catch(error=>this.fail(error.message,"path"));
    channel.onmessage=e=>{
      this.dataQueue=this.dataQueue.then(()=>this.onData(e.data)).catch(error=>this.fail(error.message,"echo"));
    };
    channel.onerror=()=>this.fail("DataChannelでエラーが発生しました。","echo");
    channel.onclose=()=>{if(!this.ended)this.fail("検証完了前にDataChannelが閉じました。","echo");};
  }
  async snapshot() { return selectedPath(await this.pc.getStats()); }
  async open() {
    if(this.ended)return;
    this.report.connection_success=true;this.report.echo_success=false;
    this.report.path_before=await this.snapshot();
    if(this.ended)return;
    const ice=this.pc.sctp?.transport?.iceTransport;
    if(ice)ice.addEventListener("selectedcandidatepairchange",()=>{
      if(this.transferring && !this.ended)this.fail("転送中に経路が変わったため、判定を中止しました。","path");
    });
    status("端末間で接続しました。64KiBの往復を検証しています。","転送中");
    event("DataChannel open · 測定データは端末間のみ");
    if(this.role==="answerer")this.send({type:"ready"});
  }
  send(message) { this.channel.send(JSON.stringify(message)); }
  exchange(kind,message) {
    if(this.pending)throw new Error("検証の順序が不正です。");
    return new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>{this.pending=null;reject(new Error("データ応答がタイムアウトしました。"));},10000);
      this.pending={kind,resolve:value=>{clearTimeout(timer);this.pending=null;resolve(value);},reject:error=>{clearTimeout(timer);this.pending=null;reject(error);}};
      try { this.send(message); } catch(error) { this.pending.reject(error); }
    });
  }
  async sendPayload() {
    // Wait for the local open handler's stats snapshot before transferring.
    this.report.path_before=await this.snapshot();
    if(this.ended)return;
    this.transferring=true;
    for(let index=0;index<BLOCKS;index++) {
      const echoed=await this.exchange("echo",{type:"data",index,body:bodyFor(index)});
      if(echoed.index!==index || echoed.body!==bodyFor(index))throw new Error("往復データが一致しません。");
    }
    this.report.path_after=await this.snapshot();
    if(this.ended)return;
    this.transferring=false;
    this.report.local_evidence=pathEvidence(this.report.path_before,this.report.path_after,this.local,this.remote);
    const peer=await this.exchange("complete-ack",{type:"complete",bytes:CHUNK*BLOCKS,evidence:this.report.local_evidence});
    if(peer.bytes!==CHUNK*BLOCKS)throw new Error("相手の確認データ長が一致しません。");
    this.complete(peer.evidence);
    this.send({type:"receipt"});
    setTimeout(()=>this.cleanup(),500);
  }
  async onData(raw) {
    if(this.ended)return;
    if(typeof raw!=="string" || raw.length>10000)throw new Error("想定外のDataChannel messageです。");
    const data=JSON.parse(raw);
    if(this.pending?.kind===data.type) {this.pending.resolve(data);return;}
    switch(data.type) {
      case "ready": this.senderReady();break;
      case "data": this.echoBlock(data);break;
      case "complete": await this.receiveComplete(data);break;
      case "receipt":
        if(this.role!=="answerer" || !this.done)throw new Error("確認順序が不正です。");
        this.cleanup();break;
      default: throw new Error("想定外のデータ順序です。");
    }
  }
  senderReady() {
    if(this.role!=="offerer" || this.senderStarted)throw new Error("転送開始が不正です。");
    this.senderStarted=true;
    this.sendPayload().catch(error=>this.fail(error.message,"echo"));
  }
  echoBlock(data) {
    if(this.role!=="answerer")throw new Error("受信roleが不正です。");
    this.transferring=true;
    if(data.index!==this.received || this.received>=BLOCKS || data.body!==bodyFor(this.received))throw new Error("受信データが一致しません。");
    this.received++;
    this.send({type:"echo",index:data.index,body:data.body});
  }
  async receiveComplete(data) {
    if(this.role!=="answerer" || this.received!==BLOCKS || data.bytes!==CHUNK*BLOCKS)throw new Error("データの受信が未完了です。");
    this.report.path_after=await this.snapshot();
    if(this.ended)return;
    this.transferring=false;
    this.report.local_evidence=pathEvidence(this.report.path_before,this.report.path_after,this.local,this.remote);
    this.send({type:"complete-ack",bytes:CHUNK*BLOCKS,evidence:this.report.local_evidence});
    this.complete(data.evidence);
    setTimeout(()=>this.cleanup(),5000);
  }
  complete(peerEvidence) {
    if(this.ended)return;
    this.report.echo_success=true;this.report.payload_bytes=CHUNK*BLOCKS;
    this.report.peer_evidence=peerEvidence;
    this.report.nat_traversal_success=!this.diagnostic && this.report.local_evidence?.qualifies_for_cross_nat===true
      && peerEvidence?.qualifies_for_cross_nat===true ? true : null;
    this.report.completed_at=new Date().toISOString();
    clearTimeout(this.deadline);
    this.done=true;
    el("echo").textContent="64 KiB";
    el("path").textContent=this.report.local_evidence.selected_public_ipv4_path ? "公開IPv4" : "情報不足";
    el("nat").textContent=this.report.nat_traversal_success ? "成功" : "判定保留";
    el("state-badge").dataset.tone=this.report.nat_traversal_success ? "good" : "neutral";
    status(this.report.nat_traversal_success ? "別回線の端末間で、WebRTCのNAT越えとデータ往復を確認しました。"
      : "64KiBの往復は成功しました。別NATの証拠が揃わないため、NAT越え判定は保留です。","往復完了");
    event("64KiBの内容一致と相手の確認を受信");
    this.finishView();
  }
  fail(message,stage) {
    if(this.ended || this.done)return;
    this.report.failure_stage=stage;this.report.failure_reason=message;
    this.report.completed_at=new Date().toISOString();
    status(message,"未完了");event("終了 · "+stage);
    this.finishView();this.cleanup();
  }
  finishView() {
    el("start").disabled=false;el("join").hidden=true;el("network").disabled=false;
    showResult(this.report);
  }
  cleanup() {
    this.ended=true;clearTimeout(this.deadline);
    if(this.pending)this.pending.reject(new Error("測定を終了しました。"));
    this.channel?.close();this.pc?.close();this.ws?.close();
    if(current===this)el("stop").disabled=true;
  }
}

function begin(token) {
  current?.cleanup();
  el("events").replaceChildren();el("json-details").hidden=true;el("json-details").open=false;
  el("download").disabled=true;el("start").disabled=true;el("join").hidden=true;
  el("network").disabled=true;el("stop").disabled=false;el("export-status").textContent="";
  el("echo").textContent="—";el("path").textContent="—";el("nat").textContent="未検証";
  el("state-badge").dataset.tone="neutral";
  const url=new URL(location.href);url.hash="room="+token;
  history.replaceState(null,"",url);
  el("room-link").value=url.href;el("invitation").hidden=false;
  current=new Experiment(token);
  try { current.start(); } catch(error) { current.fail(error.message,"setup"); }
}
el("start").addEventListener("click",()=>begin(Date.now()+"-"+crypto.randomUUID()));
function refreshInvitation() {
  const token=new URLSearchParams(location.hash.slice(1)).get("room");
  if(!token || token===current?.token)return;
  if(current && !current.ended && !current.done)current.fail("別の招待リンクを開いたため、前の測定を終了しました。","navigation");
  el("join").hidden=false;
  el("invitation").hidden=true;
  status("招待リンクを開きました。「このルームに参加」を押してください。","招待あり");
}
el("join").addEventListener("click",()=>{
  const token=new URLSearchParams(location.hash.slice(1)).get("room");
  if(token)begin(token);
});
// Opening a second invitation in an existing Safari tab may change only the
// fragment; the document and module are not necessarily reloaded.
window.addEventListener("hashchange",refreshInvitation);
refreshInvitation();
el("stop").addEventListener("click",()=>{
  if(current?.done)current.cleanup();else current?.fail("ユーザーが測定を中止しました。","cancelled");
});
el("copy").addEventListener("click",async()=>{
  try {await navigator.clipboard.writeText(el("room-link").value);el("copy-status").textContent="コピーしました。もう1台の端末で開いてください。";}
  catch {el("room-link").select();el("copy-status").textContent="リンク欄を長押し・選択してコピーしてください。";}
});
el("download").addEventListener("click",()=>{
  if(!current)return;
  const url=URL.createObjectURL(new Blob([JSON.stringify(current.report,null,2)+"\n"],{type:"application/json"}));
  const link=document.createElement("a");link.href=url;link.download="peer-nat-"+current.token+".json";
  document.body.append(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),30000);
  el("export-status").textContent="JSONにはIPアドレスを含みます。相手側でも結果を保存してください。";
});
