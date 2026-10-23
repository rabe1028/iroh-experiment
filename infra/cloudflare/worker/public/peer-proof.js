// Pure validation used by the browser and by regression tests.
const excludedV4=[
  [0x00000000,8], // 0/8
  [0x0a000000,8], // 10/8
  [0x7f000000,8], // 127/8
  [0xe0000000,3], // multicast and reserved 224..255
  [0x64400000,10], // shared address space 100.64/10 (including Tailscale)
  [0xa9fe0000,16], // 169.254/16
  [0xac100000,12], // 172.16/12
  [0xc0a80000,16], // 192.168/16
  [0xc0000000,24], // 192.0.0/24
  [0xc0000200,24], // documentation
  [0xc6120000,15], // benchmarks
  [0xc6336400,24], // documentation
  [0xcb007100,24], // documentation
];
export function publicV4(address) {
  if(typeof address!=="string" || !/^(\d{1,3}\.){3}\d{1,3}$/.test(address))return false;
  const parts=address.split(".").map(Number);
  if(parts.some(v=>v>255))return false;
  const numeric=parts.reduce((value,part)=>value*256+part,0);
  return !excludedV4.some(([base,bits])=>Math.floor(numeric/2**(32-bits))===Math.floor(base/2**(32-bits)));
}
export function candidateInfo(candidate) {
  if(typeof candidate?.candidate!=="string")return null;
  const fields=candidate.candidate.trim().split(/\s+/);
  if(!fields[0].startsWith("candidate:") || fields[6]!=="typ")return null;
  const port=Number(fields[5]);
  if(!Number.isInteger(port) || port<1 || port>65535)return null;
  return {address:fields[4],port,type:fields[7],protocol:fields[2].toLowerCase()};
}
export function srflxCandidate(candidate) {
  const parsed=candidateInfo(candidate);
  if(!parsed)return null;
  return parsed.type==="srflx" && parsed.protocol==="udp" && publicV4(parsed.address) ? parsed : null;
}
export function signalingSdp(sdp) {
  // Only explicitly filtered IPv4 srflx candidates travel in separate messages.
  return sdp.split(/\r?\n/).filter(line=>!line.startsWith("a=candidate:") && line!=="a=end-of-candidates").join("\r\n");
}
export function selectedPath(stats) {
  const reports = [...stats.values()];
  const transport = reports.find(s=>s.type==="transport" && s.selectedCandidatePairId);
  const pair = transport ? stats.get(transport.selectedCandidatePairId)
    : reports.find(s=>s.type==="candidate-pair" && s.selected===true && s.state==="succeeded");
  // A nominated pair is not necessarily the selected pair. Missing selection
  // metadata is unknown, never inferred from a successful connection.
  if (!pair) return null;
  const local=stats.get(pair.localCandidateId), remote=stats.get(pair.remoteCandidateId);
  if (!local || !remote) return null;
  const candidate = c=>({ address:c.address ?? c.ip ?? null, port:c.port ?? null,
    type:c.candidateType ?? null, protocol:c.protocol ?? null });
  return { pair_id:pair.id, local:candidate(local), remote:candidate(remote) };
}
function differentNetworks(localCandidates,remoteCandidates) {
  const localIps=[...new Set(localCandidates.map(c=>c.address))];
  const remoteIps=[...new Set(remoteCandidates.map(c=>c.address))];
  if(!localIps.length || !remoteIps.length)return false;
  if(![...localIps,...remoteIps].every(publicV4))return false;
  return localIps.every(ip=>!remoteIps.includes(ip));
}
function observedPath(path,remoteCandidates) {
  if(!path)return false;
  if(!["host","srflx","prflx"].includes(path.local.type))return false;
  if(!["host","srflx","prflx"].includes(path.remote.type))return false;
  if(path.remote.protocol?.toLowerCase()!=="udp" || !publicV4(path.remote.address))return false;
  return remoteCandidates.some(c=>c.address===path.remote.address);
}
export function pathEvidence(before,after,localCandidates,remoteCandidates) {
  const observed=[before,after].every(path=>observedPath(path,remoteCandidates));
  const different=differentNetworks(localCandidates,remoteCandidates);
  const stable=!!before?.pair_id && before.pair_id===after?.pair_id;
  return {selected_public_ipv4_path:observed,different_stun_public_ips:different,
    selected_pair_stable:stable,qualifies_for_cross_nat:[observed,different,stable].every(Boolean)};
}
