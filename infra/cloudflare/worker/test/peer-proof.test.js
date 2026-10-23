import assert from "node:assert/strict";
import test from "node:test";
import { srflxCandidate, signalingSdp, selectedPath, pathEvidence } from "../public/peer-proof.js";
const local=[{address:"8.8.8.8",port:1234}], remote=[{address:"1.1.1.1",port:5678}];
const path={pair_id:"selected",local:{type:"host",address:"192.168.1.2",protocol:"udp"},remote:{type:"srflx",address:"1.1.1.1",protocol:"udp"}};

test("only public IPv4 UDP srflx candidates can enter peer signaling",()=>{
  const candidate=(ip,type="srflx",protocol="udp")=>({candidate:`candidate:a 1 ${protocol} 123 ${ip} 1234 typ ${type} raddr 0.0.0.0 rport 0`});
  assert.ok(srflxCandidate(candidate("1.1.1.1")));
  for(const ip of ["192.168.1.1","10.0.0.1","100.64.0.1","127.0.0.1","::ffff:1.1.1.1","2001:4860::1","203.0.113.1","198.51.100.1","host.local","1.1.1.999"])
    assert.equal(srflxCandidate(candidate(ip)),null,ip);
  assert.equal(srflxCandidate(candidate("1.1.1.1","host")),null);
  assert.equal(srflxCandidate(candidate("1.1.1.1","relay")),null);
  assert.equal(srflxCandidate(candidate("1.1.1.1","srflx","tcp")),null);
});

test("SDP cannot smuggle LAN/overlay/IPv6 candidates around the trickle filter",()=>{
  const sdp="v=0\r\na=ice-ufrag:secret\r\na=candidate:host 1 udp 1 100.64.0.1 4567 typ host\r\na=candidate:v6 1 udp 1 2001:4860::1 1234 typ host\r\na=end-of-candidates\r\n";
  for(const input of [sdp,sdp.replaceAll("\r\n","\n")]) {
    assert.equal(signalingSdp(input),"v=0\r\na=ice-ufrag:secret\r\n");
  }
});

test("selection is obtained from transport, never inferred from nomination alone",()=>{
  const entries=[{id:"p",type:"candidate-pair",state:"succeeded",nominated:true,localCandidateId:"l",remoteCandidateId:"r"},
    {id:"l",type:"local-candidate",candidateType:"host",address:"192.168.1.2",protocol:"udp"},
    {id:"r",type:"remote-candidate",candidateType:"srflx",address:"1.1.1.1",protocol:"udp"}];
  const stats=new Map(entries.map(s=>[s.id,s]));
  assert.equal(selectedPath(stats),null);
  stats.set("t",{type:"transport",selectedCandidatePairId:"p"});
  assert.equal(selectedPath(stats).remote.address,"1.1.1.1");
});

test("echo alone, LAN, relay, missing stats, same WAN, or changing paths cannot prove cross-NAT",()=>{
  assert.equal(pathEvidence(path,path,local,remote).qualifies_for_cross_nat,true);
  const changed=structuredClone(path);changed.pair_id="different";
  assert.equal(pathEvidence(path,changed,local,remote).qualifies_for_cross_nat,false);
  for(const address of ["192.168.1.3","100.64.0.2","8.8.8.8"]){
    const other=structuredClone(path);other.remote.address=address;
    assert.equal(pathEvidence(other,other,local,remote).qualifies_for_cross_nat,false);
  }
  const relay=structuredClone(path);relay.local.type="relay";
  assert.equal(pathEvidence(relay,relay,local,remote).qualifies_for_cross_nat,false);
  assert.equal(pathEvidence(null,path,local,remote).qualifies_for_cross_nat,false);
  assert.equal(pathEvidence(path,path,local,local).qualifies_for_cross_nat,false);
  assert.equal(pathEvidence(path,path,local,[...remote,...local]).qualifies_for_cross_nat,false);
});
