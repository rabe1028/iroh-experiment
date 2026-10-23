import assert from "node:assert/strict";
import test from "node:test";
import { SignalingRoom, roomExpiry, ROOM_TTL_MS } from "../src/signal-room.js";
const token=now=>`${now}-12345678-1234-4321-8321-123456789abc`;
function socket(role="offerer") {
  return { info:{role,expires:Date.now()+60000,messages:0,bytes:0,sdpSent:false},sent:[],
    deserializeAttachment(){return structuredClone(this.info);},serializeAttachment(info){this.info=info;},
    send(data){this.sent.push(JSON.parse(data));},close(code){this.closed=code;} };
}

test("room capability expires and cannot be recreated by reusing an old URL",()=>{
  const now=Date.now();
  assert.equal(roomExpiry(token(now),now),now+ROOM_TTL_MS);
  assert.equal(roomExpiry(token(now),now+ROOM_TTL_MS),null);
  assert.equal(roomExpiry(token(now+31000),now),null);
  for(const invalid of ["guess",token(now)+"/other",token(now).replace("4321","1321")])assert.equal(roomExpiry(invalid,now),null);
});

test("only SDP for the assigned role and bounded ICE messages are forwarded",()=>{
  const a=socket(),b=socket("answerer");
  const room=new SignalingRoom({getWebSockets:()=>[a,b]});
  const offer={type:"offer",sdp:"v=0\r\na=ice-ufrag:demo\r\n"};
  room.webSocketMessage(a,JSON.stringify(offer));
  assert.deepEqual(b.sent,[offer]);
  assert.equal(a.sent.length,0);
  room.webSocketMessage(a,JSON.stringify(offer));
  assert.equal(a.closed,1008);
  const wrong=socket("answerer");room.webSocketMessage(wrong,JSON.stringify(offer));
  assert.equal(wrong.closed,1008);
});

test("data payloads, expired connections, excessive size and flooding cannot use the signaling relay",()=>{
  const messages=[JSON.stringify({type:"data",body:"payload"}),"x".repeat(24577),"not-json"];
  for(const message of messages){const a=socket(),b=socket();new SignalingRoom({getWebSockets:()=>[a,b]}).webSocketMessage(a,message);assert.equal(a.closed,1008);assert.equal(b.sent.length,0);}
  const expired=socket();expired.info.expires=0;
  new SignalingRoom({getWebSockets:()=>[expired]}).webSocketMessage(expired,JSON.stringify({type:"candidate",candidate:{candidate:"candidate:test"}}));
  assert.equal(expired.closed,1008);
  const flooded=socket();flooded.info.messages=32;
  new SignalingRoom({getWebSockets:()=>[flooded]}).webSocketMessage(flooded,JSON.stringify({type:"candidate",candidate:{candidate:"candidate:test"}}));
  assert.equal(flooded.closed,1008);
});

test("room alarm closes signaling and removes ephemeral room storage",async()=>{
  const a=socket(),b=socket();let deleted=false;
  await new SignalingRoom({getWebSockets:()=>[a,b],storage:{deleteAll:async()=>{deleted=true;}}}).alarm();
  assert.equal(a.closed,1000);assert.equal(b.closed,1000);assert.equal(deleted,true);
});


test("two admissions exhaust a room even after both sockets disconnect",async()=>{
  const state={storage:{transaction:async callback=>callback({get:async()=>2})}};
  const request=new Request("https://observe.example.com/signal/"+token(Date.now()),{headers:{upgrade:"websocket"}});
  assert.equal((await new SignalingRoom(state).fetch(request)).status,409);
  const old=new Request("https://observe.example.com/signal/"+token(Date.now()-ROOM_TTL_MS),{headers:{upgrade:"websocket"}});
  assert.equal((await new SignalingRoom(state).fetch(old)).status,410);
});
