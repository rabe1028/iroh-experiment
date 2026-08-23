# iroh Serverless NAT Traversal / Direct Media Path 実験計画書

**Status:** Draft  
**更新日:** 2026-08-23  
**対象バージョン:** iroh 1.0.3を初期基準として固定  
**想定成果物:** 公開Gitリポジトリ、再現可能な実験コード、匿名化した測定結果

---

## 1. 概要

本プロジェクトでは、インターネット側のクライアントからLAN内のHTTPサーバーやカメラへ、安全かつ低コストで接続する方法を検証する。

接続基盤にはirohを使用する。irohは可能な場合はendpoint間のdirect QUIC接続を作り、direct接続に失敗した場合はrelayへfallbackできる。初期実装ではiroh 1.0.3を固定して評価する。

この実験では、外部UDPアドレスを取得する方法として、次の3方式を比較する。

1. **Cloudflare STUN**
2. **Cloudflare HTTP/3を使った疑似QAD**
3. **Fly.io上のiroh QAD-only server**

Cloudflare Durable Objectsを使ったWebSocket relayも評価するが、用途は認証、candidate交換、接続開始などの**control planeに限定**する。

カメラ映像などの大容量データは、次の条件を必須とする。

> **direct IP pathが確立した場合だけ送信し、relay経路では絶対に送信しない。**

この条件はアプリケーションの判定だけに依存させず、control用Endpointとmedia用Endpointを分離し、media用Endpointにはrelayを設定しないことで構造的に保証する。

---

## 2. 背景

### 2.1 元の課題

LAN内に既存のHTTPサーバーやカメラが存在し、外部ネットワークから接続したい。

既存サーバーは変更しないことを基本とし、LAN内gatewayが次の処理を行う。

```text
Remote client
    │
    │ iroh QUIC
    ▼
LAN gateway
    │
    │ TCP / HTTP / HTTPS
    ▼
Existing LAN service
```

Remote側ではlocalhostにTCP listenerを作る。

```text
127.0.0.1:18080
    │
    │ iroh stream
    ▼
192.168.1.20:80
```

これにより、既存のHTTP clientはirohを認識する必要がない。

```text
curl
reqwest
hyper
Go net/http
Node.js fetch
Browser
gRPC client
WebSocket client
```

初期段階ではHTTP-aware proxyを作らず、**1つのTCP接続を1つのiroh bidirectional streamへ対応させるraw TCP tunnel**を使用する。

---

### 2.2 Relayをmedia transportに使わない理由

HTTP APIや小さなcontrol messageではrelay転送量は小さい。一方、カメラ映像をrelayすると、継続的な帯域費用とサーバー負荷が発生する。

そのため、本プロジェクトでは通信を次の2種類へ分離する。

| 種類 | Relay利用 | 主なデータ |
|---|---:|---|
| Control | 許可 | 認証、candidate交換、接続指示、状態通知 |
| Media | 禁止 | 映像、大容量ファイル、長時間stream |

---

## 3. QAD、STUN、HTTP/3の関係

### 3.1 QUIC Address Discovery

QUIC Address Discoveryはirohだけの考え方ではなく、IETF QUIC Working Groupで検討されているQUIC拡張である。ただし、2026年8月時点ではRFCではなくInternet-Draftである。

QAD serverは、受信したQUIC packetの送信元IPアドレスとUDP portを、`OBSERVED_ADDRESS` frameとしてclientへ返す。

```text
Local endpoint
192.168.1.10:42424
        │
        │ QUIC
        ▼
NAT
203.0.113.20:53124
        │
        ▼
QAD server
        │
        │ OBSERVED_ADDRESS
        ▼
"203.0.113.20:53124"
```

irohは独自の接続設定として、標準QAD portにUDP 7842を使用する。現在の`iroh-relay`はrelay転送を無効にし、QAD serverだけを有効にする構成をサポートしている。

---

### 3.2 STUN

STUNも、serverから見えた外部IPアドレスとUDP portを取得する。

```text
STUN Binding Request
        ↓
XOR-MAPPED-ADDRESS
```

目的はQADとほぼ同じだが、結果をQUIC transport frameではなくSTUN messageで返す。

本実験で最も重要な条件は、**iroh通信と同じUDP socketからSTUN requestを送ること**である。

別socketを使うと、NATが異なる外部portを割り当てる可能性がある。

```text
iroh socket :42424 → public :53124
STUN socket :42425 → public :60431
```

この場合、STUNで得た`:60431`はiroh通信には利用できない。

---

### 3.3 HTTP/3疑似QAD

Cloudflare WorkersはHTTP/3 requestを受け取れるが、raw UDP serverや独自QUIC ALPNをWorker内でlistenすることはできない。したがって、本物のiroh QAD serverをWorkerだけで実装することはできない。

一方、HTTP/3もUDP上のQUICである。irohと同じUDP socketからCloudflareへHTTP/3接続を作れば、Cloudflare Edgeが観測したclient source IPとportをresponse headerとして返せる可能性がある。

CloudflareのResponse Header Transform Rulesでは、次のfieldを利用できる。

```text
ip.src
cf.edge.client_port
cf.edge.client_tcp
cf.timings.client_quic_rtt_msec
http.request.version
```

これらはresponse headerの動的な値として利用できる。

概念的なresponseは次のようになる。

```http
HTTP/3 204 No Content
X-Observed-IP: 203.0.113.20
X-Observed-Port: 53124
X-Observed-Is-TCP: false
X-Observed-QUIC-RTT-Ms: 18
```

ただし、`cf.edge.client_port`がirohのNAT traversal用途で期待するUDP source portと完全に一致するかは、Cloudflareが保証している用途ではない。そのため、STUNおよび本物のQADと比較して実測する。

---

## 4. 目的

### 4.1 主目的

以下を実験で確認する。

1. Cloudflare STUNで得た外部アドレスを使い、irohがdirect接続できるか
2. Cloudflare HTTP/3疑似QADの結果が、STUNおよび本物のQADと一致するか
3. Fly.io上のQAD-only serverをreference implementationとして利用できるか
4. direct path確立後、映像をrelayせずに安定して転送できるか
5. direct pathが失われた場合、映像転送がrelayへfallbackせず停止するか
6. WebSocket relayのPing間隔変更により、Durable Objectのコストを削減できるか
7. raw TCP tunnelで既存HTTP libraryとの互換性を維持できるか

---

### 4.2 非目的

次の内容は、この実験の対象外とする。

- symmetric NATを含むすべてのネットワークでdirect接続を保証すること
- TURNやmedia relayによる接続保証
- ブラウザだけで動く完全なclient
- 汎用VPNまたはsubnet routerの実装
- video codec、transcode、画像認識の評価
- multi-tenant production relayの完成
- HTTP/3 origin serverのUDP forwarding

direct接続が不可能な環境では、media sessionは明示的に失敗させる。

---

## 5. 提案アーキテクチャ

```text
                         Cloudflare
             ┌─────────────────────────────┐
             │ Worker / Durable Object     │
             │                             │
             │ - authentication            │
             │ - device registration       │
             │ - candidate exchange        │
             │ - control relay             │
             └──────────────┬──────────────┘
                            │
              small control messages only
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
Remote Control Endpoint                Home Control Endpoint
          │                                   │
          └────────── rendezvous ─────────────┘


Remote Media Endpoint ═════════════════ Home Media Endpoint
                       direct QUIC
                  relay transport disabled
                                              │
                                              │ raw TCP
                                              ▼
                                      LAN HTTP / Camera
```

---

### 5.1 Control Endpoint

Control Endpointではrelayを許可する。

用途は以下に限定する。

```text
- Endpoint認証
- service authorization
- media candidate交換
- direct接続開始指示
- connection state
- bitrateやcamera選択
- small API calls
```

Control EndpointはCloudflare Durable Object上のWebSocket relay、または実験中は既存iroh relayを利用できる。

---

### 5.2 Media Endpoint

Media Endpointでは以下を必須とする。

```text
- Control Endpointとは別のEndpointId
- relay mapを設定しない
- RelayUrlをcandidateに含めない
- direct IP candidateだけを受け付ける
- direct path確認後だけstreamを開始する
- direct path消失時はstreamを停止する
```

relay側ACLでもMedia EndpointIdを拒否する。

これにより、アプリケーションのbugがあっても、Media Endpointはrelayへ接続できない。

---

### 5.3 Candidate形式

candidate交換には、少なくとも次の情報を含める。

```rust
struct DirectCandidate {
    endpoint_id: EndpointId,
    addr: SocketAddr,
    source: CandidateSource,
    observed_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    network_epoch: u64,
}

enum CandidateSource {
    Local,
    Ipv6Global,
    PortMapping,
    CloudflareStun,
    CloudflareHttp3,
    FlyIrohQad,
    Manual,
}
```

`network_epoch`はWi-Fi切り替え、interface変更、IP変更などで更新する。

古いepochのcandidateは使用しない。

---

## 6. 比較対象

### 6.1 Cloudflare STUN

Cloudflareは以下のSTUN endpointを提供している。

```text
stun.cloudflare.com:3478/udp
stun.cloudflare.com:53/udp
```

CloudflareはSTUN serviceをfree and unlimitedとしている。

#### 実装案

irohのIP transportにSTUN packetのdemultiplexerを追加する。

```text
One UDP socket
    │
    ├─ STUN packet
    │     └─ STUN probe handler
    │
    └─ QUIC packet
          └─ noq / iroh
```

STUN responseから得た`XOR-MAPPED-ADDRESS`を次のAPI相当でirohへ登録する。

```rust
endpoint.add_external_addr(observed_addr).await;
```

#### 長所

- インフラ費用が$0
- STUNは広く利用されている
- Cloudflare Anycastを利用できる
- server実装が不要
- UDP 3478が使えない場合はUDP 53を試せる

#### 短所

- same-socket STUNのため、irohまたはnoqへの変更が必要
- STUN responseは通常暗号化されない
- destination-dependent NATでは、Cloudflare向けmappingとpeer向けmappingが異なる可能性がある

---

### 6.2 Cloudflare HTTP/3疑似QAD

#### Cloudflare側

proxed subdomainを用意し、HTTP/3を有効にする。

静的assetとして空のファイルまたは小さなresponseを返す。

Response Header Transform Ruleでは、概念的に次のheaderを追加する。

```text
X-Observed-IP
  = to_string(ip.src)

X-Observed-Port
  = to_string(cf.edge.client_port)

X-Observed-Is-TCP
  = to_string(cf.edge.client_tcp)

X-Observed-QUIC-RTT-Ms
  = to_string(cf.timings.client_quic_rtt_msec)

X-Observed-HTTP-Version
  = http.request.version
```

Transform RulesはFree planを含む全planで利用でき、Free planでは10個まで有効化できる。

静的asset requestは無料かつ無制限である。したがって、既存のCloudflare proxied domainがある場合、実験用reflectorのインフラ費用は原則$0になる。

#### Client側

通常のHTTP clientを使わず、irohと同じ`noq::Endpoint`からHTTP/3接続を作る。

```text
Same noq Endpoint / UDP socket
    ├─ ALPN: iroh application protocol
    ├─ ALPN: /iroh-qad/0
    └─ ALPN: h3
```

clientは次の条件を確認する。

```text
- HTTP versionがHTTP/3
- X-Observed-Is-TCPがfalse
- observed portが1〜65535
- TLS certificateが正しい
- responseがcacheされた固定値ではない
```

#### 長所

- UDP 443を使える
- raw UDP serverを運用しなくてよい
- static assetとTransform Ruleだけなら$0
- QUIC RTTも同時に取得できる
- TLSでCloudflareまで保護される

#### 短所

- same-socket HTTP/3 clientの実装量が大きい
- `cf.edge.client_port`はNAT traversal専用の契約ではない
- Cloudflare側の実装変更に影響される可能性がある
- destination-dependent NATの問題はSTUNと同じ
- WebPKI用TLS設定とiroh用TLS設定を同じQUIC endpoint上で扱う必要がある

---

### 6.3 Fly.io QAD-only server

現在の`iroh-relay`は、relay転送を無効にし、QADのみを有効にできる。

概念的な設定は以下である。

```text
enable_relay = false
enable_quic_addr_discovery = true
```

Fly.ioでpublic UDP serviceを動かすにはDedicated IPv4が必要であり、外部portと内部portを同じにする必要がある。

```text
Dedicated IPv4
UDP 7842
    │
    ▼
iroh QAD-only process
```

#### 長所

- iroh標準QADをそのまま利用できる
- client側の変更が小さい
- reference implementationとして使える
- QADの暗号化された`OBSERVED_ADDRESS` frameを利用できる

#### 短所

- Dedicated IPv4の固定費がある
- Fly Machineの費用がある
- UDPだけで停止中Machineを確実に起動できるか検証が必要
- QAD packetのegress費用が発生する
- Cloudflare STUNより運用対象が増える

---

## 7. 仮説

### H1: Cloudflare STUNの有効性

Endpoint-Independent Mapping型NATでは、same-socket Cloudflare STUNで取得した外部アドレスはFly.io QADで取得したアドレスと一致し、同程度のdirect接続成功率を得られる。

### H2: HTTP/3疑似QADの正確性

Cloudflare HTTP/3疑似QADで取得した`ip.src:cf.edge.client_port`は、同じsocketからCloudflare STUNへ接続した際の外部アドレスと一致する。

### H3: UDP 443 fallback

UDP 3478とUDP 53が遮断され、UDP 443だけが許可されたネットワークでは、HTTP/3疑似QADがSTUNでは取得できないdirect candidateを取得できる。

### H4: Fly.io QADのreference利用

Fly.io QAD-only serverは、本物のiroh QADとして動作し、Cloudflare方式の正解確認に利用できる。

### H5: Media relay排除

Control EndpointとMedia Endpointを分け、Media Endpointでrelayを無効にすれば、direct path障害時にもmedia byteがrelayへ流れない。

### H6: Durable Objectコスト削減

iroh relay protocolのPing間隔を長くすると、Durable Objectへ届くWebSocket message数とwake回数を削減できる。

現在のiroh relay protocolでは15秒のPing intervalが設定されている。

ただし、QUIC connectionやpath側にも別のheartbeatが存在する可能性があるため、relay PingだけでなくDurable Objectが実際に受信した全messageを計測する。

---

## 8. 実験環境

### 8.1 Endpoint

最低2つのendpointを用意する。

```text
Endpoint A: LAN gateway role
Endpoint B: Remote client role
```

必要に応じて第三のobserver endpointを追加する。

各実験runでは、次を固定または記録する。

```text
- iroh version
- noq version
- Rust toolchain
- OS
- CPU architecture
- local UDP port
- enabled discovery methods
- relay configuration
- network profile
- git commit
```

---

### 8.2 Network profile

#### Synthetic profile

Linux network namespace、nftables、traffic controlを使い、次を再現する。

| ID | Network |
|---|---|
| L0 | 同一LAN、NATなし |
| N1 | Global IPv6 |
| N2 | Single IPv4 NAT、port preserving |
| N3 | Single IPv4 NAT、port randomized |
| N4 | Double NAT |
| N5 | Address-dependent mapping |
| N6 | Address-and-port-dependent mapping |
| N7 | UDP 3478 blocked、UDP 53 allowed |
| N8 | UDP 3478/53 blocked、UDP 443 allowed |
| N9 | すべての外向きUDP blocked |
| N10 | Packet loss / latency injection |

追加conditionとして次を重ねる。

```text
RTT: 20ms / 100ms / 250ms
Loss: 0% / 1% / 5%
Reordering: 0% / 1%
MTU: 1280 / 1400 / 1500
```

#### Real network profile

実ネットワークでも確認するが、公開結果ではprovider名、場所、SSIDなどを公開しない。

```text
R1: residential IPv4 NAT
R2: residential IPv6
R3: mobile CGNAT
R4: public Wi-Fi
R5: enterprise-style restricted network
```

---

## 9. 実験項目

## E0. Baseline接続

### 目的

通常のiroh接続が、relayからdirect pathへ移行する過程を確認する。

### 手順

1. 公式iroh relayまたは通常のself-host relayを設定する
2. Endpoint AとBを起動する
3. relay経由でconnectionを開始する
4. direct pathへの切り替えを待つ
5. path eventと転送byteを記録する
6. 10MBのテストデータを転送する

### 記録

```text
- relay connection開始時刻
- first QUIC handshake時刻
- direct path確立時刻
- selected path
- relay tx/rx bytes
- direct tx/rx bytes
- RTT
```

---

## E1. Fly.io QAD-only reference

### 目的

本物のiroh QADから得られる外部アドレスをreference valueにする。

### 手順

1. Fly.ioへQAD-only serverをdeployする
2. Dedicated IPv4を割り当てる
3. UDP 7842を公開する
4. relay転送が無効であることを確認する
5. Endpoint A、Bの両方からQADを実行する
6. observed addressを記録する
7. candidateを交換してdirect接続を試す

### Always-on試験

Machineを常時起動し、次を測定する。

```text
- QAD success rate
- QAD latency
- response bytes
- CPU / memory
- monthly cost projection
```

### Autostart試験

Fly Proxyではserviceに`auto_start_machines`と`auto_stop_machines`を設定でき、service protocolにはUDPも指定できる。ただし、停止中Machineへの最初のUDP datagramが期待どおり起動を引き起こすかは、この実験で確認する。

試験caseは次の通り。

```text
A. Machine停止中にUDP QADだけを送る
B. UDP QADを1秒間隔で5回送る
C. HTTPS wake request後にUDP QADを送る
D. suspended stateからUDP QADを送る
```

---

## E2. Same-socket Cloudflare STUN

### 目的

same-socket STUNから取得した外部アドレスでdirect接続できるか確認する。

### 実装

次のinterfaceを用意する。

```rust
#[async_trait]
trait ExternalAddrProbe {
    async fn probe(
        &self,
        context: &ProbeContext,
    ) -> Result<ExternalAddrObservation>;
}

struct ExternalAddrObservation {
    method: ProbeMethod,
    addr: SocketAddr,
    rtt: Duration,
    observed_at: SystemTime,
}
```

`ProbeContext`はirohのUDP transportと同じ送受信経路を利用する。

### Probe順序

```text
1. stun.cloudflare.com:3478/udp
2. stun.cloudflare.com:53/udp
```

### 検証

各runで次を比較する。

```text
Cloudflare STUN addr
Fly.io QAD addr
actual direct peer source addr
```

---

## E3. Cloudflare HTTP/3疑似QAD

### 目的

Cloudflare Edgeが観測したHTTP/3 source portをdirect candidateとして利用できるか確認する。

### Cloudflare設定

```text
observe.example.invalid
    ├─ proxied DNS
    ├─ HTTP/3 enabled
    ├─ static asset
    └─ Response Header Transform Rule
```

公開リポジトリでは実domainを記載せず、`.invalid`または`.example`を使う。

### Client実装

1. irohと同じ`noq::Endpoint`を利用する
2. WebPKI TLS client configを用意する
3. ALPN `h3`でCloudflareへ接続する
4. GET `/observe`を送る
5. response headerからIP、port、RTTを取得する
6. HTTP/3以外へfallbackした場合は結果を無効とする
7. external candidateとしてirohへ追加する

### 検証項目

```text
H3 observed address == STUN observed address
H3 observed address == Fly QAD observed address
H3 observed addressでdirect connection成功
```

特に次を分けて記録する。

```text
same IP, same port
same IP, different port
different IP
missing port
TCP fallback
```

---

## E4. Discovery fallback chain

### 目的

複数方式を組み合わせた場合の接続成功率と接続時間を測る。

比較する戦略は次の通り。

```text
Strategy A:
  Fly QAD only

Strategy B:
  STUN 3478
  → STUN 53
  → Fly QAD

Strategy C:
  STUN 3478
  → STUN 53
  → HTTP/3
  → Fly QAD

Strategy D:
  STUN 3478、HTTP/3、Fly QADを並列実行
  → 最初の有効candidateを利用
```

順次実行と並列実行の両方を比較する。

並列実行では外部serviceへのpacket数が増えるため、成功率だけでなくprobe traffic量も計測する。

---

## E5. Direct-only Media Endpoint

### 目的

映像や大容量dataがrelayへ流れないことを確認する。

### 手順

1. Control connectionを確立する
2. Media Endpointのdirect candidateを交換する
3. Media Endpointにはrelayを設定しない
4. direct path確立を確認する
5. synthetic media streamを開始する
6. direct pathを故意に切断する
7. media transferが停止することを確認する
8. relay側にmedia byteが存在しないことを確認する

### Traffic profile

```text
5 Mbps   × 60 minutes
20 Mbps  × 60 minutes
50 Mbps  × 15 minutes
```

追加でburst trafficを試す。

```text
100 Mbps × 60 seconds
```

### Fault injection

```text
- Wi-Fi interface down
- public IP change
- NAT mapping reset
- direct UDP block
- 5% packet loss
- relayだけ到達可能な状態
```

---

## E6. LAN HTTP compatibility

### 目的

既存のHTTP libraryやLAN serverを変更せず利用できるか確認する。

### 構成

```text
Remote localhost TCP listener
        │
        │ iroh bidirectional stream
        ▼
LAN gateway
        │
        │ TCP
        ▼
LAN service
```

### 対象

| Protocol | Test |
|---|---|
| HTTP/1.1 | GET、POST、chunked response |
| HTTPS | TLS passthrough、SNI、certificate hostname |
| HTTP/2 | TLS ALPN passthrough |
| WebSocket | Upgrade後の双方向通信 |
| SSE | 長時間response |
| gRPC | HTTP/2とtrailer |
| Raw TCP | echo、large transfer |

HTTPSではlocalhost名ではなく、元のhostnameを維持するlocal DNSまたはproxy設定を試す。

HTTP/3 originはUDP forwardingが必要なため、この段階では対象外とする。

---

## E7. Cloudflare Durable Object relayとPing間隔

### 目的

WebSocket relayをcontrol planeとして運用する場合のidle costを測る。

Durable ObjectsはWebSocket Hibernationに対応し、hibernate可能なidle objectはduration課金されない。Incoming WebSocket messageはbilling上20 messagesを1 requestとして計算し、outgoing messageはrequest課金されない。

### Ping interval

次を比較する。

```text
15 seconds
30 seconds
60 seconds
120 seconds
300 seconds
```

### 100 endpointの場合の理論値

1か月を30日とする。

```text
billing requests
≈ endpoints
  × 2,592,000 seconds
  ÷ ping interval
  ÷ 20
```

| Interval | Incoming messages / month | Billing requests / month |
|---:|---:|---:|
| 15秒 | 17,280,000 | 864,000 |
| 60秒 | 4,320,000 | 216,000 |
| 120秒 | 2,160,000 | 108,000 |
| 300秒 | 864,000 | 43,200 |

ただし実際には、relay protocol Ping以外のQUIC packet、reconnect、network reportなどが存在する可能性がある。したがって理論値ではなく、Durable Object metricsを正とする。

### 記録

```text
- incoming WebSocket messages
- outgoing messages
- DO wake count
- active duration
- hibernation duration
- reconnect count
- connection detection latency
- estimated monthly cost
```

---

## 10. 測定指標

### 10.1 Discovery

```text
probe_success
probe_latency_ms
observed_ip_equal_to_reference
observed_port_equal_to_reference
observed_addr_stability
bytes_sent
bytes_received
```

### 10.2 Direct connection

```text
direct_connection_success
time_to_first_direct_path_ms
direct_path_rtt_ms
path_changes
hole_punch_attempts
```

### 10.3 Relay

```text
relay_control_tx_bytes
relay_control_rx_bytes
relay_media_tx_bytes
relay_media_rx_bytes
relay_connection_count
relay_reconnect_count
```

### 10.4 Media

```text
media_duration_seconds
media_target_bitrate_mbps
media_actual_bitrate_mbps
media_packet_loss
media_stall_count
media_disconnect_time_ms
```

### 10.5 Infrastructure

```text
Cloudflare Worker requests
Durable Object billing requests
Durable Object active GB-s
Fly Machine active seconds
Fly Machine stopped rootfs GB-month
Fly egress bytes
```

---

## 11. Result schema

公開するrun結果は、次のようなJSONにする。

```json
{
  "schema_version": 1,
  "run_id": "synthetic-n2-stun-0001",
  "timestamp": "2026-08-23T00:00:00Z",
  "git_revision": "0000000",
  "iroh_version": "1.0.3",
  "method": "cloudflare-stun-3478",
  "network_profile": "N2",
  "reference_method": "fly-iroh-qad",
  "observed_ip_equal": true,
  "observed_port_equal": true,
  "probe_latency_ms": 24,
  "direct_connection_success": true,
  "time_to_direct_ms": 418,
  "selected_path": "direct-ip",
  "relay_control_tx_bytes": 2048,
  "relay_control_rx_bytes": 4096,
  "relay_media_tx_bytes": 0,
  "relay_media_rx_bytes": 0,
  "media_throughput_mbps": 19.8,
  "failure_reason": null
}
```

public IPそのものは保存しない。

---

## 12. 実験回数

探索段階では各cellを30回実行する。

```text
method × network profile × 30 runs
```

最終比較ではbinaryなdirect successについて100回実行する。

```text
method × network profile × 100 runs
```

Media試験は次を基本とする。

```text
短時間試験: 各条件10回
60分試験: 各主要条件2回以上
fault injection: 各fault 20〜50回
```

success rateにはWilson confidence intervalを付ける。

実験順序はrandomizeし、時間帯やネットワーク状態の偏りを減らす。

Synthetic NATではrunごとにnamespaceとNAT stateを再作成する。

---

## 13. 合格基準

### 13.1 Cloudflare STUN

Cloudflare STUNは、次を満たせば主方式の候補とする。

```text
- Endpoint-Independent Mapping profileで
  Fly QADとの差が2 percentage points以内

- observed address不一致率が1%未満

- P95 probe latencyがFly QAD以下、
  または差が100ms以内

- same-socket実装が安定し、
  QUIC packet処理を壊さない
```

---

### 13.2 HTTP/3疑似QAD

次を満たせばUDP 443 fallbackとして採用する。

```text
- cf.edge.client_portが
  STUNまたはFly QADのobserved portと一致する

- TCP fallbackを確実に検出できる

- STUN 3478/53が使えないprofileで
  direct successを追加できる

- false candidateにより
  direct接続時間を大幅に悪化させない
```

具体的には、STUNだけでは失敗するrunのうち、HTTP/3追加によって5%以上のrunをdirect接続へ移せれば、有効なfallbackと判断する。

---

### 13.3 Fly.io QAD

次を満たせばreference serverとして採用する。

```text
- QAD success rate 99%以上
- always-on時のP95 latencyが1秒未満
- relay trafficが0
- QAD-only configurationが再現可能
```

Autostart構成は次を満たす場合だけserverless optionとして扱う。

```text
- 停止状態から自動で起動できる
- retry込みP95 discovery timeが5秒以内
- manual API startが不要
```

満たさない場合はalways-on reference serverとして扱う。

---

### 13.4 Media

Media Endpointの必須条件は次の通り。

```text
relay_media_tx_bytes == 0
relay_media_rx_bytes == 0
```

これはすべての正常系、障害系runで満たす必要がある。

1回でもmedia byteがrelayで観測された場合、設計不合格とする。

direct path消失後は、media transferを2秒または2 heartbeat以内に停止する。

---

### 13.5 HTTP互換性

次のprotocol testがすべて通ればraw TCP tunnelを採用する。

```text
HTTP/1.1
HTTPS
HTTP/2
WebSocket
SSE
gRPC
Raw TCP
```

HTTP-aware translationは、raw tunnelでは実現できない認可やroutingが必要になった場合だけ追加する。

---

## 14. 運用コスト

料金は2026年8月23日時点の公開料金を使う。domain取得費用、税、開発者の人件費は含めない。

### 14.1 比較

| 方式 | 最低月額 | 小規模運用 | 主な従量費 |
|---|---:|---:|---|
| Cloudflare STUN | $0 | $0 | なし |
| Cloudflare H3 static asset | $0 | $0 | なし |
| Cloudflare H3 Worker Free | $0 | $0 | 100,000 request/day上限 |
| Cloudflare H3 Worker Paid | $5 | $5〜 | request・CPU超過 |
| Fly.io QAD always-on | 約$3.94〜$4.54 | 約$4〜$5 | egress |
| Fly.io QAD stopped | 約$2＋rootfs | workload依存 | 起動時間・egress |
| Cloudflare DO control relay | $0 Free / $5 Paid | workload依存 | request・active duration |

Cloudflare Workers Freeは1日100,000 requestまでで、Paidは月額最低$5、月10 million requestsと30 million CPU millisecondsを含む。

Fly.ioのDedicated IPv4は月$2である。`shared-cpu-1x / 256MB`はregionにより概ね月$1.94〜$2.54であり、always-on QADの固定費は約$3.94〜$4.54になる。停止中Machineはrootfs 1GBあたり月$0.15で、Asia Pacificのpublic internet egressは$0.04/GBである。

---

### 14.2 Scale例

1 deviceあたり1日10回probeするとする。

| Devices | Probes / month | CF STUN | CF H3 static | CF Worker | Fly QAD |
|---:|---:|---:|---:|---:|---:|
| 100 | 30,000 | $0 | $0 | Free内 | 約$4〜 |
| 1,000 | 300,000 | $0 | $0 | Free内 | 約$4〜 |
| 10,000 | 3,000,000 | $0 | $0 | Free上限付近 | 約$4〜＋egress |
| 100,000 | 30,000,000 | $0 | $0 | Paid約$11＋CPU超過 | 約$4〜＋egress |

Fly.ioのegressは、実際のQAD packet captureから1 probeあたりのbyte数を計測して再計算する。

```text
monthly egress cost
= probes
  × response bytes per probe
  ÷ 1GB
  × region egress rate
```

---

## 15. 開発コスト

以下は、Rust、QUIC、Cloudflareを扱えるengineer 1名を想定した粗い見積もりである。

| Work item | Engineering days |
|---|---:|
| Repository、CI、共通telemetry | 4〜7日 |
| Baseline iroh tunnel | 2〜4日 |
| Fly.io QAD-only deployment | 1〜3日 |
| Same-socket STUN integration | 5〜10日 |
| HTTP/3疑似QAD client | 7〜15日 |
| Cloudflare rules / static asset | 1〜2日 |
| Control / Media Endpoint分離 | 4〜7日 |
| Direct-only media test | 3〜6日 |
| Durable Object relay PoC | 7〜15日 |
| Synthetic NAT test environment | 5〜10日 |
| Analysis、graphs、documentation | 3〜6日 |

### MVP

次を含むMVPは、概ね15〜25 engineering daysと見積もる。

```text
- Fly QAD reference
- Cloudflare STUN
- direct-only media endpoint
- raw TCP HTTP tunnel
- basic synthetic NAT tests
```

### Full experiment

HTTP/3疑似QAD、Durable Object relay、広いNAT matrixまで含めると、概ね30〜55 engineering daysと見積もる。

金額換算する場合は、次の式を使う。

```text
development cost
= engineering days × daily engineering rate
```

---

## 16. Repository構成

```text
.
├── Cargo.toml
├── README.md
├── EXPERIMENT_PLAN.md
├── LICENSE-APACHE
├── LICENSE-MIT
│
├── crates/
│   ├── common/
│   ├── control-protocol/
│   ├── tcp-tunnel/
│   ├── media-sender/
│   ├── media-receiver/
│   ├── external-addr-probe/
│   ├── cloudflare-stun/
│   ├── cloudflare-h3-observer/
│   └── experiment-runner/
│
├── infra/
│   ├── cloudflare/
│   │   ├── worker/
│   │   ├── durable-object/
│   │   └── terraform/
│   │
│   └── fly/
│       ├── fly.toml
│       └── relay-config.example.toml
│
├── experiments/
│   ├── netns/
│   ├── nat-profiles/
│   ├── media/
│   ├── http-compat/
│   └── relay-cost/
│
├── scripts/
│   ├── run-matrix.sh
│   ├── sanitize-results.py
│   └── aggregate-results.py
│
├── schemas/
│   └── experiment-result.schema.json
│
├── results/
│   ├── raw.example/
│   └── public/
│
└── docs/
    ├── architecture.md
    ├── protocol.md
    ├── cost-model.md
    └── findings.md
```

---

## 17. Pull Request単位の進め方

### PR 1: Baseline

```text
- iroh 1.0.3 pin
- echo protocol
- path telemetry
- JSON result schema
```

### PR 2: Raw TCP tunnel

```text
- local TCP listener
- service ID routing
- HTTP compatibility tests
```

### PR 3: Fly QAD-only

```text
- infrastructure config
- always-on measurement
- autostart experiment
```

### PR 4: Same-socket STUN

```text
- STUN codec
- UDP demultiplex
- Cloudflare STUN probes
- add_external_addr integration
```

### PR 5: HTTP/3 observer

```text
- same-endpoint H3 connection
- Cloudflare Transform Rule
- STUN / QAD comparison
```

### PR 6: Media separation

```text
- separate EndpointIds
- relay disabled for media
- fail-closed state machine
- synthetic stream
```

### PR 7: Durable Object relay

```text
- WebSocket hibernation
- relay handshake
- EndpointId routing
- Ping interval experiment
```

### PR 8: Final report

```text
- aggregate data
- confidence intervals
- cost comparison
- recommendation
```

---

## 18. Public repositoryの情報管理

### 18.1 公開しない情報

以下はcommitしない。

```text
- raw public IP addresses
- private IP addresses from real networks
- SSID
- provider name
- physical location
- Cloudflare account ID
- Cloudflare API token
- Fly API token
- TLS private key
- iroh SecretKey
- long-lived EndpointId
- raw packet capture
- raw qlog containing addresses
```

### 18.2 匿名化

公開結果では、IPアドレスそのものではなく比較結果を保存する。

```text
observed_ip_equal: true
observed_port_equal: false
```

address単位で追跡が必要な場合は、非公開saltを使ったHMACへ変換する。

```text
HMAC-SHA256(secret_session_salt, socket_address)
```

saltはcommitしない。

### 18.3 Ephemeral identity

実験runごと、または実験sessionごとに新しいEndpoint secretを生成する。

公開結果にはEndpointIdを直接入れず、run-local labelを使う。

```text
endpoint-a
endpoint-b
observer-1
```

---

## 19. Security要件

LAN gatewayは任意のhostとportを受け付けるopen proxyにしない。

```text
service_id: "camera-ui"
    → 192.168.10.20:80

service_id: "home-api"
    → 192.168.10.30:8080
```

認可は次の組み合わせで行う。

```text
Remote EndpointId
×
Service ID
×
Short-lived capability
```

STUN responseではtransaction IDを検証する。

HTTP/3 observerではCloudflareのTLS certificateを検証する。

Control relayでは接続元EndpointIdのsecret key ownershipを確認する。

Media Endpointではrelay transportをcompile-timeまたはconfiguration-timeに無効にする。

---

## 20. 主なリスク

### 20.1 Destination-dependent NAT

STUN、HTTP/3、QADで得たmappingがpeer向けmappingと異なる可能性がある。

対策として、address一致だけでなく実際のdirect接続成功率を重視する。

---

### 20.2 HTTP/3 source portの意味

`cf.edge.client_port`が期待するUDP source portであるか、CloudflareのNAT traversal向け仕様として保証されていない。

STUNおよびFly QADとの比較を必須にする。

---

### 20.3 same-socket実装

irohの公開APIだけでは、同じUDP socketから任意のSTUN packetやHTTP/3 connectionを作ることが難しい可能性がある。

次の順で対応する。

```text
1. public extension pointで実装
2. noq layerへ小さなhookを追加
3. iroh fork
4. upstream proposal
```

forkする場合はcommitを固定し、patchを小さく保つ。

---

### 20.4 Fly.io autostart

UDP packetのみでMachineが期待どおり起動しない可能性がある。

その場合は次を比較する。

```text
- always-on QAD
- HTTPS wake + QAD
- suspended Machine
```

---

### 20.5 Ping intervalの延長

Pingを長くするとDurable Objectコストは下がるが、proxy、firewall、NATのidle timeoutで接続が切れやすくなる可能性がある。

15、30、60、120、300秒を実測し、reconnect countと検出時間で判断する。

---

### 20.6 Relayへの意図しないmedia送信

同じiroh connectionにcontrolとmediaを混在させると、relay側は暗号化されたpayloadの種類を判別できない。

そのため、Media Endpointは必ず別EndpointIdとする。

---

## 21. 最終的な選択ルール

実験結果から次の順で判断する。

### 21.1 Cloudflare STUNを主方式にする条件

```text
- Fly QADと同程度のdirect success
- same-socket実装が安定
- 追加インフラ費用$0
```

### 21.2 HTTP/3疑似QADを追加する条件

```text
- STUN blocked環境で接続成功率を改善
- source portがreferenceと一致
- false candidateの影響が小さい
```

### 21.3 Fly.io QADをproductionで残す条件

```text
- Cloudflare方式の成功率がQADより大きく低い
- same-socket client変更の保守負担が高い
- 月約$4〜$5の固定費が許容可能
```

### 21.4 Media transferの条件

```text
if direct_path_is_verified {
    start_media();
} else {
    return DirectPathRequired;
}
```

direct path消失時は次の動作とする。

```text
stop_media();
close_media_connection();
notify_control_endpoint();
```

relayへのfallbackは行わない。

---

## 22. 期待する最終構成

現時点で最も有力な構成は次である。ただし、採用は実験結果で決定する。

```text
Address discovery:
  1. Global IPv6
  2. PCP / NAT-PMP / UPnP / manual mapping
  3. Cloudflare STUN 3478
  4. Cloudflare STUN 53
  5. Cloudflare HTTP/3 observer
  6. Fly.io iroh QAD

Control:
  Cloudflare Durable Object WebSocket relay
  hibernation enabled
  extended Ping interval

Media:
  separate iroh Endpoint
  no relay configuration
  direct IP path only

LAN access:
  raw TCP tunnel
  existing HTTP libraries unchanged
```

---

## 23. 実験終了時の成果物

実験終了時に、公開リポジトリへ次を追加する。

```text
- 再現可能なsource code
- Cloudflare infrastructure definition
- Fly.io deployment definition
- Synthetic NAT environment
- JSON Schema
- 匿名化したraw result
- 集計CSV
- success rate graph
- latency distribution
- monthly cost model
- final recommendation
- known limitations
```

最終reportでは、少なくとも次を明示する。

```text
- 採用したaddress discovery方式
- fallback順序
- direct success rate
- direct接続までの時間
- relay control traffic
- relay media trafficが0である証拠
- 月額運用費
- client patchの保守範囲
- 接続できないnetwork condition
```