# Cloudflare最小実験（2026-10-23更新）

STUNとH3はirohの同じUDP socketから観測できる。Fly QADやDurable Object relayは不要。
`stun-probe`は既定でsame-socket、`h3-probe --stun-server`は同じlive Endpointで両方式を比較する。
由来・transport patchは[vendor/README.md](../../vendor/README.md)を参照。

## 確認済みの範囲

- ローカルでSTUN・H3・iroh接続の実際の送信元port一致、IPv4/IPv6、並列STUN、TLS名前検証、終了時キャンセル、candidate登録を確認。
- Cloudflare STUN 3478のsame-socket観測とcandidate登録に成功。53は1秒・1回の試行で未成功（遮断の断定はしない）。
- CloudflareのH3 zone設定・実測、実NAT間でのdirect成功率比較は未実施。
- アカウントID・実domain・token・生の観測結果はcommitしない。rawは`results/raw/`へ保存。

## 仕様確認と訂正

以前の「Cloudflareにはclient source portのfieldが存在しない」という記述は撤回する。
2026-10-23に公式の[Response Header Transform対応field一覧](https://developers.cloudflare.com/rules/transform/response-header-modification/reference/fields-functions/)で、
`cf.edge.client_port`、`cf.edge.client_tcp`、`cf.timings.client_quic_rtt_msec`を確認した。
fieldの掲載は、対象zoneでの設定成功やNAT traversalへの有効性を実証するものではない。

[QUIC RTT field](https://developers.cloudflare.com/ruleset-engine/rules-language/fields/reference/cf.timings.client_quic_rtt_msec/)はミリ秒単位で、TCP時は0。
[TCP判定field](https://developers.cloudflare.com/ruleset-engine/rules-language/fields/reference/cf.edge.client_tcp/)はBoolean。
Workerの値は[Request API](https://developers.cloudflare.com/workers/runtime-apis/request/#incomingrequestcfproperties)を参照。

## 事前条件

Rustと外向きUDP（STUNは3478/53、H3は443）を用意する。
このMacではXcodeのライセンス未同意エラーを避け、利用可能なCommand Line Toolsで検証した。
同じ環境なら、以降のcargoコマンドの前にこのshellだけで選択できる。

```sh
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
cargo test --workspace --locked --offline
```

H3にはHTTP/3を有効化したCloudflare proxied hostnameと、`/observe`で2xxを返す応答元が必要。
以下のWorker例または小さな静的応答を使う。Transform Rulesにはそのzoneの編集権限が必要。
CLIは既定でIPv4のsocketを1つbindする。IPv6は`--bind '[::]:0'`で選択できる。
同じfamilyに複数socketを持つEndpointのprobeは経路が曖昧になるため拒否する。

## 1. 設定不要のsame-socket STUN観測

Cloudflareの[公開STUN](https://developers.cloudflare.com/realtime/turn/faq/)は無料・無制限と案内されている。
[接続先](https://developers.cloudflare.com/realtime/realtimekit/platform/network-allowlist/)は`stun.cloudflare.com`のUDP 3478と53。
Cloudflareアカウント・独自domain・Workerは不要。

```sh
mkdir -p results/raw
cargo run --locked -p cloudflare-stun --bin stun-probe -- \
  --server stun.cloudflare.com:3478 --server stun.cloudflare.com:53 \
  > results/raw/stun.jsonl 2> results/raw/stun.stderr
```

成功したserverごとにJSONを1行出す。同じプロセスの全serverは同じbindingを再利用する。
`status: "ok"`、`observed_addr`、`local_addr`、`rtt_ms`、`same_socket_as_iroh: true`、`candidate_registered: true`を確認する。
少なくとも1つ成功すると終了コード0、すべて失敗すると非ゼロ。serverごとの失敗はstderrに出る。
CLI終了時にはEndpointを閉じるため、出力addressを別プロセスのmedia socketへ流用しない。

診断用に`--standalone`を指定すると別UDP socketを使い、`same_socket_as_iroh: false`となる。
別々の`stun-probe`プロセスもsocketを共有しない。

## 2. H3でIPとRTTを取得する最小Worker

proxed hostnameの`/observe*`へ次のWorkerを設定し、zoneのHTTP/3を有効にする。
この例はTransform RuleなしでIP・任意のRTT・coloを返す。portは欠測となる。
コードは設定用サンプルであり、デプロイ済みのresourceではない。

```js
export default {
  async fetch(request) {
    const cf = request.cf ?? {};
    const ip = request.headers.get("cf-connecting-ip") ?? "";
    return new Response(JSON.stringify({
      observed_ip: ip,
      observed_port: null,
      rtt_ms: cf.clientQuicRtt ?? null,
      http_protocol: cf.httpProtocol ?? "",
    }), {
      headers: {
        "content-type": "application/json",
        "cache-control": "no-store",
        "x-observed-ip": ip,
        "x-observed-rtt-ms": String(cf.clientQuicRtt ?? ""),
        "x-observed-colo": cf.colo ?? "",
      },
    });
  },
};
```

`/observe*`はCache Ruleでもbypassにする。clientはJSON bodyを解析せず、response headerを読む。
この例のport欠測と、Cloudflareプラットフォーム全体でportが取得不可能という主張を混同しない。

## 3. 任意: Transform Ruleでportも返す

Worker例または静的な2xx応答を用意した同じproxed hostnameに、Response Header Transform Ruleを追加する。
既存ruleは維持し、実験用のruleを追加する。条件は実際のhostnameと`/observe`に限定する。
[作成手順](https://developers.cloudflare.com/rules/transform/response-header-modification/create-dashboard/)のSet dynamicで次を設定する。

| Response header | 式 |
|---|---|
| `x-observed-ip` | `ip.src` |
| `x-observed-port` | `to_string(cf.edge.client_port)` |
| `x-observed-is-tcp` | `to_string(cf.edge.client_tcp)` |
| `x-observed-rtt-ms` | `to_string(cf.timings.client_quic_rtt_msec)` |
| `x-observed-http-version` | `http.request.version` |

[Transform Rules](https://developers.cloudflare.com/rules/transform/)はFreeを含む各planで利用でき、proxed DNSが前提。
対象zoneで式が受理されるか、H3応答にportが付くかを確認する。APIを使う場合のphase名は`http_response_headers_transform`。
response headerだけの`cache-control`変更はCloudflare cache動作を変えないため、Cache Ruleのbypassも設定する。

現行clientはIP・port・RTT・coloを読み、TCP判定・HTTP versionが返された場合はH3と矛盾しないことを照合する。
QUIC/H3専用でTCPへfallbackしない。portは1〜65535を要求し、使用socketのfamilyと一致する完全な観測addressをirohへ登録する。

## 4. 同じlive EndpointでSTUNとH3を比較する

`observe.example.invalid`を設定済みhostnameに置き換える。
このコマンドが1つのiroh Endpointを作り、STUNを実行してから同じsocket・noq EndpointでH3接続する。

```sh
cargo run --locked -p cloudflare-h3-observer --bin h3-probe -- \
  observe.example.invalid --path /observe \
  --results results/raw/h3.jsonl --network-profile R1 \
  --stun-server stun.cloudflare.com:3478 \
  --stun-server stun.cloudflare.com:53 \
  > results/raw/h3-console.log 2> results/raw/h3.stderr
```

`--stun-server`は指定順のfallbackで、最初の成功を比較基準にする。
stdoutの`STUN_OBSERVATION`と`OBSERVATION`の`local_addr`が一致することをclient側でも検証する。
`OBSERVATION`の`same_socket_as_iroh`はtrue。IPとportが取得できれば`candidate_registered`もtrueになる。
Worker単体例ではportが欠測となり、H3 addressの登録は行わない。

`COMPARISON`は`same-ip-port-missing`、`same-ip-same-port`、`same-ip-different-port`、`different-ip`のいずれか。
共通schemaの`h3.jsonl`は比較の有無、IP/port一致、same-socket、probe全体時間、失敗理由を記録する。
edge RTTとcoloはconsole logで確認する。H3失敗時も結果行を追記し、非ゼロ終了する。
UDP遮断とDNS・TLS・zone設定不備は失敗理由を見て区別する。

従来の`--compare-stun-json`も残しているが、別プロセスのJSONとの比較になる。
同じlocal portの番号を再利用しても同じsocketとは限らないため、same-socket比較には上記のlive probeを使う。
file比較は単一JSON objectのみ対応し、複数行の`stun.jsonl`はそのまま渡せない。

## 5. Media socketへの統合

media receiver/senderにも`--stun-server`を指定できる。
receiverは候補交換の直前にmedia socketから観測し、`source: cloudflare-stun`・TTL・epochを付けて候補を返す。
senderも自身のmedia socket上で観測・登録する。control socketのmappingは使用しない。

```sh
cargo run --locked --bin media-receiver -- \
  --results results/raw/media-receiver.jsonl --network-profile R1 \
  --stun-server stun.cloudflare.com:3478 --stun-server stun.cloudflare.com:53
# 別端末。receiverが出すCONTROL_EP_IDを指定する
cargo run --locked --bin media-sender -- <CONTROL_EP_ID> \
  --results results/raw/media-sender.jsonl --network-profile R1 \
  --stun-server stun.cloudflare.com:3478 --stun-server stun.cloudflare.com:53
```

mediaは別EndpointId・relay無効のまま。controlは既存iroh基盤を使い、DO relayは未実装。
このオプションは観測とcandidate交換への統合であり、両側NATのhole punching成功を保証しない。
LANまたは到達性を確認したmappingから始め、実NAT間の接続成功率・双方向のcandidate交換は別途評価する。

## APIと実験上の制約

- STUN: `cloudflare_stun::probe::discover(&endpoint, server_addr, &config)`、またはhostnameを受ける`discover_server`。
- H3: `cloudflare_h3_observer::observe_on_endpoint(&endpoint, host, path, port, timeout)`。
- 同一Endpointを渡す。別readerの起動や`SO_REUSEPORT`による別socketのbindは行わない。
- STUNは送信元・transactionを検証し、boundedな受信channelのoverflowは観測失敗として扱う。
- 登録addressのnetwork変更時のrefresh・失効は自動化していない。呼び出し側で再観測し、古い値は`remove_external_addr`する。
- H3のlegacy `observe` helperは独立socketの診断用で、same-socketはfalse。
- mapping一致はpeerへの到達性を保証しない。Fly QAD・実peerとの比較、30/100回のmatrix、長時間media、relay byte実測は未完了。
