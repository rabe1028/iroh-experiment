# Cloudflare最小実験（2026-10-23更新）

STUNとH3はirohの同じUDP socketから観測できる。Fly QADやDurable Object relayは不要。
`stun-probe`は既定でsame-socket、`h3-probe --stun-server`は同じlive Endpointで両方式を比較する。
由来・transport patchは[vendor/README.md](../../vendor/README.md)を参照。

## 確認済みの範囲

- ローカルでSTUN・H3・iroh接続の実際の送信元port一致、IPv4/IPv6、並列STUN、TLS名前検証、終了時キャンセル、candidate登録を確認。
- Cloudflare STUN 3478のsame-socket観測とcandidate登録に成功。53は1秒・1回の試行で未成功（遮断の断定はしない）。
- Cloudflareの実zoneへWorkerを配置し、port返却Transform RuleとCache bypassを有効化。既存のHTTP/3有効設定を確認。
- 公開DNSで解決したedge IPを明示したH3単独probeに成功。同じiroh socketでIP・portを観測しcandidate登録を確認。実NAT間のdirect接続は未検証。
- その後の予備実験では通常DNSが成功。live比較はIPv4 4/5回、IPv6 3/3回完了し、完了した7回はIP・portが一致。IPv4の1回はSTUN timeoutでH3未実行。
- STUN単独3478は成功とtimeoutが混在。53は両modeとも各3runで応答なし。詳細と制約は[予備測定結果](../../results/2026-10-23-preflight.md)。
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

## 2. 観測Workerの準備・デプロイ

`worker/`にデプロイ可能なWorkerとWranglerの固定依存を用意した。Node.js 22以上を使う。
実験専用の未使用サブドメインを選ぶ。既存サイトのhostnameは使わない。
Workerは`GET /observe`と`HEAD /observe`だけを受け付け、IP・任意のQUIC RTT・colo・HTTP versionをheaderに返す。
portは次節のTransform Ruleで付ける。clientはJSON bodyではなくresponse headerを読む。

```sh
cd infra/cloudflare/worker
npm ci
npm test
npm run check                 # 認証不要のデプロイdry-run
npm run login                 # ブラウザでCloudflareの認証・許可
npm run whoami
npm run configure -- --hostname observe.example.invalid \
  --account-id <ACCOUNT_ID> --zone-id <ZONE_ID>
npm run deploy
```

`configure`は`wrangler.local.json`と`zone-plan.local.json`を生成するだけで、remote変更は行わない。
実domain・IDを含む両file、Wrangler作業用file、依存はGit対象外。
Workerは[Custom Domain](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/)で配置し、DNSと証明書はCloudflareが管理する。
`workers.dev`・preview URLは無効。Worker内の永続logging・storage・外部fetchは使わない。
HTTP/3の有効化はzone全体の設定。以下のruleはhostnameと`/observe`に限定する。

```sh
npm run setup-zone            # ローカルのplanを表示。remote変更なし
```

通常のWrangler OAuthログインは、zoneのTransform/Cache/HTTP3設定の編集権限を含まない。
Dashboardで次節のruleとCache bypass、HTTP/3を設定するか、対象zoneに限定したAPI tokenで自動設定する。
必要な権限はZone Read、Zone Settings Edit、Zone Transform Rules Edit、Cache Settings Edit。
tokenはチャットや引数に貼らず、ローカル環境変数`CLOUDFLARE_API_TOKEN`に設定する。

```sh
npm run setup-zone -- --apply
```

自動設定はzoneの所属・既存phaseを先に確認し、HTTP/3をonにする。
既存ruleset全体の置換はせず、hostname由来の`ref`で自分のruleだけを追加・更新する。
Dashboard作成ruleも同名・同条件・同actionなら引き継ぎ、`ref`を付けて更新する。
初回の設定前snapshotは`zone-<ZONE_ID>-before.local.json`へ保存し、再実行時も上書きしない。
APIエラー時は途中まで設定済みの場合がある。Dashboardで確認・修正後に再実行できる。
Free planを含む対象zoneで式が受理されるかは、実際のAPI応答・H3測定で確認する。

認証後のlive smoke testはrepo rootからのcargoコマンド（第4節）か、次のwrapperで実行する。

```sh
# このMacでCommand Line Toolsを使う場合
export DEVELOPER_DIR=/Library/Developer/CommandLineTools
npm run probe -- --network-profile R1
# STUNに依存せずH3の配置だけを確認
npm run probe -- --h3-only --network-profile setup-h3-only
# IPv6を別runとして測る場合
npm run probe -- --network-profile R1-v6 --bind '[::]:0'
```

wrapperは毎回`results/raw/h3-<UTC時刻>/`にコマンド・結果・console・stderrを保存する。
通常DNSの反映待ちなどを切り分ける場合は、確認したedge IPを`--resolve <EDGE_IP>`で指定できる。
接続先IPだけを固定し、元hostnameによるSNI・WebPKI・HTTP authority検証は維持する。自動DoH fallbackはしない。
`run.json`のコマンドに指定IPを記録し、通常DNSを使うrunとは区別する。
`summary.json`でH3成功・port取得・candidate登録を確認する。port欠測のH3成功とpeer NAT越え成功を混同しない。
ローカル開発の`npm run dev`はHTTP動作の確認用で、Cloudflare edgeのH3/port観測の実証にはならない。
デプロイ・zone設定が済んだだけでも実測成功とは扱わない。

撤去時は実験用Custom DomainとWorker、planに記載された`ref`のTransform/Cache ruleを削除する。
HTTP/3はzone全体の設定なので、snapshotを参照し、その後の他用途も確認して必要な場合だけ元に戻す。

## 3. 任意: Transform Ruleでportも返す

Workerまたは静的な2xx応答を用意した同じproxied hostnameに、Response Header Transform Ruleを追加する。
既存ruleは維持し、実験用のruleを追加する。条件は実際のhostnameと`/observe`に限定する。
[作成手順](https://developers.cloudflare.com/rules/transform/response-header-modification/create-dashboard/)のSet dynamicで次を設定する。

| Response header | 式 |
|---|---|
| `x-observed-ip` | `ip.src` |
| `x-observed-port` | `to_string(cf.edge.client_port)` |
| `x-observed-is-tcp` | `to_string(cf.edge.client_tcp)` |
| `x-observed-rtt-ms` | `to_string(cf.timings.client_quic_rtt_msec)` |
| `x-observed-http-version` | `http.request.version` |

[Transform Rules](https://developers.cloudflare.com/rules/transform/)はFreeを含む各planで利用でき、proxied DNSが前提。
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
Transform RuleなしのWorker単体ではportが欠測となり、H3 addressの登録は行わない。

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

## 6. 別回線のnative peerで接続を確認する

`nat-peer`はSTUNまたはH3で観測した**同じEndpointのaddressだけ**を相手に渡し、
relay無効のiroh接続で64KiBを往復する小さな実験用binary。
LAN・loopback・Tailscaleの100.64/10・ULA等は既定で拒否する。
Tailscale/SSHはbinaryの配置・操作・candidate file交換にだけ使う。
iroh relay、N0 discovery、LAN候補の自動追加は使用しない。

```sh
export DEVELOPER_DIR=/Library/Developer/CommandLineTools  # このMacで必要な場合
cargo build --locked --bin nat-peer
```

自宅側とテザリング側で同じ新しい`NAT_RUN_ID`を使い、双方に新規run directoryを用意する。
自宅側で`NAT_RUN_ID=$(uuidgen)`を生成し、同じ値を相手側にも設定する。
例はH3。`observe.example.invalid`は設定済みhostnameへ置き換える。

```sh
# 自宅側。先にテザリング側の回線切替・binary配置を済ませてから起動する
target/debug/nat-peer --method h3 --role listen --run-id "$NAT_RUN_ID" \
  --h3-host observe.example.invalid \
  --candidate-out "results/raw/$NAT_RUN_ID/home.candidate.json" \
  --peer-file "results/raw/$NAT_RUN_ID/mobile.peer.json" \
  --results "results/raw/$NAT_RUN_ID/home.result.json"

# テザリング側の別PC。同じrun idを指定する
target/debug/nat-peer --method h3 --role dial --run-id "$NAT_RUN_ID" \
  --h3-host observe.example.invalid \
  --candidate-out "results/raw/$NAT_RUN_ID/mobile.candidate.json" \
  --peer-file "results/raw/$NAT_RUN_ID/home.peer.json" \
  --results "results/raw/$NAT_RUN_ID/mobile.result.json"
```

双方の`READY`を確認したら、各candidate fileを相手の`--peer-file`へSSH等で交換する。
途中のJSONを読まないよう、一旦別名へcopyしてからrenameする。
既存fileへの上書きは拒否するため、再試行は新しいrun id・directoryで行う。
Endpointはfile待ちの間も生きたまま。既定では観測から30秒を超えたcandidateの接続試行は拒否する。
準備に時間がかかった場合は両側を再起動し、fresh candidateを交換する。
候補交換は最大120秒、接続試行20秒、echo10秒で終了する。

STUNを試す場合は新規runで`--method stun --stun-server stun.cloudflare.com:3478`に置き換える。
H3の明示edge IP診断には`--resolve <EDGE_IP>`、IPv6には双方で`--bind '[::]:0'`を指定できる。
listenerも相手の観測candidateへQUIC接続を開始してNAT filterを開きながらincoming接続を待つ。
これは観測addressに向けた同時送信の実験で、mapping refreshやport予測を含む一般的なNAT traversal実装ではない。

両側のresultで次を確認する。

- `same_socket_as_iroh`・`candidate_registered`: 観測とpeer接続が同じEndpoint。
- `direct_connection_success`: 認証済みiroh接続でdirect pathを選択。未試行はnull。
- `echo_success`・`payload_bytes: 65536`: データ往復の確認。接続だけの成功とは区別する。
- `ipv4_pair: true`・`different_observed_ips: true`: IPv4で異なる外部IP。実際に別回線へ接続したことも確認する。

IPv6 direct成功や同じ外部IPでの成功は、それぞれglobal IPv6到達性・hairpin等の確認として分ける。
ローカルテスト用`--allow-private-peer`のrunも別NAT間の成功には数えない。
raw candidate/resultは実IP・一時EndpointIdを含むため公開しない。

## APIと実験上の制約

- STUN: `cloudflare_stun::probe::discover(&endpoint, server_addr, &config)`、またはhostnameを受ける`discover_server`。
- H3: `cloudflare_h3_observer::observe_on_endpoint(&endpoint, host, path, port, timeout)`。
- H3の明示IP診断: `observe_on_endpoint_at(&endpoint, addr, host, path, timeout)`。公開CAによる元hostnameの検証を維持する。
- 同一Endpointを渡す。別readerの起動や`SO_REUSEPORT`による別socketのbindは行わない。
- STUNは送信元・transactionを検証し、boundedな受信channelのoverflowは観測失敗として扱う。
- 登録addressのnetwork変更時のrefresh・失効は自動化していない。呼び出し側で再観測し、古い値は`remove_external_addr`する。
- H3のlegacy `observe` helperは独立socketの診断用で、same-socketはfalse。
- mapping一致はpeerへの到達性を保証しない。Fly QAD・実peerとの比較、30/100回のmatrix、長時間media、relay byte実測は未完了。
