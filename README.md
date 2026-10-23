# iroh-experiment

iroh Serverless NAT Traversal / Direct Media Path 実験のリポジトリ。
詳細は `EXPERIMENT_PLAN.md` を参照。

スマホのブラウザからのHTTP/3・STUN予備テストは、配置済みobserverの`/test`で実行できる。[手順](infra/cloudflare/README.md#スマホのブラウザからの予備テスト)を参照。2台のブラウザでのWebRTC端末間試験は同じobserverの`/peer`で実行できる。irohのQUIC経路の別NAT間接続はnative版で検証する。

予備測定の数値・制約は[2026-10-23の結果](results/2026-10-23-preflight.md)を参照。

## 対象バージョン

- iroh: **1.0.3**（same-socket拡張を含む`vendor/iroh`へ固定）
- H3: noq **1.1.1** + ローカル`h3-noq` adapter。`noq-udp`にはtruncated UDP受信の再試行修正を適用。由来と変更点は[vendor/README.md](vendor/README.md)。

## 現在の進捗（2026-10-23）

STUN・H3のsame-socket統合まで実装済み。Cloudflare STUN 3478への単発観測を確認し、Cloudflare H3でのIP・port観測とcandidate登録も確認。同じlive socketでSTUN/H3のIP・port一致を7回確認。別NAT間のpeer接続はテザリング側PCの準備待ち。
以下はコードの実装状況であり、実環境での合格を示すものではない。

| 項目 | 状況 | 残作業・制約 |
|---|---|---|
| E0 Baseline | echo・path telemetry・JSONL出力を実装 | 実ネットワーク測定 |
| Raw TCP tunnel | service routing・EndpointId認可・互換性テストを実装 | short-lived capabilityは未実装 |
| Cloudflare STUN | 同一UDP socketで観測・応答分離・external address登録を実装 | 3478は成功とtimeoutが混在。53は各mode 3runで応答なし。実NAT比較は未実施 |
| HTTP/3 observer | 同一noq Endpoint・socketでH3観測とSTUN比較。完全なaddressはirohへ登録。Worker・設定生成・zone設定補助を用意 | 通常DNSでのH3観測成功。live STUN比較はIPv4 4/5・IPv6 3/3完了し全件一致。別NAT間peer検証は未実施 |
| Media separation | Endpoint分離・relay無効化・停止gate・合成stream・media socket上のSTUNを実装 | 実NAT・長時間・UDP遮断試験。relay byte数は未計測（null） |
| Fly.io QAD-only | 未実装 | デプロイ定義・基準測定 |
| Durable Object relay | 未実装 | relay protocol・Ping間隔実験 |
| 実験基盤・最終report | direct-only `nat-peer`と予備測定reportを追加 | 別NAT環境・matrix runner・最終集計 |

2026-10-23にCommand Line Toolsを選択してworkspaceテストを実行し、成功。
STUN/H3/irohの実際の送信元port一致、IPv4/IPv6、並列probe、送信元・transaction検証、終了時のキャンセル、candidate登録、TCP互換性、mediaの正常転送・接続切断を確認した。
このMacでは`DEVELOPER_DIR=/Library/Developer/CommandLineTools cargo test --workspace --locked --offline`を使用。
GitHub ActionsでもLinux・macOSでformat、Clippy、既定featureのビルド、workspaceテストを実行する。

Cloudflareの公式field一覧には`cf.edge.client_port`とQUIC RTTが掲載されている。
以前の「portは取得できない」という調査記述は訂正した。fieldの利用可否・値の意味は対象zoneで実測する。
設定と出典は[Cloudflare実験手順](infra/cloudflare/README.md)を参照。

## 構成

| Path | 内容 |
|---|---|
| `crates/common/` | 結果スキーマ (`ExperimentResult`, §11)、ALPN定数 |
| `crates/baseline/` | E0 Baseline echo実験 (`baseline-acceptor` / `baseline-dialer`) |
| `crates/tcp-tunnel/` | raw TCP tunnel (`gateway` / `client`) |
| `crates/cloudflare-stun/` | STUN codec・単体probe (`stun-probe`) |
| `crates/cloudflare-h3-observer/` | same-socket HTTP/3観測・STUN比較 (`h3-probe`) |
| `crates/media-separation/` | direct-only合成media (`media-receiver` / `media-sender`) |
| `infra/cloudflare/` | Cloudflare設定・最小実験手順 |
| `schemas/experiment-result.schema.json` | 結果JSONLのスキーマ |
| `results/raw/` | 非公開の実測データ保存先（gitignore対象） |
| `results/raw.example/` | 公開用の匿名化サンプル保存先（現時点では空） |

## 使い方 (E0 Baseline)

```sh
cargo run --release --bin baseline-acceptor -- \
    --results results/raw/e0.jsonl --network-profile L0
# 別ターミナル
cargo run --release --bin baseline-dialer <ENDPOINT_ID> \
    --results results/raw/e0.jsonl --network-profile L0
```

acceptorが `ENDPOINT_ID=...` を出力するのでdialerに渡す。
dialerが生成したrun idがストリーム先頭に送られ、両プロセスの結果行が
同じ `run_id` を持つため1実行の2行をペアリングできる。
10 MiBのランダムデータを送りecho照合し、path telemetry
(relay→direct移行時刻、selected path、RTT、UDP datagram数) を記録して
結果を1行JSONで追記する。

公開IPアドレスは結果に保存しない (§18)。

## Cloudflareで今できる最小実験

1. `stun-probe`は既定でirohの実socketを使用する。同じプロセスで指定した3478/53の観測は同じbindingを使う。
2. WorkerまたはTransform Ruleを設定し、`h3-probe --stun-server stun.cloudflare.com:3478`で同じsocketからSTUNとH3を比較する。
3. `x-observed-port`が返る場合はportも比較し、完全な観測addressをirohへ登録する。
4. media実験のsender/receiverにも`--stun-server`を指定でき、relayを無効にしたmedia socket自体で観測する。

手順は[infra/cloudflare/README.md](infra/cloudflare/README.md)に記載。
`--compare-stun-json`で別プロセスのfileを読む場合は別socket比較となる。same-socket比較には`--stun-server`を使う。
同じsocketで一致しても、相手peerに対するmappingの有効性やNAT越え成功は別途確認する。
direct mediaのローカル確認は別途可能だが、Cloudflareだけでのcontrol relayはまだない。

## 次の作業

1. CloudflareのH3観測用zoneを設定し、same-socketのSTUN/H3比較を実測する。
2. Fly QAD基準・NAT試験環境を整備し、direct成功率とmedia障害時の挙動を測る。
3. mappingのrefresh・失効、network epoch追従を整備する。
4. Durable Object relay、集計・最終reportへ進む。

元のPR単位の計画と各項目の進捗は[実験計画 §17](EXPERIMENT_PLAN.md#17-pull-request単位の進め方)を参照。
