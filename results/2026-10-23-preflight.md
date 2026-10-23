# Cloudflare予備実験（2026-10-23）

自宅側のMacから実Cloudflare STUNと配置済みH3 observerを測定した。
別PCをテザリングへ接続する前の測定であり、**別NAT間のpeer接続成功を示す結果ではない**。
実domain・IP・EndpointId・生ログはGit対象外の`results/raw/`に保存した。

## 観測結果

| 条件 | 完了run / 全run | 確認内容 |
|---|---:|---|
| IPv4 STUN 3478・iroh socket | 2 / 3 | 観測addressをirohへ登録 |
| IPv4 STUN 3478・独立socket（診断） | 1 / 3 | iroh socketとの比較証拠には使わない |
| IPv4 STUN 53・iroh socket | 0 / 3 | 応答待ちtimeout |
| IPv4 STUN 53・独立socket（診断） | 0 / 3 | 応答待ちtimeout |
| IPv4 H3単独・通常DNS | 3 / 3 | port取得・same-socket・candidate登録 |
| IPv6 STUN 3478・iroh socket | 1 / 1 | 観測addressをirohへ登録 |
| IPv6 H3単独・通常DNS | 1 / 1 | port取得・same-socket・candidate登録 |
| IPv4 STUN→H3・同じlive socket | 4 / 5 | 完了した4回はすべてIP・port一致。残る1回はSTUN timeoutでH3未実行 |
| IPv6 STUN→H3・同じlive socket | 3 / 3 | 3回ともIP・port一致 |

比較が完了した計7回はすべて`same-ip-same-port`だった。
Cloudflareのport headerを実際に取得・登録・比較できたという予備的証拠であり、
そのmappingが別peerにも通用するかは別の検証が必要。

STUN単独runは同じ解決済みserver IPへ各2送信、各応答待ち1.5秒。
比較runはSTUNの各応答待ち2秒・最大3送信を使用し、成功後に同じEndpointでH3を実行した。
独立socketのrunは、STUN応答の有無を切り分ける診断用で、same-socket比較には含めない。
H3は10秒の観測deadline。今回の通常DNSは成功し、edge IP固定は使用していない。

IPv4 H3単独runのedge RTTは17〜138ms、coloはKIX/PDXに分かれた。
STUNの`rtt_ms`は再送・backoffを含む初回送信からの経過時間であり、
H3のedge smoothed RTTとは測定定義も接続先も異なるため、protocol性能差として比較しない。
sample数が小さく、順次実行のため、一般的な成功率・安定性は結論づけない。
STUN 53の応答がなかった理由も、今回の測定だけでは遮断と断定しない。

## peer接続の進捗

`nat-peer`でCloudflare観測candidateだけを交換し、relay無効のiroh接続で64KiBを往復する経路を用意した。
ローカルの模擬STUNを使う別プロセス間テストでは成功し、
private/Tailscale candidate、別runのcandidate、echo受信拒否を成功扱いしないことも確認した。

同じMacの2プロセスで実Cloudflareのpublic candidateを使う診断も各方式1組試したが、
片側のSTUN/H3観測でtimeoutしたためpeer接続は未実行。NAT越え失敗としては数えない。
この診断は仮に成功しても、同一回線内のhairpin等の確認にとどまる。

別NAT間の本測定には「自宅側Mac」と「テザリング接続した別PC」を使用する。
IPv4・異なる外部IP・Cloudflare観測candidateのみ・relay無効を確認し、
接続成功、データ往復成功、観測段階の失敗を分けて記録する。
Tailscaleは操作・candidate file交換にのみ使用し、データ接続先から除外する。
現時点では相手PCの指定と回線切替が未完了で、本測定は未実施。

手順は[Cloudflare実験ガイド](../infra/cloudflare/README.md)を参照。
