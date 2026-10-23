# Cloudflare DO relay 実測（2026-10-23）

E7のcontrol relay PoCとして、Cloudflare Durable Object上に`iroh-relay 1.0.3`互換の
`iroh-relay-v2` WebSocket relayを実装し、公開WorkerとRust/Wasm版irohで測定した。
公式n0 relayへのfallbackは使用していない。

## 疎通と再生成後の復帰

- 同じPCのブラウザ2タブで、認証されたEndpointId間の64KiB往復と両端の内容一致に成功。
- 先に起動したEndpointを待機させ、DO再生成を挟んでから相手が参加しても往復に成功。
  終了後の集計はconstructor実行8回、WebSocket upgrade 2回、認証2回、転送186 frame、drop 0。
  接続を張り直さず、WebSocket attachmentから認証とroutingを復帰できた。
- 同一originのrelay経路を転送前後に確認。ブラウザから直接UDPは使用していない。
  別端末・別NAT・native同一socket STUN/H3の成功を示す結果ではない。

## 測定条件

`/relay-lab`からPing 15 / 30 / 60 / 120 / 300秒を比較する。
各間隔についてEndpointのみ1個を待機させる条件と、2 Endpointで64KiBを往復した後に
QUIC接続を維持する条件を用意し、全10条件を同時に330秒観測する。

- client/server両方のrelay protocol Ping間隔を変更する。irohのQUIC heartbeatは既定の5秒。
  clientのPing応答期限・送信timeout・QUIC idle timeoutは延長しない。
- DOのstatus取得は開始・終了に1回ずつ。観測中はアプリデータとstatus pollingを送らない。
  browser内のiroh healthは1秒ごとに読み、変化時だけ記録する。
- `messages`はDOの受信handler回数。Ping/Pong、認証、QUIC frameを分類して保存する。
- `survived`は最終接続状態だけでなく、観測中の再接続と終了socket増加がないことも要求する。
  回復した接続も「無切断の維持成功」へ丸めない。
- constructor実行差は再生成の観測値。正確なwake回数・hibernation durationではない。

## 予備測定で見つかった問題

最初の330秒pilotで再接続を検出した。診断用の次の測定でも全条件に再接続があり、
QUICの5条件は途中で接続を失った。失敗した結果も非公開rawデータに保存した。

実装レビューで、`getWebSockets()`の返すclose処理中socketを同時接続枠へ数える問題を特定した。
2 Endpointの再接続後に退役済みsocketで枠が埋まり、後続upgradeを拒否していた。
有効socketだけを枠へ数える修正と回帰テストを追加した。生涯upgrade上限20回は維持する。
これは再接続を妨げる問題の修正であり、最初の再接続の原因を特定したものではない。

## 修正後の330秒測定

2026-10-23 13:52:10–13:57:50 UTCに公開Workerで測定した。
観測本体は330.004秒、取得できた各条件のserver timestamp差は330.088–330.160秒。
完了時の集計失敗も含め、`all_conditions_survived=false`を保持する。

| 条件 | Ping秒 | 受信message | client Ping / Pong | QUIC frame | constructor増分 | 再接続 | 結果 |
|---|---:|---:|---:|---:|---:|---:|---|
| Endpoint待機 | 15 | 31 | 9 / 22 | 0 | 22 | 0 | 維持 |
| Endpoint待機 | 30 | 17 | 6 / 11 | 0 | 11 | 0 | 維持 |
| Endpoint待機 | 60 | 未取得 | 未取得 | 未取得 | 未取得 | 未取得 | relay切断・集計timeout |
| Endpoint待機 | 120 | 3 | 1 / 2 | 0 | 3 | 0 | 維持 |
| Endpoint待機 | 300 | 1 | 0 / 1 | 0 | 2 | 0 | 維持 |
| QUIC接続維持 | 15 | 286 | 0 / 44 | 242 | 0 | 0 | 維持 |
| QUIC接続維持 | 30 | 270 | 0 / 22 | 248 | 0 | 0 | 維持 |
| QUIC接続維持 | 60 | 280 | 0 / 10 | 270 | 0 | 0 | 維持 |
| QUIC接続維持 | 120 | 262 | 0 / 4 | 258 | 0 | 0 | 維持 |
| QUIC接続維持 | 300 | 239 | 0 / 2 | 237 | 0 | 0 | 維持 |

QUICの全5条件で準備時の64KiB往復・両端一致に成功し、観測中は両端ともrelay/QUIC接続を維持した。
取得済み9条件は再接続0・終了socket増加0。Endpoint待機60秒は177秒でrelay未接続、180秒で復帰、
304秒から再度未接続となり、最終statsも10秒でtimeoutした。取得できなかったcounterは補完しない。
この切断の原因は未特定であり、Ping 60秒固有の不具合とは断定しない。

待機だけなら15秒から300秒への変更で受信messageは31→1へ減った。一方、QUIC接続を保持した条件は
286→239で、237–270のQUIC frameが残った。QUIC条件ではconstructor増分も全て0だった。
この短い1回の観測では、relay Ping延長だけで同率のhibernation・料金改善が得られるとはいえない。

## プラットフォームmetricsの扱い

同じobjectと時間窓に絞ってCloudflare GraphQLからInvocations/Periodicを一括取得する。
時間窓は全条件の開始から終了までを分単位で外側へ丸めるため、準備・終了処理を含む場合がある。
`sampleInterval`を保持し、空データやAPIエラーを0へ変換しない。

- `sum.duration`: GB*s、`activeTime`/`cpuTime`: microseconds（実際のschema introspectionで確認）。
- Invocationsのrequest数はraw handler数であり、billing request数ではない。
  Hibernationの受信messageはInvocations側へ入り、Periodicの受信数0だけで未受信と判断しない。
- 請求request数と月額は未確定。短い1回の測定を長期の料金・最適Pingへ外挿しない。
  切断で小さくなった値を低コストの成功例として採用しない。

最終runの全10 objectについて、13:52:00–13:58:00 UTCの共通窓を14:03 UTCに取得した。
APIのrate budgetによる初回拒否後、5分待って再取得し、欠落・query上限到達ともになし。

| 条件 | Ping秒 | platform duration (GB*s) | raw invocations（請求requestではない） |
|---|---:|---:|---:|
| Endpoint待機 | 15 | 0.080029 | 59 |
| Endpoint待機 | 30 | 0.049473 | 34 |
| Endpoint待機・切断あり | 60 | 8.482420 | 29 |
| Endpoint待機 | 120 | 0.028820 | 11 |
| Endpoint待機 | 300 | 0.339055 | 8 |
| QUIC接続維持 | 15 | 0.110839 | 498 |
| QUIC接続維持 | 30 | 0.064731 | 468 |
| QUIC接続維持 | 60 | 0.041777 | 462 |
| QUIC接続維持 | 120 | 0.053652 | 454 |
| QUIC接続維持 | 300 | 0.057677 | 419 |

Periodicの`sampleInterval`は全条件1。QUIC条件のhibernation invocation群は
`sampleInterval`約2.44–2.74のadaptive estimateを含む。表のinvocationsは準備と終了も含む
全type合計で、330秒のDO counter差分と同じ値ではない。

失敗したEndpoint待機60秒ではHTTP `clientDisconnected`が3件、active timeは66.268909秒を記録した。
全条件でCPU/メモリ上限超過・fatal internal errorの報告は0。これは切断原因の特定には足りない。
待機300秒もdurationは15秒より大きく、短時間の測定にはばらつきがある。
単調な料金低減や最適間隔は、この結果から確定しない。

参考: [Hibernation WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)、
[DO metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/)、
[DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)。

## 記録と未検証範囲

非公開rawデータは`results/raw/cloudflare-relay/`へ保存する。招待capability・room token・
object ID・実hostnameを公開reportへ転載しない。再現手順は[Cloudflare README](../infra/cloudflare/README.md)。

Worker単体テスト、Rust workspaceテスト、native/wasm Clippy、format、Worker dry-runを実施。
署名改変・challenge replay・未認証転送・送信元偽装・第三identity・期限・quota・
同時再接続・退役socketの枠占有・受信先切断を検査した。

今後はスマホとPCの両端JSON、長時間・回線変更・background復帰を確認する。
native別NATのdirect接続、camera/mediaのACL、障害検出時間、月額評価は未検証。
