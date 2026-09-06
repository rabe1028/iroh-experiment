# iroh-experiment

iroh Serverless NAT Traversal / Direct Media Path 実験のリポジトリ。
詳細は `EXPERIMENT_PLAN.md` を参照。

## 対象バージョン

- iroh: **1.0.3** (`Cargo.toml` で pin)

## 構成 (PR 1: Baseline)

| Path | 内容 |
|---|---|
| `crates/common/` | 結果スキーマ (`ExperimentResult`, §11)、ALPN定数 |
| `crates/baseline/` | E0 Baseline echo実験 (`baseline-acceptor` / `baseline-dialer`) |
| `schemas/experiment-result.schema.json` | 結果JSONLのスキーマ |
| `results/raw.example/` | 結果JSONLの保存先 |

## 使い方 (E0 Baseline)

```sh
cargo run --release --bin baseline-acceptor -- \
    --results results/raw.example/e0.jsonl --network-profile L0
# 別ターミナル
cargo run --release --bin baseline-dialer <ENDPOINT_ID> \
    --results results/raw.example/e0.jsonl --network-profile L0
```

acceptorが `ENDPOINT_ID=...` を出力するのでdialerに渡す。
dialerが生成したrun idがストリーム先頭に送られ、両プロセスの結果行が
同じ `run_id` を持つため1実行の2行をペアリングできる。
10 MiBのランダムデータを送りecho照合し、path telemetry
(relay→direct移行時刻、selected path、RTT、UDP datagram数) を記録して
結果を1行JSONで追記する。

公開IPアドレスは結果に保存しない (§18)。

## ロードマップ (§17)

1. ~~PR 1: Baseline (echo protocol, path telemetry, result schema)~~
2. PR 2: Raw TCP tunnel
3. PR 3: Fly.io QAD-only reference
4. PR 4: Same-socket Cloudflare STUN
5. PR 5: HTTP/3 observer
6. PR 6: Media separation
7. PR 7: Durable Object relay
8. PR 8: Final report
