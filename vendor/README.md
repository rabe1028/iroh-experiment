# Fixed upstream snapshots for same-socket experiments

This repository uses an opt-in iroh IP transport extension instead of a second UDP socket or `SO_REUSEPORT`.
The application crates depend on the same patched iroh package through `[patch.crates-io]`.
The added APIs are experimental, native-only, and gated by `unstable-raw-udp`.

## Provenance

| Snapshot | Published version | Upstream revision | License |
|---|---|---|---|
| `iroh/` | iroh 1.0.3 | `f2eb930dda3779c6d852b72f3712aacd6e573ab1` (`iroh/`) | MIT OR Apache-2.0, with included BSD3 notices |
| `h3-noq/` | derived from h3-quinn 0.0.10 | `2dc3412bdf6083451920d5bfd7a9484d054c1859` (`h3-quinn/`) | MIT |
| `noq-udp/` | noq-udp 1.1.1 | `12a4bf0b42070b570fb8cf90fe315c630b03f56e` (`noq-udp/`) | MIT OR Apache-2.0 |

Sources were copied from the published crates corresponding to the versions already in Cargo.lock.
`.cargo_vcs_info.json` and `Cargo.toml.orig` retain the original upstream metadata; the normalized `Cargo.toml` is the active manifest.
The iroh MIT/Apache license texts were also copied from that exact upstream revision.
These are locally modified snapshots, not unmodified upstream releases. `noq` core remains the registry version **1.1.1**, fixed by the H3 adapter dependency and Cargo.lock. Its UDP layer is the local `noq-udp` 1.1.1 snapshot.

## Transport patch

`iroh/src/socket/transports/stun.rs` adds weak socket handles and bounded STUN broadcast receivers.
`IpTransport::poll_recv` examines each received GRO segment, copies STUN-framed datagrams to subscribers, and leaves QUIC packets on the ordinary receive path.
STUN has no QUIC fixed bit; the existing `grease_quic_bit(false)` setting makes noq ignore these datagrams.
Application probes validate the source, transaction ID, address family and nonzero port before registering an external address.

STUN sends use `UdpSender::try_send` on the exact existing socket. On backpressure they retry after 1 ms, bounded by the probe's send timeout and endpoint cancellation.
This avoids overwriting netwatch's single `poll_send_ready` waker while QUIC or other probes are waiting to send.
No second reader, socket rebinding trick, global socket registry, or socket ownership cycle is introduced.
Handles are invalidated as soon as endpoint closure starts.

The Endpoint adds `stun_sockets()` and `connect_quic(config, addr, server_name)`.
The latter uses the existing noq endpoint and its CID demultiplexer, so ordinary iroh traffic, H3 and QAD can coexist without QUIC packet sniffing or a second connection dispatcher.
H3 uses an independent WebPKI TLS client configuration; ordinary iroh identity authentication is unchanged.

`h3-noq` is the existing h3-quinn adapter renamed locally and linked to noq 1.1.1.
The API adaptation uses noq's ordered `read_chunk(max)` returning `Bytes`, and removes the obsolete Quinn `IllegalOrderedRead` error arm.
The application keeps the old `h3_quinn`/`quinn` import aliases to make the adapter substitution easy to review.

## Observed peer path isolation

`QuicTransportConfigBuilder::disable_path_discovery()` is an opt-in experiment setting
under `unstable-raw-udp`. It disables QUIC NAT traversal candidate exchange, multipath,
and observed-address reports. Server handshake migration retains iroh's required default.
The experiment application additionally checks authenticated peer identity and the exact
observed IP/port before and throughout echo, rejecting unexpected paths or lost path events.
Ordinary endpoints keep the upstream configuration defaults.

## Truncated UDP receive retry

The single-message Unix receive path in noq-udp 1.1.1 reuses `msghdr` after a truncated packet.
On macOS a following `EAGAIN` can retain `MSG_TRUNC`, making the retry loop spin without returning to Tokio.
The local patch moves `prepare_recv` inside that loop, resetting output flags and address/control lengths before each syscall.
A subprocess regression sends an oversized foreign STUN datagram before the genuine response, and kills the child on timeout so a regression cannot hang the test runner.
This also protects builds without the same-socket feature: the fix is in the shared UDP layer.

## Review and maintenance

[patches/iroh-same-socket.patch](patches/iroh-same-socket.patch), [patches/h3-noq.patch](patches/h3-noq.patch), [patches/noq-udp-truncation.patch](patches/noq-udp-truncation.patch), and [patches/iroh-observed-path.patch](patches/iroh-observed-path.patch) contain the changes relative to the published snapshots, excluding copied license texts.
To review a future refresh, compare those files against the next pinned upstream version; do not update the registry cache in place.
The rest of the upstream files are retained, including notices. Vendor crates are excluded from workspace membership.

The APIs reject ambiguous probes with multiple sockets of the same address family.
Mapping refresh, expiry and automatic network-epoch tracking remain application responsibilities.
Same-socket equality does not prove peer connectivity through destination-dependent NATs.

Validation lives in the application crates: STUN response correlation/concurrency and live iroh transfer, IPv4/IPv6 H3 with a test CA, TLS hostname rejection, CLI defaults, external-address registration, and relay-disabled media discovery.

## Experimental relay ping interval

`Builder::relay_ping_interval()` is a separate opt-in API gated by `unstable-relay-ping`,
available to native and browser endpoints. It accepts exactly 15/30/60/120/300 seconds.
The regular default remains 15 seconds. Only the relay actor's periodic ping timer changes;
QUIC heartbeat (5 seconds), relay pong deadlines, and stalled-send timeout remain upstream defaults.
The Cloudflare DO server independently uses the same selected interval for its own binary relay pings.
The patch is [patches/iroh-relay-ping.patch](patches/iroh-relay-ping.patch).
