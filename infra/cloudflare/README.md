# Cloudflare zone configuration for HTTP/3 pseudo-QAD (plan E3 / PR 5)

Public placeholders only — real domain, account ID and tokens are never
committed (plan §18.1).

## Platform finding (verified 2026-08 against current Cloudflare docs)

The plan assumed a Response Header Transform Rule could echo the client's
**IP, port, and RTT**. Reality:

| Value | Source | Available? |
|---|---|---|
| client IP | `ip.src` (rules language) / `CF-Connecting-IP` | yes |
| **client source port** | — | **no field exists** (`cf.edge.server_port` is the *edge's* port) |
| QUIC RTT | Workers `request.cf.clientQuicRtt` | yes (Worker only) |
| protocol confirmation | Workers `request.cf.httpProtocol == "HTTP/3"` | yes (Worker only) |

So H3 pseudo-QAD confirms **IP equality only**; the E3 category
`same-ip/same-port` is unreachable through Cloudflare today. The observer
models this as `observed_port: null` and `Comparison::SameIpPortMissing`.

## Recommended setup: Worker (gives RTT + protocol confirmation)

1. Zone: add a proxied A/AAAA record for `observe.example.invalid`
   (orange cloud), any origin (a static page is fine).
2. Zone → Network → enable **HTTP/3**.
3. Add a Worker on route `observe.example.invalid/observe*`:

```js
export default {
  async fetch(request) {
    const cf = request.cf ?? {};
    const ip = request.headers.get("cf-connecting-ip") ?? "";
    const body = JSON.stringify({
      observed_ip: ip,
      // No client source port exists anywhere in the platform.
      observed_port: null,
      rtt_ms: cf.clientQuicRtt ?? null,
      http_protocol: cf.httpProtocol ?? "",
      colo: cf.colo ?? "",
    });
    return new Response(body, {
      headers: {
        "content-type": "application/json",
        "x-observed-ip": ip,
        "x-observed-port": "", // absent by design; see README
        "x-observed-rtt-ms": String(cf.clientQuicRtt ?? ""),
        "x-observed-colo": cf.colo ?? "",
      },
    });
  },
};
```

4. Cache rule: bypass cache for `/observe*` (observations must not be cached).

## Alternative: Response Header Transform Rule (IP only)

If a Worker route is undesirable, a Transform Rule can still expose the IP
(no RTT, no protocol confirmation):

```jsonc
// PUT /zones/{zone_id}/rulesets/phases/http_response_transform/entrypoint
{
  "rules": [
    {
      "expression": "http.request.uri.path eq \"/observe\"",
      "action": "rewrite",
      "action_parameters": {
        "headers": {
          "x-observed-ip": {
            "operation": "set",
            "expression": "ip.src"
          }
        }
      }
    }
  ]
}
```

## Client usage

```sh
cargo run -p cloudflare-h3-observer --bin h3-probe -- \
  observe.example.invalid --path /observe \
  --results results/raw/h3.jsonl --network-profile home-wifi \
  --compare-stun-json results/raw/stun-last.json
```

A TCP fallback (QUIC blocked) surfaces as an error from `h3-probe`, which per
plan E3 step 6 marks the result invalid instead of degraded.
