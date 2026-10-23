import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { createHash } from "node:crypto";

const { values } = parseArgs({ options: {
  hostname: { type: "string" },
  "account-id": { type: "string" },
  "zone-id": { type: "string" },
} });
const host = values.hostname?.toLowerCase();
if (!host || host.length > 253 || !host.includes(".") ||
    !host.split(".").every(label => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label)) ||
    host.endsWith(".workers.dev")) {
  throw new Error("--hostname must be a dedicated hostname in your Cloudflare zone (no URL/path/wildcard)");
}
for (const key of ["account-id", "zone-id"]) {
  if (!/^[a-f0-9]{32}$/i.test(values[key] ?? "")) {
    throw new Error(`--${key} must be a 32-character Cloudflare ID`);
  }
}
const config = JSON.parse(await readFile(new URL("../wrangler.json", import.meta.url)));
config.account_id = values["account-id"];
config.routes = [{ pattern: host, custom_domain: true }];
const expression = `(http.host eq "${host}" and http.request.uri.path eq "/observe")`;
const ref = "iroh_h3_" + createHash("sha256").update(host).digest("hex").slice(0, 16);
const headers = Object.fromEntries(Object.entries({
  "x-observed-ip": "ip.src",
  "x-observed-port": "to_string(cf.edge.client_port)",
  "x-observed-is-tcp": "to_string(cf.edge.client_tcp)",
  "x-observed-rtt-ms": "to_string(cf.timings.client_quic_rtt_msec)",
  "x-observed-http-version": "http.request.version",
}).map(([name, expression]) => [name, { operation: "set", expression }]));
const plan = {
  zone_id: values["zone-id"],
  hostname: host,
  http3: { value: "on" },
  rules: [
    {
      phase: "http_response_headers_transform",
      rule: { ref, description: "iroh H3 observation", expression,
        action: "rewrite", action_parameters: { headers }, enabled: true },
    },
    {
      phase: "http_request_cache_settings",
      rule: { ref, description: "iroh H3 observation cache bypass", expression,
        action: "set_cache_settings", action_parameters: { cache: false }, enabled: true },
    },
  ],
};
// Keep the real host/IDs local. No credentials are stored in these files.
for (const [name, content] of [["wrangler.local.json", config], ["zone-plan.local.json", plan]]) {
  await writeFile(new URL(`../${name}`, import.meta.url), JSON.stringify(content, null, 2) + "\n", { mode: 0o600 });
}
console.log("Created wrangler.local.json and zone-plan.local.json (ignored by git). No remote changes made.");
