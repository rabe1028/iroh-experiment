import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";

export async function configureZone(plan, token, { request = fetch,
  directory = new URL("../", import.meta.url) } = {}) {
  if (!token) throw new Error("CLOUDFLARE_API_TOKEN required (never pass a token as a CLI argument)");
  const base = `https://api.cloudflare.com/client/v4/zones/${plan.zone_id}`;
  async function api(path, method = "GET", body) {
    const response = await request(`${base}${path}`, {
      method,
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(30_000),
    });
    const data = await response.json();
    // Cloudflare uses 404/10003 for an absent phase entry point. Permission
    // and other failures must not be treated as an empty ruleset.
    if (method === "GET" && response.status === 404 &&
        path.endsWith("/entrypoint") && data.errors?.some(e => e.code === 10003)) return null;
    if (!response.ok || !data.success) {
      throw new Error(`Cloudflare ${method} ${path}: HTTP ${response.status}; codes ${(data.errors ?? []).map(e => e.code).join(",")}`);
    }
    return data.result;
  }
  // Read all current state before making changes. Only append/update our own
  // rule; never PUT an entire existing phase ruleset back over other rules.
  const zone = await api("");
  if (zone.status !== "active" || !plan.hostname.endsWith(`.${zone.name}`)) {
    throw new Error("Observer hostname must be a subdomain of this active zone");
  }
  const http3 = await api("/settings/http3");
  const phases = [];
  for (const entry of plan.rules) {
    const existing = await api(`/rulesets/phases/${entry.phase}/entrypoint`);
    // Adopt an identically named/scoped Dashboard-created observer rule too,
    // assigning the deterministic ref when PATCHing it. Do not duplicate it.
    const matches = (existing?.rules ?? []).filter(r => r.ref === entry.rule.ref ||
      (r.description === entry.rule.description && r.expression === entry.rule.expression &&
        r.action === entry.rule.action));
    if (matches.length > 1) {
      throw new Error(`Duplicate observer rules in ${entry.phase}`);
    }
    phases.push({ ...entry, existing, match: matches[0] });
  }
  // Snapshot is local-only and useful for undoing the zone-wide HTTP/3 toggle.
  await writeFile(new URL(`zone-${plan.zone_id}-before.local.json`, directory),
    JSON.stringify({ zone_id: plan.zone_id, http3, phases }, null, 2) + "\n", { flag: "wx", mode: 0o600 }).catch(error => {
      if (error.code !== "EEXIST") throw error; // Preserve the first snapshot on re-run.
    });
  if (http3.value !== "on") await api("/settings/http3", "PATCH", plan.http3);
  for (const { phase, rule, existing, match } of phases) {
    if (!existing) {
      await api("/rulesets", "POST", { name: "iroh H3 observer", kind: "zone", phase, rules: [rule] });
    } else {
      if (match) {
        await api(`/rulesets/${existing.id}/rules/${match.id}`, "PATCH", rule);
      } else {
        await api(`/rulesets/${existing.id}/rules`, "POST", rule);
      }
    }
    console.log(`Configured ${phase}`);
  }
  console.log("Zone configured. The actual H3 response and source port still need a live probe.");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { values } = parseArgs({ options: { apply: { type: "boolean", default: false } } });
  const plan = JSON.parse(await readFile(new URL("../zone-plan.local.json", import.meta.url)));
  if (values.apply) {
    await configureZone(plan, process.env.CLOUDFLARE_API_TOKEN);
  } else {
    console.log(JSON.stringify(plan, null, 2));
    console.log("Plan only. Use --apply with CLOUDFLARE_API_TOKEN to update the zone.");
  }
}
