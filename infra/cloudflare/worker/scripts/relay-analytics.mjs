// Read-only platform metrics. Raw invocation counts are not billing requests.
import { readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: { results: { type: "string" }, out: { type: "string" } } });
if (!values.results || !values.out) throw new Error("--results <relay-lab.json> --out <private-output.json> required");
const report = JSON.parse(await readFile(values.results, "utf8"));
if (report.schema !== "cloudflare-iroh-relay-ping/v1" || !Array.isArray(report.conditions)) throw new Error("relay lab report required");
const config = JSON.parse(await readFile(new URL("../wrangler.local.json", import.meta.url), "utf8"));
async function credential() {
  if (process.env.CLOUDFLARE_API_TOKEN) return process.env.CLOUDFLARE_API_TOKEN;
  for (const file of [`${homedir()}/Library/Preferences/.wrangler/config/default.toml`, `${homedir()}/.wrangler/config/default.toml`]) {
    try {
      const text = await readFile(file, "utf8");
      const token = text.match(/^oauth_token\s*=\s*"([^"]+)"/m)?.[1];
      if (token) return token;
    } catch (error) { if (error.code !== "ENOENT") throw error; }
  }
  throw new Error("Existing Wrangler login or CLOUDFLARE_API_TOKEN required");
}
const token = await credential();
async function api(path, body) {
  const response = await fetch(`https://api.cloudflare.com/client/v4/${path}`, {
    method: body ? "POST" : "GET", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(30_000),
  });
  const data = await response.json();
  if (!response.ok || data.errors?.length) throw new Error(`Cloudflare metrics unavailable: ${JSON.stringify(data.errors ?? response.status)}`);
  return data;
}
const namespaces = await api(`accounts/${config.account_id}/workers/durable_objects/namespaces`);
const namespace = namespaces.result.filter(item => item.class === "IrohRelay" && item.script === config.name);
if (namespace.length !== 1) throw new Error("Expected exactly one IrohRelay namespace for the configured Worker");
const query = `query RelayMetrics($account: String!, $namespace: String!, $objects: [String!]!, $start: Time!, $end: Time!) {
  viewer { accounts(filter: {accountTag: $account}) {
    invocations: durableObjectsInvocationsAdaptiveGroups(limit: 100,
      filter: {namespaceId: $namespace, objectId_in: $objects, datetime_geq: $start, datetime_lt: $end}) {
      dimensions { objectId type status } avg { sampleInterval } sum { requests errors wallTime }
    }
    periodic: durableObjectsPeriodicGroups(limit: 20,
      filter: {namespaceId: $namespace, objectId_in: $objects, datetime_geq: $start, datetime_lt: $end}) {
      dimensions { objectId } avg { sampleInterval } sum { duration activeTime cpuTime inboundWebsocketMsgCount outboundWebsocketMsgCount rowsRead rowsWritten fatalInternalErrors exceededCpuErrors exceededMemoryErrors }
    }
  } }
}`;
const output = { schema: "cloudflare-iroh-relay-platform/v1", collected_at: new Date().toISOString(),
  scope: "Per object, rounded outwards to whole minute buckets; includes preparation/cleanup in those buckets.",
  billing_requests: null, conditions: [] };
for (const condition of report.conditions) {
  const object = condition.final?.object_id ?? condition.baseline?.object_id;
  const entry = { key: condition.key, object_id: object ?? null };
  output.conditions.push(entry);
  const end = condition.final?.sampled_at ?? Date.parse(report.finished_at);
  if (!object || !condition.baseline || !Number.isFinite(end)) { entry.error = "Object id or sample window unavailable"; continue; }
  entry.final_statistics_missing = !condition.final;
  entry.start = new Date(Math.floor(condition.baseline.sampled_at / 60_000) * 60_000).toISOString();
  entry.end = new Date(Math.ceil(end / 60_000) * 60_000).toISOString();
}
const completed = output.conditions.filter(entry => entry.object_id && entry.start && entry.end);
if (completed.length) {
  const start = completed.map(entry => entry.start).sort()[0];
  const end = completed.map(entry => entry.end).sort().at(-1);
  output.query_start = start; output.query_end = end;
  try {
    const result = await api("graphql", { query, variables: { account: config.account_id,
      namespace: namespace[0].id, objects: completed.map(entry => entry.object_id), start, end } });
    const data = result.data.viewer.accounts[0];
    output.possibly_truncated = data.invocations.length >= 100 || data.periodic.length >= 20;
    for (const entry of completed) {
      entry.data = { invocations: data.invocations.filter(row => row.dimensions.objectId === entry.object_id),
        periodic: data.periodic.filter(row => row.dimensions.objectId === entry.object_id) };
      entry.empty = !entry.data.invocations.length && !entry.data.periodic.length;
    }
  } catch (error) { for (const entry of completed) entry.error = String(error); }
}

await writeFile(values.out, JSON.stringify(output, null, 2) + "\n", { mode: 0o600 });
console.log(JSON.stringify({ saved: values.out, conditions: output.conditions.length,
  with_data: output.conditions.filter(item => item.data && !item.empty).length,
  errors: output.conditions.filter(item => item.error).length }));
