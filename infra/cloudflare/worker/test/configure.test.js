import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, mkdir, copyFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(execFile);
async function configure(t, args) {
  const dir = await mkdtemp(`${tmpdir()}/iroh-config-test-`);
  t.after(() => rm(dir, { recursive: true, force: true }));
  await mkdir(`${dir}/scripts`);
  await copyFile(new URL("../scripts/configure.mjs", import.meta.url), `${dir}/scripts/configure.mjs`);
  await copyFile(new URL("../wrangler.json", import.meta.url), `${dir}/wrangler.json`);
  const run = exec(process.execPath, [`${dir}/scripts/configure.mjs`, ...args]);
  return { dir, run };
}
const ids = ["--account-id", "a".repeat(32), "--zone-id", "b".repeat(32)];

test("configuration uses a custom domain and scopes both rules to that host's exact observation path", async t => {
  const { dir, run } = await configure(t, ["--hostname", "Observe.example.com", ...ids]);
  await run;
  const config = JSON.parse(await readFile(`${dir}/wrangler.local.json`));
  assert.deepEqual(config.routes, [{ pattern: "observe.example.com", custom_domain: true }]);
  assert.equal(config.workers_dev, false);
  assert.equal(config.preview_urls, false);
  assert.deepEqual(config.assets, { directory: "./public", binding: "ASSETS", run_worker_first: true });
  const plan = JSON.parse(await readFile(`${dir}/zone-plan.local.json`));
  assert.deepEqual(plan.rules.map(r => r.phase), ["http_response_headers_transform", "http_request_cache_settings"]);
  for (const entry of plan.rules) {
    assert.equal(entry.rule.expression, '(http.host eq "observe.example.com" and http.request.uri.path eq "/observe")');
  }
  assert.deepEqual(plan.rules[0].rule.action_parameters.headers["x-observed-port"], {
    operation: "set", expression: "to_string(cf.edge.client_port)",
  });
  assert.deepEqual(plan.rules[1].rule.action_parameters, { cache: false });
});

test("invalid hosts cannot inject routes or Cloudflare expressions", async t => {
  for (const host of ["https://example.com", "*.example.com", 'a.example.com") or true',
    "example.com/path", "a.workers.dev", "a..com", "-a.example.com"]) {
    const { run } = await configure(t, [`--hostname=${host}`, ...ids]);
    await assert.rejects(run, /dedicated hostname/);
  }
});

test("missing or malformed IDs cannot create deployment configuration", async t => {
  for (const args of [[], ["--account-id", "wrong"], ids.slice(0, 2)]) {
    const { run } = await configure(t, ["--hostname", "observe.example.com", ...args]);
    await assert.rejects(run, /32-character Cloudflare ID/);
  }
});
