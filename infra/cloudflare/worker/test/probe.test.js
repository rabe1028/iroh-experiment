import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, mkdir, copyFile, writeFile, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(execFile);
async function probe(t, outcome, options = []) {
  const root = await mkdtemp(`${tmpdir()}/iroh-probe-test-`);
  t.after(() => rm(root, { recursive: true, force: true }));
  const worker = `${root}/infra/cloudflare/worker`;
  await mkdir(`${worker}/scripts`, { recursive: true });
  await mkdir(`${root}/bin`);
  await copyFile(new URL("../scripts/probe.mjs", import.meta.url), `${worker}/scripts/probe.mjs`);
  await writeFile(`${worker}/wrangler.local.json`, JSON.stringify({ routes: [{ pattern: "observe.example.com" }] }));
  await writeFile(`${root}/bin/cargo`, `#!/usr/bin/env node
const fs = require('node:fs');
const args = process.argv.slice(2);
const result = args[args.indexOf('--results') + 1];
const outcome = ${JSON.stringify(outcome)};
fs.writeFileSync(result, JSON.stringify({failure_reason: outcome.error ?? null}) + '\\n');
if (outcome.observation) console.log('OBSERVATION=' + JSON.stringify(outcome.observation));
if (outcome.comparison) console.log('COMPARISON=' + JSON.stringify(outcome.comparison));
process.exitCode = outcome.error ? 1 : 0;
`, { mode: 0o700 });
  let exitCode = 0;
  try {
    await exec(process.execPath, [`${worker}/scripts/probe.mjs`, ...options], {
      env: { ...process.env, PATH: `${root}/bin:${process.env.PATH}` },
    });
  } catch (error) { exitCode = error.code; }
  const [run] = await readdir(`${root}/results/raw`);
  const directory = `${root}/results/raw/${run}`;
  return { exitCode, summary: JSON.parse(await readFile(`${directory}/summary.json`)),
    command: JSON.parse(await readFile(`${directory}/run.json`)) };
}

test("probe wrapper records live comparison readiness and preserves explicit DNS diagnostics", async t => {
  const { exitCode, summary, command } = await probe(t, {
    observation: { observed_port: 12345, candidate_registered: true }, comparison: "same-ip-same-port",
  }, ["--resolve", "192.0.2.1", "--network-profile", "test"]);
  assert.equal(exitCode, 0);
  assert.equal(summary.h3_success, true);
  assert.equal(summary.ready_for_address_comparison, true);
  assert.equal(summary.stun_comparison_completed, true);
  assert.ok(command.args.includes("192.0.2.1"));
  assert.ok(command.args.includes("stun.cloudflare.com:3478"));
});

test("port-missing H3 success cannot claim readiness or completed STUN comparison", async t => {
  const { summary, command } = await probe(t, {
    observation: { observed_port: null, candidate_registered: false },
  }, ["--h3-only"]);
  assert.equal(summary.h3_success, true);
  assert.equal(summary.ready_for_address_comparison, false);
  assert.equal(summary.stun_requested, false);
  assert.equal(summary.stun_comparison_completed, false);
  assert.ok(!command.args.includes("--stun-server"));
});

test("STUN failure leaves H3 unattempted; an actual H3 failure is measured false", async t => {
  for (const [error, expected] of [["same-socket STUN failed: timeout", null], ["H3 observation failed: timeout", false]]) {
    const { exitCode, summary } = await probe(t, { error });
    assert.equal(exitCode, 1);
    assert.equal(summary.h3_success, expected);
    assert.equal(summary.stun_comparison_completed, false);
    assert.equal(summary.ready_for_address_comparison, false);
  }
});
