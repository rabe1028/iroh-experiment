import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { configureZone } from "../scripts/setup-zone.mjs";

const zoneId = "a".repeat(32);
const plan = { zone_id: zoneId, hostname: "observe.example.com", http3: { value: "on" },
  rules: [{ phase: "http_response_headers_transform", rule: {
    ref: "observer", action: "rewrite", expression: '(http.host eq "observe.example.com")',
    action_parameters: { headers: {} }, enabled: true,
  } }],
};

async function fixture(t, { existing = { id: "ruleset", rules: [{ id: "other", ref: "other" }] },
  http3 = "off", failure = null, zone = "example.com" } = {}) {
  const dir = await mkdtemp(`${tmpdir()}/iroh-zone-test-`);
  t.after(() => rm(dir, { recursive: true, force: true }));
  const calls = [];
  const request = async (url, init) => {
    const path = url.split(`/zones/${zoneId}`)[1];
    const method = init.method;
    calls.push({ path, method, body: init.body && JSON.parse(init.body) });
    let result;
    if (failure && path === failure.path) {
      return Response.json({ success: false, errors: [{ code: failure.code, message: "private message" }] }, { status: failure.status });
    }
    if (!path) result = { name: zone, status: "active" };
    else if (path === "/settings/http3") result = { value: http3 };
    else if (path.endsWith("/entrypoint")) {
      if (!existing) return Response.json({ success: false, errors: [{ code: 10003 }] }, { status: 404 });
      result = existing;
    } else result = { id: "created" };
    return Response.json({ success: true, result });
  };
  return { calls, options: { request, directory: pathToFileURL(`${dir}/`) }, dir };
}

test("existing rules are preserved: add one rule, snapshot once, enable HTTP/3", async t => {
  const { calls, options, dir } = await fixture(t);
  await configureZone(plan, "test-token", options);
  const mutations = calls.filter(c => c.method !== "GET");
  assert.deepEqual(mutations.map(c => [c.method, c.path]), [
    ["PATCH", "/settings/http3"], ["POST", "/rulesets/ruleset/rules"],
  ]);
  assert.deepEqual(mutations[1].body, plan.rules[0].rule);
  const snapshot = JSON.parse(await readFile(`${dir}/zone-${zoneId}-before.local.json`));
  assert.equal(snapshot.phases[0].existing.rules[0].ref, "other");
  assert.equal(snapshot.http3.value, "off");
});

test("re-running updates only the matching rule, without duplicate or whole-ruleset replacement", async t => {
  const existing = { id: "ruleset", rules: [{ id: "other", ref: "other" }, { id: "ours", ref: "observer" }] };
  const { calls, options } = await fixture(t, { existing, http3: "on" });
  await configureZone(plan, "test-token", options);
  await configureZone(plan, "test-token", options);
  const mutations = calls.filter(c => c.method !== "GET");
  assert.equal(mutations.length, 2);
  assert.ok(mutations.every(c => c.method === "PATCH" && c.path === "/rulesets/ruleset/rules/ours"));
});

test("an absent entry point creates a zone ruleset in the correct phase", async t => {
  const { calls, options } = await fixture(t, { existing: null, http3: "on" });
  await configureZone(plan, "test-token", options);
  const mutation = calls.find(c => c.method === "POST");
  assert.equal(mutation.path, "/rulesets");
  assert.equal(mutation.body.kind, "zone");
  assert.equal(mutation.body.phase, "http_response_headers_transform");
});

test("a matching Dashboard-created observer rule is adopted instead of duplicated", async t => {
  const rule = { ...plan.rules[0].rule, description: "iroh H3 observation" };
  const manual = { ...rule, id: "dashboard-rule", ref: "dashboard-ref" };
  const { calls, options } = await fixture(t, { http3: "on", existing: { id: "ruleset", rules: [manual] } });
  await configureZone({ ...plan, rules: [{ phase: plan.rules[0].phase, rule }] }, "test-token", options);
  const mutations = calls.filter(c => c.method !== "GET");
  assert.equal(mutations.length, 1);
  assert.equal(mutations[0].path, "/rulesets/ruleset/rules/dashboard-rule");
  assert.equal(mutations[0].body.ref, "observer");
});

test("permission failure cannot be mistaken for an absent ruleset and makes no remote changes", async t => {
  const { calls, options } = await fixture(t, { failure: {
    path: "/rulesets/phases/http_response_headers_transform/entrypoint", status: 403, code: 10000,
  } });
  await assert.rejects(configureZone(plan, "secret-not-in-errors", options), error => {
    assert.match(error.message, /HTTP 403/);
    assert.doesNotMatch(error.message, /secret-not-in-errors|private message/);
    return true;
  });
  assert.ok(calls.every(c => c.method === "GET"));
});

test("wrong-zone hostname and duplicate observer refs fail before mutation", async t => {
  for (const params of [
    { zone: "unrelated.com" },
    { existing: { id: "ruleset", rules: [{ ref: "observer" }, { ref: "observer" }] } },
  ]) {
    const { calls, options } = await fixture(t, params);
    await assert.rejects(configureZone(plan, "test-token", options));
    assert.ok(calls.every(c => c.method === "GET"));
  }
});
