import { readFile, mkdir, writeFile, open } from "node:fs/promises";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const { values } = parseArgs({ options: {
  "network-profile": { type: "string", default: "unspecified" },
  bind: { type: "string", default: "0.0.0.0:0" },
  "h3-only": { type: "boolean", default: false },
  resolve: { type: "string" },
} });
const config = JSON.parse(await readFile(new URL("../wrangler.local.json", import.meta.url)));
const hostname = config.routes[0].pattern;
const root = fileURLToPath(new URL("../../../../", import.meta.url));
const run = `results/raw/h3-${new Date().toISOString().replaceAll(/[:.]/g, "-")}`;
await mkdir(new URL(`../../../../${run}/`, import.meta.url), { recursive: true });
const args = ["run", "--locked", "-p", "cloudflare-h3-observer", "--bin", "h3-probe", "--",
  hostname, "--path", "/observe", "--results", `${run}/result.jsonl`,
  "--network-profile", values["network-profile"], "--bind", values.bind];
if (!values["h3-only"]) args.push(
  "--stun-server", "stun.cloudflare.com:3478", "--stun-server", "stun.cloudflare.com:53");
if (values.resolve) args.push("--resolve", values.resolve);
await writeFile(`${root}${run}/run.json`, JSON.stringify({ hostname, bind: values.bind,
  network_profile: values["network-profile"], args }, null, 2) + "\n");
const stdout = await open(`${root}${run}/console.log`, "w", 0o600);
const stderr = await open(`${root}${run}/stderr.log`, "w", 0o600);
try {
  const child = spawn("cargo", args, { cwd: root, stdio: ["ignore", stdout.fd, stderr.fd] });
  const code = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve(code ?? (signal ? 1 : 0)));
  });
  const output = await readFile(`${root}${run}/console.log`, "utf8");
  const observationLine = output.split("\n").find(line => line.startsWith("OBSERVATION="));
  const observation = observationLine ? JSON.parse(observationLine.slice("OBSERVATION=".length)) : null;
  const resultText = await readFile(`${root}${run}/result.jsonl`, "utf8").catch(error => {
    if (error.code === "ENOENT") return "";
    throw error;
  });
  const resultLine = resultText.trim().split("\n").at(-1);
  const result = resultLine ? JSON.parse(resultLine) : null;
  // A STUN/setup/build error can prevent the H3 attempt entirely. Keep that
  // unknown instead of recording it as a measured H3 failure.
  const h3Success = observation ? code === 0
    : result?.failure_reason?.startsWith("H3 observation failed") ? false : null;
  const portObserved = observation?.observed_port != null;
  const ready = code === 0 && portObserved && observation?.candidate_registered === true;
  await writeFile(`${root}${run}/summary.json`, JSON.stringify({
    exit_code: code, h3_success: h3Success, port_observed: portObserved,
    stun_requested: !values["h3-only"],
    stun_comparison_completed: output.split("\n").some(line => line.startsWith("COMPARISON=")),
    candidate_registered: observation?.candidate_registered ?? false,
    ready_for_address_comparison: ready,
  }, null, 2) + "\n");
  console.log(`Probe ${code === 0 ? "completed" : "failed"}. Private logs: ${run}/`);
  if (code === 0) console.log(ready
    ? "Source port observed and candidate registered. Peer NAT traversal still needs a separate test."
    : "H3 responded, but source port/candidate is missing. Check the Transform Rule before the peer experiment.");
  process.exitCode = code;
} finally {
  await stdout.close();
  await stderr.close();
}
