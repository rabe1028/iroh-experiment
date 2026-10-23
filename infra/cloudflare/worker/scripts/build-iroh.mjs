import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
const cwd = fileURLToPath(new URL("../../../..", import.meta.url));
function run(command, args) {
  const result = spawnSync(command, args, { cwd, stdio: "inherit", env: process.env });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
const version = spawnSync("wasm-bindgen", ["--version"], { encoding: "utf8" });
if (version.stdout?.trim() !== "wasm-bindgen 0.2.127") {
  throw new Error("Install matching CLI: cargo install wasm-bindgen-cli --version 0.2.127 --locked");
}
run("cargo", ["build", "-p", "browser-iroh", "--target", "wasm32-unknown-unknown", "--release", "--locked"]);
run("wasm-bindgen", ["target/wasm32-unknown-unknown/release/browser_iroh.wasm", "--target", "web",
  "--out-dir", "infra/cloudflare/worker/public/iroh-wasm", "--no-typescript"]);
