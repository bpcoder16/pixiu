import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";

// 固定版本的最小补丁：实际请求和代码示例共用此处的认证 Header 构造逻辑。
// 升级上游时必须重新核对入口和校验值，不允许对未知 bundle 静默替换。
const upstreamSHA256 = "3908d0e9f6425c9224a88d393550729a38dd4e1e49ab23609ca23382a6e941eb";
const original = '"header"===e.scheme.in&&i.push({name:e.scheme.name,value:e.authValue||"123"})';
const replacement = String.raw`"header"===e.scheme.in&&(() => {
  // Pixiu: Swagger 2.0 通过显式扩展选择 Bearer 补齐，不推断普通 API Key 的含义。
  const setting = e.scheme.extensions?.["x-pixiu-bearer"];
  const enabled = setting === true || setting === "true";
  if (!enabled || String(e.scheme.name).toLowerCase() !== "authorization") {
    return i.push({ name: e.scheme.name, value: e.authValue || "123" });
  }
  // 同时接受纯 Token 和完整 Bearer Header；清空输入时不发送占位凭据。
  const token = String(e.authValue ?? "").trim().replace(/^Bearer(?:\s+|$)/i, "").trim();
  if (token) i.push({ name: e.scheme.name, value: "Bearer " + token });
})()`;

const args = process.argv.slice(2);
if (args.length > 1 || (args.length === 1 && args[0] !== "--check")) {
  throw new Error("用法：node infra/ginx/elements/patch-bearer.mjs [--check]");
}
const bundleURL = new URL("./web-components.min.js", import.meta.url);
const source = await readFile(bundleURL, "utf8");
const sha256 = value => createHash("sha256").update(value).digest("hex");
if (sha256(source) === upstreamSHA256) {
  if (args[0] === "--check") throw new Error("尚未应用 Pixiu Bearer 补丁");
  if (source.split(original).length !== 2) throw new Error("上游认证 Header 入口不唯一");
  await writeFile(bundleURL, source.replace(original, replacement));
  console.log("已应用 Pixiu Bearer 补丁");
} else if (source.split(replacement).length === 2 && sha256(source.replace(replacement, original)) === upstreamSHA256) {
  console.log("Pixiu Bearer 补丁及上游内容校验通过");
} else {
  throw new Error("Elements bundle 与固定版本不一致，请核对上游版本和补丁");
}
