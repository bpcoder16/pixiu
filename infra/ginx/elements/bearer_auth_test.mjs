import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

// 直接验证随服务交付的固定版本 bundle 中的共用认证构造函数，覆盖请求和代码示例共用的路径。
const bundle = await readFile(new URL("./web-components.min.js", import.meta.url), "utf8");
const start = bundle.indexOf("const d_=");
const end = bundle.indexOf(";function f_", start);
assert.ok(start >= 0 && end > start, "Elements 升级后须重新核对认证构造入口");
const buildAuth = vm.runInNewContext(`(${bundle.slice(start + "const d_=".length, end)})`, {
  mC: scheme => scheme.type === "apiKey",
  gC: scheme => scheme.type === "oauth2",
  t: { isObject: value => value !== null && typeof value === "object" },
});

function headersFor(scheme, authValue) {
  // 将另一个 VM 中的返回值转成当前环境的普通对象，避免原型差异影响断言。
  return JSON.parse(JSON.stringify(buildAuth([{ scheme, authValue }], [], [])[1]));
}

const bearer = {
  type: "apiKey", in: "header", name: "Authorization",
  extensions: { "x-pixiu-bearer": "true" },
};

test("显式声明的 Bearer 支持纯 Token 和完整 Header，且只补一次前缀", () => {
  for (const value of ["test-token", "Bearer test-token", "bearer test-token", "  Bearer   test-token  "]) {
    assert.deepEqual(headersFor(bearer, value), [{ name: "Authorization", value: "Bearer test-token" }]);
  }
  assert.deepEqual(headersFor({ ...bearer, extensions: { "x-pixiu-bearer": true } }, "test-token"), [
    { name: "Authorization", value: "Bearer test-token" },
  ]);
});

test("空 Token 不生成 Authorization，也不发送上游的占位凭据", () => {
  for (const value of [undefined, "", "  ", "Bearer", "Bearer "]) {
    assert.deepEqual(headersFor(bearer, value), []);
  }
});

test("普通 API Key 和其他认证方案保持原有行为", () => {
  for (const scheme of [
    { type: "apiKey", in: "header", name: "Authorization" },
    { ...bearer, extensions: { "x-pixiu-bearer": false } },
    { ...bearer, extensions: { "x-pixiu-bearer": "false" } },
    { ...bearer, name: "X-API-Key" },
  ]) {
    assert.deepEqual(headersFor(scheme, "test-token"), [{ name: scheme.name, value: "test-token" }]);
  }
  assert.deepEqual(headersFor({ type: "http", scheme: "basic" }, "dXNlcjpwYXNz"), [
    { name: "Authorization", value: "Basic dXNlcjpwYXNz" },
  ]);
  const [query, headers] = buildAuth([{ scheme: { ...bearer, in: "query" }, authValue: "test-token" }], [], []);
  assert.equal(query[0].value, "test-token");
  assert.equal(headers.length, 0);
});
