import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

async function loadDocument(spec, options = {}) {
  const source = await readFile(new URL("./openapi.js", import.meta.url), "utf8");
  const element = { dataset: { documentUrl: "/internal/openapi/doc.json" } };
  const error = { hidden: true };
  const requests = [];
  await vm.runInNewContext(source, {
    URL,
    window: { location: { href: "https://docs.example:8443/internal/docs#/operations/ping" } },
    document: {
      querySelector: () => element,
      getElementById: () => error,
    },
    customElements: { whenDefined: async () => {} },
    fetch: async (url) => {
      requests.push(String(url));
      return {
        ok: options.ok ?? true,
        status: options.status ?? 200,
        url: options.url ?? String(url),
        json: async () => {
          if (options.invalidJSON) throw new SyntaxError("Invalid JSON");
          return structuredClone(spec);
        },
      };
    },
  });
  return { element, error, requests };
}

test("Swagger 缺省服务器字段使用文档地址，保留 basePath 和接口定义", async () => {
  const spec = {
    swagger: "2.0",
    basePath: "/v2",
    paths: { "/ping": { get: { responses: { 200: { description: "OK" } } } } },
  };
  const { element, error, requests } = await loadDocument(spec);
  assert.deepEqual(requests, ["https://docs.example:8443/internal/openapi/doc.json"]);
  const actual = JSON.parse(element.apiDescriptionDocument);
  assert.deepEqual(actual, { ...spec, host: "docs.example:8443", schemes: ["https"] });
  assert.equal(element.apiDescriptionUrl, requests[0]);
  assert.equal(error.hidden, true);
});

test("仅补齐空字段，保留显式服务器、协议和操作级覆盖", async () => {
  for (const { fields, expected } of [
    {
      fields: { host: "api.example", schemes: ["http", "https"] },
      expected: { host: "api.example", schemes: ["http", "https"] },
    },
    {
      fields: { host: "api.example", schemes: [] },
      expected: { host: "api.example", schemes: ["https"] },
    },
    {
      fields: { host: "", schemes: ["http"] },
      expected: { host: "docs.example:8443", schemes: ["http"] },
    },
    {
      fields: { host: "", schemes: null },
      expected: { host: "docs.example:8443", schemes: ["https"] },
    },
  ]) {
    const spec = { swagger: "2.0", ...fields, paths: { "/ping": { get: { schemes: ["http"] } } } };
    const { element } = await loadDocument(spec);
    assert.deepEqual(JSON.parse(element.apiDescriptionDocument), {
      ...spec,
      ...expected,
    });
  }
});

test("重定向后以最终文档地址解析缺省字段和相对引用", async () => {
  const { element } = await loadDocument({ swagger: "2.0", basePath: "/v2" }, {
    url: "http://api.example:8080/spec/doc.json",
  });
  assert.deepEqual(JSON.parse(element.apiDescriptionDocument), {
    swagger: "2.0", basePath: "/v2", host: "api.example:8080", schemes: ["http"],
  });
  assert.equal(element.apiDescriptionUrl, "http://api.example:8080/spec/doc.json");
});

test("OpenAPI 3 文档保持原样", async () => {
  const spec = { openapi: "3.0.3", servers: [{ url: "/v2" }], paths: {} };
  const { element } = await loadDocument(spec);
  assert.deepEqual(JSON.parse(element.apiDescriptionDocument), spec);
});

test("请求失败或无效 JSON 显示加载错误", async () => {
  for (const options of [{ ok: false, status: 401 }, { invalidJSON: true }]) {
    const { element, error } = await loadDocument({}, options);
    assert.equal(element.apiDescriptionDocument, undefined);
    assert.equal(element.hidden, true);
    assert.equal(error.hidden, false);
    assert.match(error.textContent, /无法加载 API 文档/);
  }
});
