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
  const spec = {
    openapi: "3.0.3",
    servers: [{ url: "/v2" }],
    paths: {
      "/upload": {
        post: {
          requestBody: {
            content: {
              "multipart/form-data": {
                schema: { type: "object", properties: { file: { type: "string", format: "binary" } } },
              },
            },
          },
        },
      },
    },
    // 即使扩展中出现 Swagger 风格的参数，也不能对 OpenAPI 3 执行兼容转换。
    "x-legacy": { parameters: [{ name: "file", in: "formData", type: "file" }] },
  };
  const { element } = await loadDocument(spec);
  assert.deepEqual(JSON.parse(element.apiDescriptionDocument), spec);
});

test("Swagger 操作级文件参数转换为 binary，保留普通字段、媒体类型和元信息", async () => {
  const file = { name: "file", in: "formData", type: "file", required: true, description: "上传文件" };
  const optional = { name: "attachment", in: "formData", type: "file", required: false, "x-note": "附件" };
  const key = { name: "key", in: "formData", type: "string", required: true, default: "uploads/demo.txt" };
  const spec = {
    swagger: "2.0",
    host: "api.example",
    schemes: ["https"],
    consumes: ["multipart/form-data"],
    paths: {
      "/upload": {
        post: { parameters: [file, key] },
        put: { consumes: ["multipart/form-data"], parameters: [file, optional, key] },
      },
    },
  };
  const original = structuredClone(spec);
  const expected = structuredClone(spec);
  for (const operation of Object.values(expected.paths["/upload"])) {
    operation.parameters = operation.parameters.map(parameter => parameter.type === "file"
      ? { ...parameter, type: "string", format: "binary" }
      : parameter);
  }
  const { element } = await loadDocument(spec);
  assert.deepEqual(JSON.parse(element.apiDescriptionDocument), expected);
  assert.deepEqual(spec, original, "仅转换 fetch 得到的页面副本");
});

test("Swagger 公共文件参数支持路径级和根级定义，保留本地与外部引用", async () => {
  const file = { name: "file", in: "formData", type: "file", required: true };
  const spec = {
    swagger: "2.0",
    host: "api.example",
    schemes: ["https"],
    consumes: ["multipart/form-data"],
    parameters: { SharedFile: file },
    paths: {
      "/upload": {
        parameters: [file],
        post: {},
        patch: { parameters: [{ $ref: "#/parameters/SharedFile" }] },
      },
      "/external": {
        post: { parameters: [{ $ref: "./parameters.json#/File" }] },
      },
    },
  };
  const { element, requests } = await loadDocument(spec);
  const expected = structuredClone(spec);
  expected.parameters.SharedFile = { ...file, type: "string", format: "binary" };
  expected.paths["/upload"].parameters = [{ ...file, type: "string", format: "binary" }];
  assert.deepEqual(JSON.parse(element.apiDescriptionDocument), expected);
  assert.equal(requests.length, 1, "加载脚本不自行请求外部引用");
});

test("只转换真正的 formData 文件参数，不递归改写 schema、示例或扩展", async () => {
  const file = { name: "file", in: "formData", type: "file" };
  const spec = {
    swagger: "2.0",
    host: "api.example",
    schemes: ["https"],
    paths: {
      "/upload": {
        "x-example": { parameters: [file] },
        post: {
          parameters: [
            { name: "file", in: "formData", type: "string" },
            { name: "files", in: "formData", type: "array", items: { type: "file" } },
            { name: "query", in: "query", type: "file" },
            { name: "body", in: "body", schema: file },
            { $ref: "#/parameters/File", ...file },
          ],
          responses: { 200: { description: "下载文件", schema: { type: "file" } } },
          "x-example": { parameters: [file] },
        },
      },
      "x-example": { post: { parameters: [file] } },
    },
    definitions: { Example: { type: "object", example: file } },
  };
  const { element } = await loadDocument(spec);
  assert.deepEqual(JSON.parse(element.apiDescriptionDocument), spec);
});

test("适配后的文件参数经过内嵌 Elements 转换后能被文件控件识别", async () => {
  // 执行交付 bundle 的真实转换器与控件判断；固定入口变化时要求随升级重新核对。
  const bundle = await readFile(new URL("./elements/web-components.min.js", import.meta.url), "utf8");
  const entry = bundle.indexOf(',(()=>{"use strict";var e={};o.r(e),');
  const predicateStart = bundle.indexOf("function jC(e){");
  const predicateEnd = bundle.indexOf("function FC(e){", predicateStart);
  assert.ok(entry > 0 && predicateStart > entry && predicateEnd > predicateStart,
    "Elements 升级后须重新核对模块加载器和文件控件入口");
  const requireBundle = vm.runInNewContext(`${bundle.slice(0, entry)};return o;})();`, { URL });
  const isFile = vm.runInNewContext(`(${bundle.slice(predicateStart, predicateEnd)})`);
  const { transformOas2Operations } = requireBundle(8187);
  const spec = {
    swagger: "2.0",
    paths: {
      "/upload": {
        post: {
          consumes: ["multipart/form-data"],
          parameters: [
            { name: "file", in: "formData", type: "file", required: true, description: "上传文件" },
            { name: "key", in: "formData", type: "string", required: true },
          ],
        },
      },
    },
  };
  const { element } = await loadDocument(spec);
  const adapted = JSON.parse(element.apiDescriptionDocument);
  const [{ request: { body } }] = transformOas2Operations(adapted);
  const { mediaType, schema } = body.contents[0];
  assert.equal(mediaType, "multipart/form-data");
  assert.equal(isFile({ schema: schema.properties.file }), true);
  assert.equal(isFile({ schema: schema.properties.key }), false);
  assert.equal(schema.properties.file.contentMediaType, "application/octet-stream");
  assert.equal(schema.properties.file.description, "上传文件");
  assert.deepEqual(Array.from(schema.required), ["file", "key"]);
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
