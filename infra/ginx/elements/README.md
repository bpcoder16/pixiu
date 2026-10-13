# Stoplight Elements 静态资源

固定使用 `@stoplight/elements@9.0.28` 的官方 npm 发布包，以下文件保持发布内容不变：

- `web-components.min.js`：独立 Web Component，包含运行时依赖。
- `styles.min.css`：配套样式，字体资源已包含在 CSS 内。
- `LICENSE`：Apache-2.0 许可证。
- `web-components.min.js.LICENSE.txt`：发布包附带的第三方版权声明。

来源：

- 上游项目：https://github.com/stoplightio/elements
- 发布包：https://registry.npmjs.org/@stoplight/elements/-/elements-9.0.28.tgz
- HTML 接入说明：https://github.com/stoplightio/elements/blob/main/docs/getting-started/elements/html.md
- npm 包完整性（SHA-512）：`uorJPmmZZY/oNaa0Ko9FhXtm2oH7/nDVMN/pEpa6FK21xChlR/RJ2wcSTlw4+IZZWpmy4+0AusMhvBvQ6UB81A==`

文件 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| `web-components.min.js` | `3908d0e9f6425c9224a88d393550729a38dd4e1e49ab23609ca23382a6e941eb` |
| `styles.min.css` | `a52002228108fb567b75caff209c4d8aa256ae591de3dea7d3b1a384b1a27b06` |
| `LICENSE` | `d45c17f7ecc80e0deab3372ff352c7d00b7d573755c3c672088c21a376673698` |
| `web-components.min.js.LICENSE.txt` | `58b2f880c6e49cc6eb9ef1b8752a22db028c373c3acf6f7ebeb1d6316c7c3294` |

JS/CSS 通过 Go embed 编译进程序，页面位于 `/elements`，读取同一路由组的 `/openapi/doc.json`。
项目加载脚本位于 `../openapi.js`，通过 `/elements/assets/openapi.js` 提供，不修改上游资源。
该脚本按浏览器最终获取文档的地址补齐 Swagger 2.0 缺省或空的 `host/schemes`，使在线调试保留 `basePath`；显式服务器配置不变。
补齐只影响页面内副本，JSON 端点保持原始文档；文档 URL 仍传给 Elements 用于解析相对引用。
使用 hash 导航，刷新接口详情页不需要服务端路径回退；启用在线调试，不配置外部代理。
Elements 沿用上游默认行为，将认证输入保存在浏览器 localStorage 的 `TryIt_securitySchemeValues` 中；刷新后会恢复输入。
加载页面和内置资源不需要访问 CDN，也不需要在应用构建或运行时安装 Node/npm。
升级时显式替换固定版本，同步许可证与校验值，并验证 Swagger 2.0 文档、认证、调试和带前缀的页面导航。
加载脚本的回归测试使用 Node 内置测试器，在仓库根目录执行 `node --test infra/ginx/openapi_script_test.mjs`，无需安装 npm 依赖。
