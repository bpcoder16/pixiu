# Scalar 静态资源

`api-reference.js` 复用 Chestnut `contrib/httphandler/gin/scalar/assets/api-reference.js`，保持原文件内容。
文件头标明来源为 `@scalar/api-reference@1.55.0/dist/browser/standalone.js`，由 jsDelivr 的 Terser v5.39.0 压缩。

- 上游项目：https://github.com/scalar/scalar
- 固定版本来源：https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.55.0
- 许可证：MIT，见本目录 `LICENSE`，取自 https://raw.githubusercontent.com/scalar/scalar/main/LICENSE；原资源内的第三方版权声明一并保留。
- SHA-256：`dcf9f0f04420123a97ea057f4cd38917ba4cf02e3209920bb1e4a5a33f3d4eaa`

通过 Go embed 编译进程序。升级时显式替换固定版本、同步许可证与校验值，并验证页面加载及 API 调试。
页面关闭外部默认字体、遥测、Agent、代理和认证持久化；不需要运行时访问 CDN。
