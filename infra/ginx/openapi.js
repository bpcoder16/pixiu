(async () => {
  const element = document.querySelector("elements-api");
  try {
    const response = await fetch(new URL(element.dataset.documentUrl, window.location.href));
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const description = await response.json();
    const documentURL = new URL(response.url);
    if (description.swagger === "2.0") {
      // Elements 需要完整服务器字段才能保留 basePath；缺省值取浏览器实际获取文档的地址。
      // 只修改页面内的副本，保留显式配置以及 JSON 端点的原始内容。
      if (!description.host) description.host = documentURL.host;
      if (!Array.isArray(description.schemes) || description.schemes.length === 0) {
        description.schemes = [documentURL.protocol.slice(0, -1)];
      }
    }
    await customElements.whenDefined("elements-api");
    // 先提供内容，避免组件再次请求未补齐的文档；URL 仍用于解析相对引用。
    element.apiDescriptionDocument = JSON.stringify(description);
    element.apiDescriptionUrl = documentURL.href;
  } catch {
    element.hidden = true;
    const error = document.getElementById("elements-error");
    error.textContent = "无法加载 API 文档，请刷新页面重试。";
    error.hidden = false;
  }
})();
