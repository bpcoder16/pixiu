package ginx

import (
	_ "embed"
	"fmt"
	"html"
	"net/http"
	"path"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/swaggo/swag"
)

//go:embed elements/web-components.min.js
var elementsScript []byte

//go:embed elements/styles.min.css
var elementsStyles []byte

//go:embed openapi.js
var elementsLoader []byte

// OpenAPIModule 声明一份命名 Swag 文档；Name 同时用作文档实例名和 URL 中的模块名。
// Name 只允许 ASCII 字母、数字、下划线和连字符；Title 为空时使用 Name。
type OpenAPIModule struct {
	Name  string
	Title string
}

var openAPIModuleName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// RegisterOpenAPI 注册 Swag 文档与 Elements 页面。
// 未传 modules 时提供默认文档的 /openapi/doc.json 和 /docs。
// 传入模块时提供 /openapi/<Name>.json、/docs/<Name> 和模块导航，/docs 展示首个模块。
// 页面与静态资源均位于 /docs 下，不注册 /elements 或重定向。
// 全部文档 GET 路由主动登记为忽略访问日志、详情采集及 Observe，仍保留 Recovery。
// 调用方须先通过 blank import 注册全部文档；校验或读取失败时不添加任何路由。
// 本函数只在启动阶段调用，不读取配置，也不自动增加认证；访问控制由调用方的路由组负责。
func RegisterOpenAPI(r *gin.RouterGroup, modules ...OpenAPIModule) error {
	named := len(modules) != 0
	if !named {
		modules = []OpenAPIModule{{Name: swag.Name, Title: "Stoplight Elements"}}
	}
	seen := make(map[string]bool, len(modules))
	for _, module := range modules {
		if !openAPIModuleName.MatchString(module.Name) || seen[module.Name] {
			return fmt.Errorf("ginx: invalid or duplicate OpenAPI module name %q", module.Name)
		}
		seen[module.Name] = true
	}
	// 先读取全部文档，再登记任何路由，避免后续模块缺失时留下部分注册。
	// 启动时固定内容，避免并发请求重复调用会修改 Spec 的 ReadDoc。
	documents := make([][]byte, len(modules))
	for i, module := range modules {
		document, err := swag.ReadDoc(module.Name)
		if err != nil {
			return fmt.Errorf("ginx: read OpenAPI document %q: %w", module.Name, err)
		}
		documents[i] = []byte(document)
	}
	for i, module := range modules {
		documentPath, pagePath := "/openapi/doc.json", "/docs"
		var navigation string
		if named {
			documentPath, pagePath = "/openapi/"+module.Name+".json", "/docs/"+module.Name
			navigation = openAPINavigation(r.BasePath(), modules, module.Name)
		}
		documentJSON := documents[i]
		RegisterIgnoredRoute(r, http.MethodGet, documentPath, func(c *gin.Context) {
			c.Data(http.StatusOK, "application/json; charset=utf-8", documentJSON)
		})
		// 所有动态文字和 URL 均转义；模块页共用静态资源和加载脚本。
		elementsPage := []byte(fmt.Sprintf(elementsHTML,
			html.EscapeString(module.title()),
			html.EscapeString(path.Join(r.BasePath(), "docs/assets/styles.min.css")),
			html.EscapeString(path.Join(r.BasePath(), "docs/assets/web-components.min.js")),
			html.EscapeString(path.Join(r.BasePath(), "docs/assets/openapi.js")),
			navigation,
			html.EscapeString(path.Join(r.BasePath(), documentPath)),
		))
		pageHandler := func(c *gin.Context) {
			c.Data(http.StatusOK, "text/html; charset=utf-8", elementsPage)
		}
		RegisterIgnoredRoute(r, http.MethodGet, pagePath, pageHandler)
		if named && i == 0 {
			RegisterIgnoredRoute(r, http.MethodGet, "/docs", pageHandler)
		}
	}
	RegisterIgnoredRoute(r, http.MethodGet, "/docs/assets/web-components.min.js", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", elementsScript)
	})
	RegisterIgnoredRoute(r, http.MethodGet, "/docs/assets/styles.min.css", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/css; charset=utf-8", elementsStyles)
	})
	RegisterIgnoredRoute(r, http.MethodGet, "/docs/assets/openapi.js", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", elementsLoader)
	})
	return nil
}

func (m OpenAPIModule) title() string {
	if m.Title == "" {
		return m.Name
	}
	return m.Title
}

func openAPINavigation(basePath string, modules []OpenAPIModule, selected string) string {
	var nav strings.Builder
	nav.WriteString(`<nav class="api-modules" aria-label="API 模块"><strong>API 文档</strong>`)
	for _, module := range modules {
		current := ""
		if module.Name == selected {
			current = ` aria-current="page"`
		}
		fmt.Fprintf(&nav, `<a href="%s"%s>%s</a>`,
			html.EscapeString(path.Join(basePath, "docs", module.Name)), current, html.EscapeString(module.title()))
	}
	nav.WriteString(`</nav>`)
	return nav.String()
}

// 使用 hash 导航，刷新操作详情页时仍由对应模块页响应，无需额外的路径回退。
const elementsHTML = `<!doctype html>
<html>
  <head>
    <title>%s</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="stylesheet" href="%s" />
    <script src="%s" defer></script>
    <script src="%s" defer></script>
    <style>
      body { margin: 0; height: 100vh; display: flex; flex-direction: column; }
      .api-modules { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid #e5e7eb; font: 14px system-ui, sans-serif; }
      .api-modules strong { margin-right: 16px; color: #374151; }
      .api-modules a { padding: 8px 16px; border-radius: 6px; color: #4b5563; text-decoration: none; }
      .api-modules a:hover { background: #f3f4f6; }
      .api-modules a[aria-current="page"] { color: #1d4ed8; background: #eff6ff; font-weight: 600; }
      elements-api { display: block; flex: 1; min-height: 0; }
      elements-api[hidden] { display: none; }
    </style>
  </head>
  <body>
    %s
    <elements-api
      data-document-url="%s"
      router="hash"
      layout="sidebar"
    ></elements-api>
    <p id="elements-error" role="alert" hidden></p>
  </body>
</html>`
