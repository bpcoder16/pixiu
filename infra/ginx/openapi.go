package ginx

import (
	_ "embed"
	"fmt"
	"html"
	"net/http"
	"path"

	"github.com/gin-gonic/gin"
	"github.com/swaggo/swag"
)

//go:embed elements/web-components.min.js
var elementsScript []byte

//go:embed elements/styles.min.css
var elementsStyles []byte

//go:embed openapi.js
var elementsLoader []byte

// RegisterOpenAPI 显式注册默认 Swag 文档与 Elements 页面。
// 相对于 r 的路径分别为 /openapi/doc.json 和 /elements。
// 调用方须先通过 blank import 注册生成的文档；未注册时返回错误，不添加任何路由。
// 本函数只在启动阶段调用，不读取配置，也不自动增加认证；访问控制由调用方的路由组负责。
func RegisterOpenAPI(r *gin.RouterGroup) error {
	document, err := swag.ReadDoc()
	if err != nil {
		return fmt.Errorf("ginx: read OpenAPI document: %w", err)
	}
	// 启动时固定文档内容，避免并发请求重复调用会修改 Spec 的 ReadDoc。
	documentJSON := []byte(document)
	r.GET("/openapi/doc.json", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/json; charset=utf-8", documentJSON)
	})
	// Elements 的 URL 均位于 HTML 属性中，使用同一组前缀并按属性转义。
	elementsPage := []byte(fmt.Sprintf(elementsHTML,
		html.EscapeString(path.Join(r.BasePath(), "elements/assets/styles.min.css")),
		html.EscapeString(path.Join(r.BasePath(), "elements/assets/web-components.min.js")),
		html.EscapeString(path.Join(r.BasePath(), "elements/assets/openapi.js")),
		html.EscapeString(path.Join(r.BasePath(), "openapi/doc.json")),
	))
	r.GET("/elements", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", elementsPage)
	})
	r.GET("/elements/assets/web-components.min.js", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", elementsScript)
	})
	r.GET("/elements/assets/styles.min.css", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/css; charset=utf-8", elementsStyles)
	})
	r.GET("/elements/assets/openapi.js", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", elementsLoader)
	})
	return nil
}

// 使用 hash 导航，刷新操作详情页时仍由 /elements 路由响应，无需额外的路径回退。
const elementsHTML = `<!doctype html>
<html>
  <head>
    <title>Stoplight Elements</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="stylesheet" href="%s" />
    <script src="%s" defer></script>
    <script src="%s" defer></script>
    <style>
      body { margin: 0; }
      elements-api { display: block; height: 100vh; }
      elements-api[hidden] { display: none; }
    </style>
  </head>
  <body>
    <elements-api
      data-document-url="%s"
      router="hash"
      layout="sidebar"
    ></elements-api>
    <p id="elements-error" role="alert" hidden></p>
  </body>
</html>`
