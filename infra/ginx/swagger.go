package ginx

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"path"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/swag"
)

//go:embed scalar/api-reference.js
var scalarScript []byte

// RegisterSwagger 显式注册 Swagger UI、默认 Swag 文档与 Scalar 页面。
// 相对于 r 的路径分别为 /swagger/index.html、/swagger/doc.json 和 /docs。
// 调用方须先通过 blank import 注册生成的文档；未注册时返回错误，不添加任何路由。
// 本函数只在启动阶段调用，不读取配置，也不自动增加认证；访问控制由调用方的路由组负责。
func RegisterSwagger(r *gin.RouterGroup) error {
	if _, err := swag.ReadDoc(); err != nil {
		return fmt.Errorf("ginx: read Swagger document: %w", err)
	}
	// JSON 编码保护内联脚本；资源路径另按 HTML 属性转义，兼容非根路由组。
	specURL, _ := json.Marshal(path.Join(r.BasePath(), "swagger/doc.json"))
	assetURL := html.EscapeString(path.Join(r.BasePath(), "docs/assets/api-reference.js"))
	page := []byte(fmt.Sprintf(scalarHTML, assetURL, specURL))
	// gin-swagger 会修改静态 Handler 的 Prefix，每次注册必须使用独立实例。
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.NewHandler()))
	r.GET("/docs", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", page)
	})
	r.GET("/docs/assets/api-reference.js", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", scalarScript)
	})
	return nil
}

const scalarHTML = `<!doctype html>
<html>
  <head>
    <title>Scalar API Reference</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <div id="app"></div>
    <script src="%s"></script>
    <script>
      Scalar.createApiReference('#app', {
        "url":%s,
        "layout":"modern",
        "theme":"default",
        "persistAuth":false,
        "withDefaultFonts":false,
        "telemetry":false,
        "proxyUrl":"",
        "agent":{"disabled":true},
        "defaultHttpClient":{"targetKey":"shell","clientKey":"curl"}
      })
    </script>
  </body>
</html>`
