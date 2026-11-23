package ginx

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/netip"
	"net/textproto"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/http/httpguts"
)

// CORSRule 描述一个路径范围的浏览器跨域策略。各规则的路径范围必须互不重叠。
type CORSRule struct {
	// PathPrefix 是按完整路径段匹配的绝对前缀，/ 表示整个 Engine。
	PathPrefix string `mapstructure:"pathPrefix"`
	// AllowOrigins 为 HTTP(S) 来源，或单独的 "*"；启用凭据时必须列举来源。
	AllowOrigins []string `mapstructure:"allowOrigins"`
	// AllowMethods 显式列举可跨域访问的业务方法，方法名区分大小写。
	AllowMethods []string `mapstructure:"allowMethods"`
	// AllowHeaders 允许预检声明的请求头，空列表表示不额外授权。
	AllowHeaders []string `mapstructure:"allowHeaders"`
	// ExposeHeaders 指定浏览器脚本可额外读取的响应头。
	ExposeHeaders []string `mapstructure:"exposeHeaders"`
	// AllowCredentials 允许浏览器在跨域请求中携带凭据。
	AllowCredentials bool `mapstructure:"allowCredentials"`
	// MaxAge 是预检缓存时长，须为非负整秒，零值省略响应头。
	MaxAge time.Duration `mapstructure:"maxAge"`
}

type corsPolicy struct {
	prefix        string
	origins       map[string]bool
	methods       map[string]bool
	headers       map[string]bool
	allowHeaders  string
	exposeHeaders string
	credentials   bool
	maxAge        string
}

// NewCORS 校验并复制策略，返回可挂载到 Config.Middlewares 的 Engine 级中间件。
// 匹配范围内的预检直接结束；实际请求继续经过路由和鉴权。空规则集直接放行。
func NewCORS(rules ...CORSRule) (gin.HandlerFunc, error) {
	policies := make([]corsPolicy, 0, len(rules))
	for i, rule := range rules {
		p, err := compileCORS(rule)
		if err != nil {
			return nil, fmt.Errorf("ginx: cors[%d]: %w", i, err)
		}
		for j := range policies {
			if policies[j].matches(p.prefix) || p.matches(policies[j].prefix) {
				return nil, fmt.Errorf("ginx: cors[%d].pathPrefix %q overlaps cors[%d] %q", i, p.prefix, j, policies[j].prefix)
			}
		}
		policies = append(policies, p)
	}
	return func(c *gin.Context) {
		u := c.Request.URL
		for i := range policies {
			p := &policies[i]
			if !p.matches(u.Path) {
				continue
			}
			// Gin 可开启路径清理或按 RawPath 路由；各表示须属于同一授权范围。
			if p.matches(path.Clean(u.Path)) && (u.RawPath == "" || p.matches(u.RawPath) && p.matches(path.Clean(u.RawPath))) {
				p.apply(c)
			}
			return
		}
	}, nil
}

func (p *corsPolicy) matches(requestPath string) bool {
	tail, ok := strings.CutPrefix(requestPath, p.prefix)
	return p.prefix == "/" || ok && (tail == "" || tail[0] == '/')
}

func compileCORS(r CORSRule) (corsPolicy, error) {
	p := corsPolicy{
		prefix:      r.PathPrefix,
		origins:     make(map[string]bool, len(r.AllowOrigins)),
		methods:     make(map[string]bool, len(r.AllowMethods)),
		headers:     make(map[string]bool, len(r.AllowHeaders)),
		credentials: r.AllowCredentials,
	}
	if !strings.HasPrefix(p.prefix, "/") || path.Clean(p.prefix) != p.prefix || strings.ContainsAny(p.prefix, "?#%\\") || !httpguts.ValidHeaderFieldValue(p.prefix) {
		return p, fmt.Errorf("invalid pathPrefix %q", p.prefix)
	}
	if len(r.AllowOrigins) == 0 {
		return p, fmt.Errorf("allowOrigins is required")
	}
	for _, origin := range r.AllowOrigins {
		if origin == "*" {
			if len(r.AllowOrigins) != 1 || r.AllowCredentials {
				return p, fmt.Errorf("allowOrigins * requires a single origin and allowCredentials=false")
			}
			p.origins["*"] = true
			continue
		}
		key, err := serializedOrigin(origin)
		if err != nil {
			return p, fmt.Errorf("allowOrigins: %w", err)
		}
		p.origins[key] = true
	}
	if len(r.AllowMethods) == 0 {
		return p, fmt.Errorf("allowMethods is required")
	}
	for _, method := range r.AllowMethods {
		if method == "*" || !httpguts.ValidHeaderFieldName(method) {
			return p, fmt.Errorf("invalid allowMethods entry %q", method)
		}
		p.methods[method] = true
	}
	for _, name := range r.AllowHeaders {
		if name == "*" || !httpguts.ValidHeaderFieldName(name) {
			return p, fmt.Errorf("invalid allowHeaders entry %q", name)
		}
		p.headers[textproto.CanonicalMIMEHeaderKey(name)] = true
	}
	for _, name := range r.ExposeHeaders {
		if name == "*" || !httpguts.ValidHeaderFieldName(name) {
			return p, fmt.Errorf("invalid exposeHeaders entry %q", name)
		}
	}
	p.allowHeaders = strings.Join(r.AllowHeaders, ", ")
	p.exposeHeaders = strings.Join(r.ExposeHeaders, ", ")
	if r.MaxAge < 0 || r.MaxAge%time.Second != 0 {
		return p, fmt.Errorf("maxAge must be a non-negative whole number of seconds")
	}
	if r.MaxAge > 0 {
		p.maxAge = strconv.FormatInt(int64(r.MaxAge/time.Second), 10)
	}
	return p, nil
}

// 浏览器按小写主机、省略默认端口的形式发送 Origin，启动时统一配置即可直接匹配。
func serializedOrigin(raw string) (string, error) {
	u, urlErr := url.Parse(raw)
	if urlErr != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Hostname() == "" ||
		u.Path != "" || strings.ContainsAny(raw, "?#*%") || !httpguts.ValidHostHeader(u.Host) {
		return "", fmt.Errorf("invalid HTTP(S) origin %q", raw)
	}
	host := strings.ToLower(u.Hostname())
	if strings.HasPrefix(u.Host, "[") || strings.Contains(host, ":") {
		addr, err := netip.ParseAddr(host)
		if err != nil || !addr.Is6() || !strings.HasPrefix(u.Host, "[") {
			return "", fmt.Errorf("invalid IPv6 origin %q", raw)
		}
		if addr.Is4In6() {
			// 浏览器用两个十六进制分组表示映射地址的 IPv4 部分。
			v4 := addr.As4()
			host = fmt.Sprintf("[::ffff:%x:%x]", binary.BigEndian.Uint16(v4[:2]), binary.BigEndian.Uint16(v4[2:]))
		} else {
			host = "[" + addr.String() + "]"
		}
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return "", fmt.Errorf("invalid origin port %q", raw)
		}
		if !(u.Scheme == "https" && n == 443 || u.Scheme == "http" && n == 80) {
			host += ":" + strconv.FormatUint(n, 10)
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return "", fmt.Errorf("empty origin port %q", raw)
	}
	return u.Scheme + "://" + host, nil
}

func (p *corsPolicy) apply(c *gin.Context) {
	req := c.Request
	h := c.Writer.Header()
	method := req.Method
	// 保留已有 Vary，仅补充影响 CORS 响应的请求头；通配符已覆盖全部维度。
	if !httpguts.HeaderValuesContainsToken(h.Values("Vary"), "*") {
		addVary(h, "Origin")
		if method == http.MethodOptions {
			addVary(h, "Access-Control-Request-Method")
			addVary(h, "Access-Control-Request-Headers")
		}
	}
	requestMethod := req.Header.Values("Access-Control-Request-Method")
	origin := req.Header.Get("Origin")
	preflight := method == http.MethodOptions && origin != "" && len(requestMethod) > 0 && requestMethod[0] != ""
	if preflight {
		defer c.AbortWithStatus(http.StatusNoContent)
		method = requestMethod[0]
		if len(requestMethod) != 1 || !p.allowsHeaders(req.Header.Values("Access-Control-Request-Headers")) {
			return
		}
	}
	if origin == "" || len(req.Header.Values("Origin")) != 1 || !p.methods[method] {
		return
	}
	if p.origins["*"] {
		origin = "*"
	} else if !p.origins[origin] {
		return
	}
	h.Set("Access-Control-Allow-Origin", origin)
	if p.credentials {
		h.Set("Access-Control-Allow-Credentials", "true")
	}
	if preflight {
		h.Set("Access-Control-Allow-Methods", method)
		if p.allowHeaders != "" {
			h.Set("Access-Control-Allow-Headers", p.allowHeaders)
		}
		if p.maxAge != "" {
			h.Set("Access-Control-Max-Age", p.maxAge)
		}
	} else if p.exposeHeaders != "" {
		h.Set("Access-Control-Expose-Headers", p.exposeHeaders)
	}
}

func addVary(h http.Header, name string) {
	if !httpguts.HeaderValuesContainsToken(h.Values("Vary"), name) {
		h.Add("Vary", name)
	}
}

func (p *corsPolicy) allowsHeaders(lines []string) bool {
	for _, line := range lines {
		for name := range strings.SplitSeq(line, ",") {
			name = textproto.TrimString(name)
			if name != "" && !p.headers[textproto.CanonicalMIMEHeaderKey(name)] {
				return false
			}
		}
	}
	return true
}
