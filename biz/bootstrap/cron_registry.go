package bootstrap

import (
	"fmt"
	"go/token"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/bpcoder16/pixiu/infra/cronx"
)

// CronRegistry 收集由应用持有的任务声明，不读配置或执行业务；零值可用。
// MustRegister 与 Handlers 可并发调用，首次使用后不得复制注册器。
type CronRegistry struct {
	mu       sync.RWMutex
	handlers []CronHandler
}

// MustNewCronRegistry 批量登记包级具名函数，默认名称为原始函数名，区分大小写。
// 非法 Handler 或名称冲突会 panic；闭包、方法值和泛型函数须改用 MustRegister 显式命名。
func MustNewCronRegistry(handlers ...cronx.Handler) *CronRegistry {
	registry := &CronRegistry{}
	for _, handler := range handlers {
		registry.MustRegister(handler)
	}
	return registry
}

// MustRegister 追加任务声明；可选 name 最多一个，提供时按原值作为稳定名称。
// 未提供名称时，只为包级具名函数自动取名并保留大小写；名称为空、含首尾空白、重复或 Handler 为 nil 均 panic。
// 自动名称会随函数重命名改变；YAML 绑定和锁标识需要兼容时，应显式固定名称。
func (r *CronRegistry) MustRegister(handler cronx.Handler, name ...string) {
	if handler == nil {
		panic("bootstrap: nil Cron handler")
	}
	if len(name) > 1 {
		panic("bootstrap: expected at most one Cron handler name")
	}
	var resolved string
	if len(name) == 1 {
		resolved = name[0]
	} else {
		resolved = cronFunctionName(handler)
	}
	if resolved == "" || strings.TrimSpace(resolved) != resolved {
		panic(fmt.Errorf("bootstrap: invalid Cron handler %q", resolved))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.handlers {
		if existing.Name == resolved {
			panic(fmt.Errorf("bootstrap: duplicate Cron handler %q", resolved))
		}
	}
	r.handlers = append(r.handlers, CronHandler{
		Name:    resolved,
		Handler: handler,
	})
}

// Handlers 按登记顺序返回独立清单；后续登记不改变已交付的清单或已构造的调度器。
func (r *CronRegistry) Handlers() []CronHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Clone(r.handlers)
}

func cronFunctionName(handler cronx.Handler) string {
	function := runtime.FuncForPC(reflect.ValueOf(handler).Pointer())
	var fullName string
	if function != nil {
		fullName = function.Name()
	}
	// 先去掉导入路径，再只接受 package.Function；避免将 func1、-fm 或泛型符号作为任务名。
	symbol := fullName[strings.LastIndexByte(fullName, '/')+1:]
	_, name, found := strings.Cut(symbol, ".")
	if !found || !token.IsIdentifier(name) {
		panic(fmt.Errorf("bootstrap: cannot infer Cron handler name from %q; provide an explicit name", fullName))
	}
	return name
}
