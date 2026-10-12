package httpauth

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// Registry 管理命名认证方案，首次 Resolve 后冻结。零值可用，使用后不得复制。
// 应在注册路由前完成所有 Register；认证器本身由调用方保证并发安全。
type Registry struct {
	mu      sync.Mutex
	schemes map[string]Authenticator
	frozen  bool
}

func NewRegistry() *Registry {
	return &Registry{}
}

// Register 注册唯一方案名；拒绝空白、首尾空白、nil 实现及冻结后的修改。
func (r *Registry) Register(name string, authenticator Authenticator) error {
	if name == "" || strings.TrimSpace(name) != name {
		return fmt.Errorf("auth: invalid scheme name %q", name)
	}
	if isNilAuthenticator(authenticator) {
		return fmt.Errorf("auth: nil authenticator for scheme %q", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("auth: registry is frozen")
	}
	if _, exists := r.schemes[name]; exists {
		return fmt.Errorf("auth: duplicate scheme %q", name)
	}
	if r.schemes == nil {
		r.schemes = make(map[string]Authenticator)
	}
	r.schemes[name] = authenticator
	return nil
}

// MustRegister 用于启动阶段，注册失败时 panic。
func (r *Registry) MustRegister(name string, authenticator Authenticator) {
	if err := r.Register(name, authenticator); err != nil {
		panic(err)
	}
}

// Resolve 冻结注册表并返回指定实现；未知名字返回错误，不使用其他方案兜底。
// HTTP 适配器在构造路由时调用，请求热路径无需访问注册表。
func (r *Registry) Resolve(name string) (Authenticator, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
	authenticator, ok := r.schemes[name]
	if !ok {
		return nil, fmt.Errorf("auth: unknown scheme %q", name)
	}
	return authenticator, nil
}

func isNilAuthenticator(authenticator Authenticator) bool {
	if authenticator == nil {
		return true
	}
	// interface 可能持有带类型的 nil，须在启动时拒绝而非请求到来后 panic。
	value := reflect.ValueOf(authenticator)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
