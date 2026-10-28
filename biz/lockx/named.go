package lockx

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// 登记只发生在启动阶段；默认引用与命名表一起发布，避免观察到不完整的登记结果。
var executors = struct {
	sync.RWMutex
	named   map[string]*Executor
	primary *Executor
}{
	named: make(map[string]*Executor),
}

// NewNamed 创建并登记命名执行器；名称不能为空白，重复名称返回错误且不替换已有实例。
// 应在启动阶段完成全部登记后再运行业务；后端的资源始终由调用方关闭。
func NewNamed(name string, backend Backend) (*Executor, error) {
	return registerExecutor(name, backend, false)
}

// NewDefault 创建并按 name 登记执行器，同时设为默认；已有默认实例时返回错误。
// 创建失败不占用名称或默认位置；其余约束与 NewNamed 相同。
func NewDefault(name string, backend Backend) (*Executor, error) {
	return registerExecutor(name, backend, true)
}

func registerExecutor(name string, backend Backend, isDefault bool) (*Executor, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("lockx: empty name")
	}
	executors.Lock()
	defer executors.Unlock()
	if isDefault && executors.primary != nil {
		return nil, errors.New("lockx: default executor is already registered")
	}
	if _, exists := executors.named[name]; exists {
		return nil, fmt.Errorf("lockx: executor %q is already registered", name)
	}
	executor, err := New(backend)
	if err != nil {
		return nil, err
	}
	executors.named[name] = executor
	if isDefault {
		executors.primary = executor
	}
	return executor, nil
}

// Default 返回显式登记的默认执行器，与对应 Named(name) 是同一实例；未登记时 panic。
// 登记表不拥有后端资源，调用方须在关闭后端之前停止使用执行器。
func Default() *Executor {
	executors.RLock()
	defer executors.RUnlock()
	if executors.primary == nil {
		panic("lockx: default executor is not registered")
	}
	return executors.primary
}

// Named 返回指定命名执行器；名称不存在时 panic，不自动创建或回退默认实例。
func Named(name string) *Executor {
	executors.RLock()
	defer executors.RUnlock()
	executor, exists := executors.named[name]
	if !exists {
		panic(fmt.Sprintf("lockx: executor %q is not registered", name))
	}
	return executor
}
