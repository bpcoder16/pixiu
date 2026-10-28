package lockx

import (
	"context"
	"errors"
	"reflect"
)

// Backend 同时支持阻塞与非阻塞获取，供执行器绑定；共享使用时须支持并发调用。
type Backend interface {
	Locker
	TryLocker
}

// Executor 绑定可复用的锁后端，调用时只需传入请求 context、业务 key 和回调。
// 不保存单次锁句柄，不创建或关闭后端资源；零值不可用，须通过 New 系列函数构造。
type Executor struct {
	backend Backend
}

// New 创建独立执行器，不登记默认或命名实例；nil（包括带类型的 nil）后端返回错误。
// 后端的配置与资源由调用方管理，发布共享执行器后不应修改后端配置。
func New(backend Backend) (*Executor, error) {
	if backend == nil {
		return nil, errors.New("lockx: nil backend")
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, errors.New("lockx: nil backend")
		}
	default:
		// 其余 Kind 不支持 nil，无需调用 IsNil。
	}
	return &Executor{backend: backend}, nil
}

// Do 等待获取锁，同步执行 fn 并释放；沿用包级 Do 的期限、错误与 panic 语义。
func (e *Executor) Do(ctx context.Context, key string, fn func(context.Context) error) error {
	if e == nil || e.backend == nil {
		return errors.New("lockx: uninitialized executor")
	}
	return Do(ctx, e.backend, key, fn)
}

// TryDo 只尝试一次；竞争失败返回 false、nil，executed 只表示是否进入过 fn。
// 其余语义沿用包级 TryDo；执行器名称不参与 key 的构造。
func (e *Executor) TryDo(ctx context.Context, key string, fn func(context.Context) error) (bool, error) {
	if e == nil || e.backend == nil {
		return false, errors.New("lockx: uninitialized executor")
	}
	return TryDo(ctx, e.backend, key, fn)
}
