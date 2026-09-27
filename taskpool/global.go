package taskpool

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrNoDefault 表示尚未注册默认任务池。
var ErrNoDefault = errors.New("taskpool: no default pool")

var defaultPool atomic.Pointer[Pool]

// NewDefault 创建并启动任务池，成功后注册为默认池；失败时默认池保持不变。
// 其他通过 New 创建的实例不受影响；替换已有默认池时，旧池仍由调用方关闭。
func NewDefault(stopCtx context.Context, cfg Config) (*Pool, error) {
	p, err := New(stopCtx, cfg)
	if err != nil {
		return nil, err
	}
	SetDefault(p)
	return p, nil
}

// Default 返回当前默认任务池；未注册时返回 nil。
func Default() *Pool { return defaultPool.Load() }

// SetDefault 注册默认任务池；传入 nil 可清除注册。旧池仍由调用方关闭。
func SetDefault(p *Pool) { defaultPool.Store(p) }

// Swap 原子替换默认任务池并返回旧池；传入 nil 可清除注册。
func Swap(p *Pool) *Pool { return defaultPool.Swap(p) }

// Submit 向当前默认任务池提交任务；未注册时返回 ErrNoDefault。
func Submit(ctx context.Context, name string, fn func(context.Context) error) error {
	p := Default()
	if p == nil {
		return ErrNoDefault
	}
	return p.Submit(ctx, name, fn)
}

// Wait 等待当前默认任务池的停机排空；与 Shutdown 合计只能对同一池调用一次。
func Wait() error {
	p := Default()
	if p == nil {
		return ErrNoDefault
	}
	return p.Wait()
}

// Shutdown 主动关闭当前默认任务池；与 Wait 合计只能对同一池调用一次。
func Shutdown() error {
	p := Default()
	if p == nil {
		return ErrNoDefault
	}
	return p.Shutdown()
}
