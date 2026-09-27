package logit

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

type startKey struct{}

type durationState struct {
	started    time.Time
	mu         sync.RWMutex
	downstream map[string]time.Duration
}

// WithStart 在 ctx 中记录首次调用的时间点；已有起点时原样返回。
// 同时初始化请求级下游耗时表，不要求预先调用 WithContext。
func WithStart(ctx context.Context) context.Context {
	if durationFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, startKey{}, &durationState{
		started:    time.Now(),
		downstream: make(map[string]time.Duration),
	})
}

func durationFromContext(ctx context.Context) *durationState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(startKey{}).(*durationState)
	return state
}

// AddDownstreamDuration 记录一次下游调用的耗时。name 在请求内须唯一；
// 同一下游多次调用时由调用方提供不同名称。ctx 须先调用 WithStart。
func AddDownstreamDuration(ctx context.Context, name string, duration time.Duration) {
	state := durationFromContext(ctx)
	if state == nil {
		panic("logit: context not initialized, call logit.WithStart first")
	}
	if name == "" || name == "self" || name == "total" {
		panic("logit: invalid downstream duration name " + name)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if _, exists := state.downstream[name]; exists {
		panic("logit: duplicate downstream duration name " + name)
	}
	state.downstream[name] = duration
}

type downstreamDuration struct {
	name     string
	duration time.Duration
}

// durationFields 从请求级耗时表获取一致快照，再按耗时和名称输出下游、自身和总耗时。
func durationFields(fields []Field, state *durationState) []Field {
	total := time.Since(state.started)
	state.mu.RLock()
	entries := make([]downstreamDuration, 0, len(state.downstream))
	for name, duration := range state.downstream {
		entries = append(entries, downstreamDuration{name: name, duration: duration})
	}
	state.mu.RUnlock()
	slices.SortFunc(entries, func(a, b downstreamDuration) int {
		if a.duration < b.duration {
			return -1
		}
		if a.duration > b.duration {
			return 1
		}
		return strings.Compare(a.name, b.name)
	})

	// 创建新切片，避免改写调用方传入字段的空余容量。
	result := make([]Field, len(fields), len(fields)+len(entries)+2)
	copy(result, fields)
	var downstreamTotal time.Duration
	for _, entry := range entries {
		result = append(result, Dur(entry.name+"_duration_ms", entry.duration))
		downstreamTotal += entry.duration
	}
	return append(result, Dur("self_duration_ms", total-downstreamTotal), Dur("total_duration_ms", total))
}
