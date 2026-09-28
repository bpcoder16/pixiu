package logit

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type startKey struct{}

type durationState struct {
	started    time.Time
	mu         sync.RWMutex
	downstream map[string]time.Duration
	next       uint64
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

// AddDownstreamDurationAuto 自动为一次下游调用生成请求内唯一的 prefix_序号；
// 空 prefix、self、total 会 panic；有效 prefix 在 ctx 没有 WithStart 时跳过。
// 适合基础功能模块在不要求业务启用耗时统计时调用。
func AddDownstreamDurationAuto(ctx context.Context, prefix string, duration time.Duration) {
	if prefix == "" || prefix == "self" || prefix == "total" {
		panic("logit: invalid downstream duration prefix " + prefix)
	}
	state := durationFromContext(ctx)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.next++
	name := prefix + "_" + strconv.FormatUint(state.next, 10)
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
