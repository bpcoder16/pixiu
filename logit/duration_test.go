package logit

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestNewDurationScopeIsolatesTiming(t *testing.T) {
	type valueKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), valueKey{}, "kept"))
	defer cancel()
	parent = WithStart(parent)
	AddDownstreamDurationAuto(parent, "parent", time.Millisecond)
	child := NewDurationScope(parent)
	AddDownstreamDurationAuto(child, "child", time.Millisecond)
	// 普通派生和 WithStart 仍共享当前作用域,不能重置已有计时。
	AddDownstreamDurationAuto(WithStart(child), "nested", time.Millisecond)
	if child.Value(valueKey{}) != "kept" {
		t.Fatal("丢失父 context 值")
	}
	logger, output := newTestLogger(t, OptEncoder(DefaultJSONEncoder))
	old := Default()
	SetDefault(logger)
	defer SetDefault(old)
	for _, test := range []struct {
		ctx    context.Context
		want   []string
		absent []string
	}{
		{
			ctx:    parent,
			want:   []string{"parent_1_duration_ms"},
			absent: []string{"child_1_duration_ms", "nested_2_duration_ms"},
		},
		{
			ctx:    child,
			want:   []string{"child_1_duration_ms", "nested_2_duration_ms"},
			absent: []string{"parent_1_duration_ms"},
		},
	} {
		output.Reset()
		InfoDuration(test.ctx, "timing")
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		for _, key := range test.want {
			if _, ok := record[key]; !ok {
				t.Errorf("缺少 %s: %v", key, record)
			}
		}
		for _, key := range test.absent {
			if _, ok := record[key]; ok {
				t.Errorf("计时串作用域 %s: %v", key, record)
			}
		}
	}
	if durationFromContext(child).started.Before(durationFromContext(parent).started) || durationFromContext(child) == durationFromContext(parent) {
		t.Fatal("未创建独立计时起点")
	}
	cancel()
	if child.Err() != context.Canceled {
		t.Fatal("未继承取消")
	}
}
