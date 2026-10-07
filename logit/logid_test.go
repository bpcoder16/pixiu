package logit

import (
	"context"
	"strings"
	"testing"
)

func TestWithContextLogIDInitializesAndAppends(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "keep"))
	defer cancel()
	ctx := WithContextLogID(parent)
	first, ok := LogIDFromContext(ctx)
	if !ok || len(first) != 36 || ctx.Value(key{}) != "keep" {
		t.Fatalf("入口 ID 或父级值错误: id=%q, ok=%v", first, ok)
	}
	AddField(ctx, Str("stage", "entry"))
	child := WithContextLogID(ctx)
	second, ok := LogIDFromContext(child)
	if !ok || second == first || len(second) != 36 {
		t.Fatalf("再次调用未生成新 ID: %q", second)
	}
	want := "logId=" + first + ",logId=" + second + ",stage=entry"
	if got := strings.Join(collectCtxFieldValues(ctx, InfoLevel), ","); got != want {
		t.Fatalf("快捷方法改变了字段追加或共享语义: %q", got)
	}
	cancel()
	if child.Err() != context.Canceled {
		t.Fatal("未继承父级取消")
	}
}

func TestLogIDFromContextUsesLastNonemptyMetaString(t *testing.T) {
	for _, ctx := range []context.Context{nil, context.Background(), WithContext(context.Background())} {
		if id, ok := LogIDFromContext(ctx); ok || id != "" {
			t.Fatalf("无 ID 的 context 返回 %q, %v", id, ok)
		}
	}
	ctx := WithContext(context.Background())
	AddField(ctx, Str(LogId, "ordinary"))
	if _, ok := LogIDFromContext(ctx); ok {
		t.Fatal("普通字段不应作为传播 ID")
	}
	AddMeta(ctx, Str(LogId, "first"), Str(LogId, "last"), Str(LogId, ""), Int(LogId, 42),
		Defer(LogId, func() Field {
			t.Error("读取传播 ID 不应执行惰性字段")
			return Str(LogId, "deferred")
		}))
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	if id, ok := LogIDFromContext(child); !ok || id != "last" {
		t.Fatalf("派生 context 未读取最后一个非空字符串 meta: %q, %v", id, ok)
	}
	if _, ok := LogIDFromContext(NewContextScope(ctx)); ok {
		t.Fatal("独立日志作用域不应继承父级 ID")
	}
}
