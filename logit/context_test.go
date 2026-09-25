package logit

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func collectCtxFields(ctx context.Context, lineLevel Level) []string {
	var keys []string
	eachVisible(ctx, lineLevel, func(f Field) error {
		keys = append(keys, f.Key)
		return nil
	})
	return keys
}

func TestWithContextIdempotent(t *testing.T) {
	ctx := WithContext(context.Background())
	s1 := findStore(ctx, ctxKeyFields)
	ctx2 := WithContext(ctx)
	if findStore(ctx2, ctxKeyFields) != s1 {
		t.Error("WithContext should reuse existing store")
	}
}

func TestAddFieldOrderAndOverride(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("a", "1"), Str("b", "2"))
	AddField(ctx, Str("a", "3")) // 覆盖,位置保持首次添加处

	got := strings.Join(collectCtxFields(ctx, InfoLevel), ",")
	if got != "a,b" {
		t.Errorf("order after override = %q, want a,b", got)
	}
	if f, ok := FindField(ctx, "a"); !ok || f.str != "3" {
		t.Errorf("override value = %q ok=%v", f.str, ok)
	}
}

func TestFieldVisibility(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("uid", "42"))
	AddDebugField(ctx, Str("payload", "big"))

	if got := strings.Join(collectCtxFields(ctx, DebugLevel), ","); got != "uid,payload" {
		t.Errorf("debug line sees %q, want uid,payload", got)
	}
	if got := strings.Join(collectCtxFields(ctx, InfoLevel), ","); got != "uid" {
		t.Errorf("info line sees %q, want uid", got)
	}
}

func TestMetaSharedAcrossFork(t *testing.T) {
	ctx := WithContext(context.Background())
	SetLogID(ctx, "L1")
	AddField(ctx, Str("stage", "parent"))

	child := ForkContext(ctx)
	AddField(child, Str("stage", "child")) // 只影响分支
	AddMeta(child, Str("trace", "T1"))     // meta 共享,父 ctx 也能看到

	if f, _ := FindField(ctx, "stage"); f.str != "parent" {
		t.Errorf("parent stage = %q, want parent", f.str)
	}
	if f, _ := FindField(child, "stage"); f.str != "child" {
		t.Errorf("child stage = %q, want child", f.str)
	}
	if _, ok := FindMeta(ctx, "trace"); !ok {
		t.Error("meta added in child should be visible in parent (shared store)")
	}
}

func TestDeleteField(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("a", "1"), Str("b", "2"))
	DeleteField(ctx, "a")
	if _, ok := FindField(ctx, "a"); ok {
		t.Error("a should be deleted")
	}
	if got := strings.Join(collectCtxFields(ctx, InfoLevel), ","); got != "b" {
		t.Errorf("after delete = %q, want b", got)
	}
	DeleteField(ctx, "missing") // 不存在则忽略
}

func TestCopyAllFields(t *testing.T) {
	src := WithContext(context.Background())
	AddField(src, Str("stage", "req"))
	SetLogID(src, "L1")

	dest := CopyAllFields(context.Background(), src)
	if f, ok := FindField(dest, "stage"); !ok || f.str != "req" {
		t.Errorf("copied normal field = %v %v", f, ok)
	}
	if _, ok := FindMeta(dest, "logId"); !ok {
		t.Error("meta field should be copied")
	}
	// 修改 dest 不影响 src
	AddField(dest, Str("stage", "bg"))
	if f, _ := FindField(src, "stage"); f.str != "req" {
		t.Error("src should be unaffected")
	}
}

func TestCopyAllFieldsWithSharedStoresDoesNotDeadlock(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("stage", "same"))
	SetLogID(ctx, "L1")

	done := make(chan context.Context, 1)
	go func() { done <- CopyAllFields(ctx, ctx) }()
	select {
	case copied := <-done:
		if f, ok := FindField(copied, "stage"); !ok || f.str != "same" {
			t.Fatalf("shared normal field = %v %v", f, ok)
		}
		if f, ok := FindMeta(copied, "logId"); !ok || f.str != "L1" {
			t.Fatalf("shared meta field = %v %v", f, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("CopyAllFields must not call back into a shared store while holding its read lock")
	}
}

func TestRangeStopsOnError(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("a", "1"), Str("b", "2"))
	var seen []string
	RangeFields(ctx, func(f Field) error {
		seen = append(seen, f.Key)
		if f.Key == "a" {
			return errStop
		}
		return nil
	})
	if len(seen) != 1 {
		t.Errorf("range should stop early, seen = %v", seen)
	}
}

var errStop = &stopError{}

type stopError struct{}

func (*stopError) Error() string { return "stop" }

func TestAddFieldPanicsWithoutInit(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("AddField on uninitialized ctx should panic")
		}
	}()
	AddField(context.Background(), Str("k", "v"))
}

func expectReservedFieldPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("写入保留字段应 panic")
		}
	}()
	fn()
}

func TestReservedFieldsContextWritesPanic(t *testing.T) {
	tests := []struct {
		name string
		add  func(context.Context, ...Field)
		find func(context.Context, string) (Field, bool)
	}{
		{"AddField", AddField, FindField},
		{"AddDebugField", AddDebugField, FindField},
		{"AddMeta", AddMeta, FindMeta},
	}
	for _, tt := range tests {
		for _, key := range []string{logIdKey, levelKey, tsKey, callerKey, msgKey} {
			t.Run(tt.name+"/"+key, func(t *testing.T) {
				ctx := WithContext(context.Background())
				SetLogID(ctx, "original")
				expectReservedFieldPanic(t, func() {
					tt.add(ctx, Str("safe", "value"), Str(key, "wrong"))
				})
				if _, ok := tt.find(ctx, "safe"); ok {
					t.Error("包含保留字段的批次不应部分写入")
				}
				if got := LogID(ctx); got != "original" {
					t.Errorf("logId = %q, want original", got)
				}
			})
		}
	}
}

func TestContextAcceptsLongUnicodeFieldKeys(t *testing.T) {
	longKey := strings.Repeat("中", 33)
	tests := []struct {
		name string
		add  func(context.Context, ...Field)
		find func(context.Context, string) (Field, bool)
	}{
		{"AddField", AddField, FindField},
		{"AddDebugField", AddDebugField, FindField},
		{"AddMeta", AddMeta, FindMeta},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := WithContext(context.Background())
			tt.add(ctx, Str("safe", "value"), Str(longKey, "accepted"))
			if got, ok := tt.find(ctx, "safe"); !ok || got.str != "value" {
				t.Errorf("普通字段未保存: %#v, %v", got, ok)
			}
			if got, ok := tt.find(ctx, longKey); !ok || got.str != "accepted" {
				t.Errorf("长字段名未保存: %#v, %v", got, ok)
			}
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	ctx := WithContext(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			AddField(ctx, Int("f", n))
			collectCtxFields(ctx, InfoLevel)
			ForkContext(ctx)
		}(i)
	}
	wg.Wait()
}
