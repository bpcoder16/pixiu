package logit

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func collectCtxFields(ctx context.Context, lineLevel Level) []string {
	var keys []string
	eachVisible(ctx, lineLevel, func(f Field) {
		keys = append(keys, f.Key)
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
	if f, ok := findStore(ctx, ctxKeyFields).get("a"); !ok || f.field.str != "3" {
		t.Errorf("override value = %q ok=%v", f.field.str, ok)
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

func TestEachVisibleMetaBeforeFields(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("fieldA", "a"), Str("fieldB", "b"))
	AddMeta(ctx, Str("metaA", "a"), Str("metaB", "b"))

	if got := strings.Join(collectCtxFields(ctx, InfoLevel), ","); got != "metaA,metaB,fieldA,fieldB" {
		t.Fatalf("visible field order = %q, want metaA,metaB,fieldA,fieldB", got)
	}
}

func TestFieldsSharedAcrossDerivedContext(t *testing.T) {
	ctx := WithContext(context.Background())
	SetLogID(ctx, "L1")
	AddField(ctx, Str("stage", "parent"))

	child, cancel := context.WithCancel(ctx)
	defer cancel()
	AddField(child, Str("stage", "child"))
	AddMeta(child, Str("trace", "T1"))

	if f, _ := findStore(ctx, ctxKeyFields).get("stage"); f.field.str != "child" {
		t.Errorf("parent stage = %q, want child", f.field.str)
	}
	if _, ok := findStore(ctx, ctxKeyMeta).get("trace"); !ok {
		t.Error("meta added in derived context should be visible in parent")
	}
	if got := LogID(child); got != "L1" {
		t.Errorf("derived logId = %q, want L1", got)
	}
}

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
		name  string
		add   func(context.Context, ...Field)
		store ctxKey
	}{
		{"AddField", AddField, ctxKeyFields},
		{"AddDebugField", AddDebugField, ctxKeyFields},
		{"AddMeta", AddMeta, ctxKeyMeta},
	}
	for _, tt := range tests {
		for _, key := range []string{logIdKey, levelKey, tsKey, callerKey, msgKey} {
			t.Run(tt.name+"/"+key, func(t *testing.T) {
				ctx := WithContext(context.Background())
				SetLogID(ctx, "original")
				expectReservedFieldPanic(t, func() {
					tt.add(ctx, Str("safe", "value"), Str(key, "wrong"))
				})
				if _, ok := findStore(ctx, tt.store).get("safe"); ok {
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
		name  string
		add   func(context.Context, ...Field)
		store ctxKey
	}{
		{"AddField", AddField, ctxKeyFields},
		{"AddDebugField", AddDebugField, ctxKeyFields},
		{"AddMeta", AddMeta, ctxKeyMeta},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := WithContext(context.Background())
			tt.add(ctx, Str("safe", "value"), Str(longKey, "accepted"))
			if got, ok := findStore(ctx, tt.store).get("safe"); !ok || got.field.str != "value" {
				t.Errorf("普通字段未保存: %#v, %v", got, ok)
			}
			if got, ok := findStore(ctx, tt.store).get(longKey); !ok || got.field.str != "accepted" {
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
		}(i)
	}
	wg.Wait()
}
