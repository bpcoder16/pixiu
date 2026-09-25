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

func collectCtxFieldValues(ctx context.Context, lineLevel Level) []string {
	var fields []string
	eachVisible(ctx, lineLevel, func(f Field) {
		fields = append(fields, f.Key+"="+f.str)
	})
	return fields
}

func newTestContextWithLogID() context.Context {
	ctx := WithContext(context.Background())
	AddMeta(ctx, Str("logId", NewLogID()))
	return ctx
}

func TestWithContextIdempotent(t *testing.T) {
	ctx := WithContext(context.Background())
	s1 := findStore(ctx, ctxKeyFields)
	ctx2 := WithContext(ctx)
	if findStore(ctx2, ctxKeyFields) != s1 {
		t.Error("WithContext should reuse existing store")
	}
}

func TestAddFieldPreservesDuplicates(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("a", "1"), Str("b", "2"))
	AddField(ctx, Str("a", "3"))

	if got := strings.Join(collectCtxFieldValues(ctx, InfoLevel), ","); got != "a=1,b=2,a=3" {
		t.Errorf("same-key fields = %q, want a=1,b=2,a=3", got)
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

func TestDuplicateFieldVisibility(t *testing.T) {
	ctx := WithContext(context.Background())
	AddField(ctx, Str("payload", "all"))
	AddDebugField(ctx, Str("payload", "debug"))

	if got := strings.Join(collectCtxFieldValues(ctx, InfoLevel), ","); got != "payload=all" {
		t.Errorf("info fields = %q, want payload=all", got)
	}
	if got := strings.Join(collectCtxFieldValues(ctx, DebugLevel), ","); got != "payload=all,payload=debug" {
		t.Errorf("debug fields = %q, want both payload values", got)
	}
}

func TestLogIDAllowedAsDebugField(t *testing.T) {
	ctx := WithContext(context.Background())
	AddDebugField(ctx, Str("logId", "debug-only"))

	if got := collectCtxFields(ctx, InfoLevel); len(got) != 0 {
		t.Errorf("info fields = %v, want none", got)
	}
	if got := strings.Join(collectCtxFieldValues(ctx, DebugLevel), ","); got != "logId=debug-only" {
		t.Errorf("debug fields = %q, want logId=debug-only", got)
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
	AddMeta(ctx, Str("logId", "L1"))
	AddField(ctx, Str("stage", "parent"))

	child, cancel := context.WithCancel(ctx)
	defer cancel()
	AddField(child, Str("stage", "child"))
	AddMeta(child, Str("trace", "T1"))

	want := "logId=L1,trace=T1,stage=parent,stage=child"
	if got := strings.Join(collectCtxFieldValues(ctx, InfoLevel), ","); got != want {
		t.Errorf("parent fields = %q, want %q", got, want)
	}
	if got := strings.Join(collectCtxFieldValues(child, InfoLevel), ","); got != want {
		t.Errorf("derived fields = %q, want %q", got, want)
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
		name string
		add  func(context.Context, ...Field)
	}{
		{"AddField", AddField},
		{"AddDebugField", AddDebugField},
		{"AddMeta", AddMeta},
	}
	for _, tt := range tests {
		for _, key := range []string{levelKey, tsKey, callerKey, msgKey} {
			t.Run(tt.name+"/"+key, func(t *testing.T) {
				ctx := WithContext(context.Background())
				expectReservedFieldPanic(t, func() {
					tt.add(ctx, Str("safe", "value"), Str(key, "wrong"))
				})
				if got := collectCtxFields(ctx, DebugLevel); len(got) != 0 {
					t.Error("包含保留字段的批次不应部分写入")
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
	}{
		{"AddField", AddField},
		{"AddDebugField", AddDebugField},
		{"AddMeta", AddMeta},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := WithContext(context.Background())
			tt.add(ctx, Str("safe", "value"), Str(longKey, "accepted"))
			want := "safe=value," + longKey + "=accepted"
			if got := strings.Join(collectCtxFieldValues(ctx, DebugLevel), ","); got != want {
				t.Errorf("字段 = %q, want %q", got, want)
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
