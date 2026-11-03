package realtime

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRouterDispatch(t *testing.T) {
	var r Router
	ctx, session := t.Context(), &Session{}
	var request *Message
	calls := 0
	// 用不同的返回值识别命中分支，同时验证回调收到的就是调用方的对象。
	handler := func(result error) Handler {
		return func(gotCtx context.Context, gotSession *Session, gotRequest *Message) error {
			t.Helper()
			if gotCtx != ctx || gotSession != session || gotRequest != request {
				t.Fatal("分发改变了回调参数")
			}
			calls++
			return result
		}
	}
	first, second, miss := errors.New("first"), errors.New("second"), errors.New("miss")
	if err := r.Handle("chat.user", "open", handler(first)); err != nil {
		t.Fatal(err)
	}
	if err := r.Handle("chat", "user.open", handler(second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Handle("chat.user", "open", handler(second)); err == nil {
		t.Fatal("重复注册应报错并保留原 handler")
	}
	dispatch := r.seal(handler(miss))
	for _, tc := range []struct {
		scene, action string
		want          error
	}{
		{"chat.user", "open", first},
		{"chat", "user.open", second},
		{"chat", "open", miss},
		{"Chat.user", "open", miss},
		{"chat.user", "Open", miss},
	} {
		request = &Message{scene: tc.scene, action: tc.action}
		calls = 0
		if got := dispatch(ctx, session, request); got != tc.want || calls != 1 {
			t.Errorf("%s/%s 分发结果 %v，调用 %d 次", tc.scene, tc.action, got, calls)
		}
	}
}

func TestRouterRegistrationAndBuild(t *testing.T) {
	var r Router
	accept := func(context.Context, *Session, *Message) error { return nil }
	for _, tc := range []struct {
		scene, action string
		handler       Handler
	}{
		{"", "open", accept},
		{"room", "read/*", accept},
		{strings.Repeat("a", 129), "open", accept},
		{"room", "open", nil},
	} {
		if err := r.Handle(tc.scene, tc.action, tc.handler); err == nil {
			t.Errorf("接纳了无效注册 %q/%q", tc.scene, tc.action)
		}
	}
	if err := r.Handle("room", strings.Repeat("a", 128), accept); err != nil {
		t.Fatalf("应能登记合法路由: %v", err)
	}
	r.seal(accept)
	if err := r.Handle("room", "another", accept); err == nil {
		t.Fatal("构建后仍能修改路由")
	}
}
