package realtime

import (
	"context"
	"sync"
	"testing"
	"time"
	"uuid"
)

func TestNamedServerRegistrationAndLifecycle(t *testing.T) {
	newServer := func() *Server {
		s, err := NewServer(baseConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Cancel(); await(t, s.Done()) })
		return s
	}
	app, other := newServer(), newServer()
	name := uuid.New().String()
	if err := Register(name, app); err != nil {
		t.Fatal(err)
	}
	if Named(name) != app || app.Ready() {
		t.Fatal("登记应返回同一实例，且不自动运行")
	}
	if err := Register(name, other); err == nil {
		t.Fatal("重复名称应被拒绝")
	}
	if err := Register(name+"-alias", app); err == nil {
		t.Fatal("同一 Server 不能登记多个实例名")
	}
	if err := Register(" ", other); err == nil {
		t.Fatal("空白名称应被拒绝")
	}
	if err := Register(name+"-nil", nil); err == nil {
		t.Fatal("nil Server 应被拒绝")
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if Named(name) != app {
				t.Error("并发读取不是同一实例")
			}
		})
	}
	wg.Wait()
	run := make(chan error, 1)
	go func() { run <- app.Run() }()
	deadline := time.Now().Add(3 * time.Second)
	for !app.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("Server 未启动")
		}
		time.Sleep(time.Millisecond)
		select {
		case err := <-run:
			t.Fatalf("提前退出: %v", err)
		default:
		}
	}
	if err := Register(name+"-running", other); err != nil {
		t.Fatal(err)
	}
	if err := Register(name+"-started", app); err == nil {
		t.Fatal("运行中的 Server 应被拒绝")
	}
	app.Cancel()
	await(t, app.Done())
	if err := await(t, run); err != nil {
		t.Fatal(err)
	}
	if Named(name) != app {
		t.Fatal("关闭不替换命名实例")
	}
	if _, err := Named(name).Broadcast(context.Background(), PushMessage{}); err == nil {
		t.Fatal("关闭后仍接受推送")
	}
	missing := false
	func() { defer func() { missing = recover() != nil }(); Named(name + "-missing") }()
	if !missing {
		t.Fatal("未知实例应 panic")
	}
}
