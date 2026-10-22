package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
)

func TestNewAppManagerInvalidTimeout(t *testing.T) {
	for _, timeouts := range [][]time.Duration{
		{-time.Second},
		{time.Nanosecond},
		{5*time.Second - time.Nanosecond},
		{5 * time.Second, 5 * time.Second},
	} {
		if manager, err := NewAppManager(timeouts...); err == nil || manager != nil {
			t.Fatalf("无效关闭期限被接受: %v, %v", timeouts, err)
		}
	}
}

func TestNewAppManagerValidTimeout(t *testing.T) {
	for _, timeouts := range [][]time.Duration{nil, {0}, {5 * time.Second}, {5*time.Second + time.Nanosecond}} {
		if manager, err := NewAppManager(timeouts...); err != nil || manager == nil {
			t.Fatalf("有效关闭期限被拒绝: %v, %v", timeouts, err)
		}
	}
}

func TestNewAppManagerCustomTimeout(t *testing.T) {
	manager, err := NewAppManager(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer close(release)
	if err := manager.Register(apprun.Service{
		Name: "blocked",
		Run:  func() error { return nil },
		Stop: func() error {
			<-release
			return nil
		},
		Wait: func(context.Context) error {
			return nil
		},
		Cancel: func() {},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, apprun.ErrShutdownTimeout) {
			t.Fatalf("未保留总关闭超时: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("自定义关闭期限未生效")
	}
}
