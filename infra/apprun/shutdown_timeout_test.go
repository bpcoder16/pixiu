package apprun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestShutdownDeadlineWithSlowErrorFormatting(t *testing.T) {
	for _, tc := range []struct {
		name        string
		operation   string
		logDelay    time.Duration
		wantTimeout bool
		wantCancel  int
	}{
		{
			name:      "停收错误保留且不误报超时",
			operation: "stop",
		},
		{
			name:       "等待错误保留且仍取消兜底",
			operation:  "wait",
			wantCancel: 1,
		},
		{
			name:        "末尾日志越过期限仍报告超时",
			operation:   "stop",
			logDelay:    1500 * time.Millisecond,
			wantTimeout: true,
			wantCancel:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager, err := New(time.Second)
				if err != nil {
					t.Fatal(err)
				}
				failure := &delayedShutdownError{logDelay: tc.logDelay}
				// 超时返回后日志协程仍可继续收尾，测试退出前推进虚拟时间让其结束。
				defer func() {
					time.Sleep(tc.logDelay)
					synctest.Wait()
				}()
				canceled := 0
				register(t, manager, Service{
					Name: "service",
					Run: func() error {
						t.Error("预取消仍运行")
						return nil
					},
					Stop: func() error {
						if tc.operation == "stop" {
							return failure
						}
						return nil
					},
					Wait: func(context.Context) error {
						if tc.operation == "wait" {
							return failure
						}
						return nil
					},
					Cancel: func() { canceled++ },
				})
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				// 慢格式化不能拖延返回；关闭协程中的日志求值仍受共享期限约束。
				started := time.Now()
				err = manager.Run(ctx)
				wantDuration := time.Duration(0)
				if tc.wantTimeout {
					wantDuration = time.Second
				}
				if elapsed := time.Since(started); elapsed != wantDuration {
					t.Fatalf("错误格式化拖延关闭: got=%s, want=%s", elapsed, wantDuration)
				}
				if !errors.Is(err, failure) {
					t.Fatalf("丢失关闭错误: %v", err)
				}
				if errors.Is(err, ErrShutdownTimeout) != tc.wantTimeout || errors.Is(err, context.DeadlineExceeded) != tc.wantTimeout {
					t.Fatalf("关闭超时判定错误: wantTimeout=%v, err=%v", tc.wantTimeout, err)
				}
				if canceled != tc.wantCancel {
					t.Fatalf("取消次数错误: got=%d, want=%d", canceled, tc.wantCancel)
				}
			})
		})
	}
}

// Format 模拟慢错误格式化，Error 独立控制关闭协程中日志字段求值的耗时。
type delayedShutdownError struct {
	logDelay time.Duration
}

func (e *delayedShutdownError) Error() string {
	time.Sleep(e.logDelay)
	return "关闭失败"
}

func (*delayedShutdownError) Format(state fmt.State, _ rune) {
	time.Sleep(2 * time.Second)
	_, _ = fmt.Fprint(state, "关闭失败")
}

func TestSharedDeadlineCancelsUnfinishedServices(t *testing.T) {
	for _, phase := range []string{"stop", "wait", "run"} {
		t.Run(phase, func(t *testing.T) {
			m, _ := New(60 * time.Millisecond)
			release := make(chan struct{})
			defer close(release)
			canceled := make(chan struct{}, 2)
			stopped := make(chan struct{}, 2)
			started := make(chan struct{}, 2)
			failure := errors.New("运行失败")
			for _, name := range []string{"one", "two"} {
				register(t, m, Service{
					Name: name,
					Run: func() error {
						started <- struct{}{}
						if phase == "run" {
							<-release
							return nil
						}
						return failure
					},
					Stop: func() error {
						stopped <- struct{}{}
						if phase == "stop" {
							<-release
						}
						return nil
					},
					Wait: func(ctx context.Context) error {
						if phase == "wait" {
							<-release
						}
						return nil
					},
					Cancel: func() { canceled <- struct{}{} },
				})
			}
			completedStop := make(chan struct{})
			register(t, m, Service{
				Name: "completed",
				Run: func() error {
					<-completedStop
					return nil
				},
				Stop: func() error {
					close(completedStop)
					return nil
				},
				Wait:   func(context.Context) error { return nil },
				Cancel: func() { t.Error("已排空服务被取消") },
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := run(m, ctx)
			for range 2 {
				await(t, started)
			}
			cancel()
			for range 2 {
				await(t, stopped)
			}
			err := await(t, done)
			if !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("未报告总超时: %v", err)
			}
			if phase != "run" && !errors.Is(err, failure) {
				t.Fatalf("丢失已完成运行错误: %v", err)
			}
			for _, name := range []string{"one", "two"} {
				if !strings.Contains(err.Error(), fmt.Sprintf("service %q %s:", name, phase)) {
					t.Errorf("超时错误缺少服务 %q 的 %s 阶段: %v", name, phase, err)
				}
			}
			if strings.Contains(err.Error(), `service "completed"`) {
				t.Errorf("超时错误包含已完成服务: %v", err)
			}
			for range 2 {
				await(t, canceled)
			}
		})
	}
}

func TestDeadlineStartsWhenStopping(t *testing.T) {
	m, _ := New(40 * time.Millisecond)
	started, stopped := make(chan struct{}), make(chan struct{})
	register(t, m, Service{
		Name: "s",
		Run: func() error {
			close(started)
			<-stopped
			return nil
		},
		Stop: func() error {
			close(stopped)
			return nil
		},
		Wait:   func(context.Context) error { return nil },
		Cancel: func() { t.Error("提前取消") },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(m, ctx)
	await(t, started)
	time.Sleep(80 * time.Millisecond)
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestNewTimeout(t *testing.T) {
	if _, err := New(-time.Second); err == nil {
		t.Fatal("接受负期限")
	}
	m, err := New(0)
	if err != nil || m.shutdownTimeout != 30*time.Second {
		t.Fatalf("默认预算: %+v %v", m, err)
	}
}
