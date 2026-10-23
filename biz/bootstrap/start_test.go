package bootstrap_test

import "context"

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
)

func TestStartConstructionFailureClosesPreviousServices(t *testing.T) {
	constructErr := errors.New("construct failed")
	shutdownErr := errors.New("shutdown failed")
	var events []string
	factory := func(name string) bootstrap.ServiceFactory {
		return func() (apprun.Service, error) {
			events = append(events, "new:"+name)
			return apprun.Service{
				Name: name,
				Run: func() error {
					t.Error("构造未全部成功时不得启动服务")
					return nil
				},
				Stop: func() error {
					events = append(events, "close:"+name)
					if name == "second" {
						return shutdownErr
					}
					return nil
				},
				Wait: func(context.Context) error {
					return nil
				},
				Cancel: func() {},
			}, nil
		}
	}
	err := bootstrap.Start(
		factory("first"),
		factory("second"),
		func() (apprun.Service, error) {
			events = append(events, "new:failed")
			return apprun.Service{}, constructErr
		},
		factory("unreached"),
	)
	if !errors.Is(err, constructErr) || !errors.Is(err, shutdownErr) {
		t.Fatalf("未保留构造及回收错误: %v", err)
	}
	want := []string{"new:first", "new:second", "new:failed", "close:second", "close:first"}
	if !slices.Equal(events, want) {
		t.Fatalf("构造失败回收顺序 = %v，预期 %v", events, want)
	}
}

func TestStartRollbackWaitFailureCancels(t *testing.T) {
	constructErr := errors.New("construct failed")
	waitErr := errors.New("wait failed")
	for _, tt := range []struct {
		name       string
		factoryErr error
		wantErr    error
	}{
		{
			name:       "构造失败",
			factoryErr: constructErr,
			wantErr:    constructErr,
		},
		{
			name:    "登记失败",
			wantErr: apprun.ErrInvalidService,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var events []string
			factory := func(name string, result error) bootstrap.ServiceFactory {
				return func() (apprun.Service, error) {
					return apprun.Service{
						Name: name,
						Run: func() error {
							t.Error("启动失败时不得运行服务")
							return nil
						},
						Stop: func() error {
							events = append(events, "stop:"+name)
							return nil
						},
						Wait: func(ctx context.Context) error {
							if ctx.Err() != nil {
								t.Error("测试要求在排空期限内返回")
							}
							events = append(events, "wait:"+name)
							return result
						},
						Cancel: func() {
							events = append(events, "cancel:"+name)
						},
					}, nil
				}
			}
			err := bootstrap.Start(
				factory("drained", nil),
				factory("pending", waitErr),
				func() (apprun.Service, error) {
					return apprun.Service{}, tt.factoryErr
				},
			)
			if !errors.Is(err, tt.wantErr) || !errors.Is(err, waitErr) {
				t.Fatalf("未保留启动及排空错误: %v", err)
			}
			// 失败服务取消一次，继续回收其他服务；已确认排空的服务不取消。
			want := []string{"stop:pending", "stop:drained", "wait:pending", "cancel:pending", "wait:drained"}
			if !slices.Equal(events, want) {
				t.Fatalf("启动失败回收顺序 = %v，预期 %v", events, want)
			}
		})
	}
}

func TestStartRollbackPanicContinuesCleanup(t *testing.T) {
	constructErr := errors.New("construct failed")
	waitErr := errors.New("wait failed")
	const panicValue = "cleanup failed"
	for _, failure := range []string{"construction", "registration"} {
		for _, operation := range []string{"stop", "wait", "cancel"} {
			t.Run(failure+"/"+operation, func(t *testing.T) {
				var events []string
				factory := func(name string) bootstrap.ServiceFactory {
					return func() (apprun.Service, error) {
						record := func(phase string) {
							events = append(events, phase+":"+name)
							if name == "second" && phase == operation {
								panic(panicValue)
							}
						}
						return apprun.Service{
							Name: name,
							Run: func() error {
								t.Error("启动失败时不得运行服务")
								return nil
							},
							Stop: func() error {
								record("stop")
								return nil
							},
							Wait: func(context.Context) error {
								record("wait")
								return waitErr
							},
							Cancel: func() {
								record("cancel")
							},
						}, nil
					}
				}
				wantErr := constructErr
				if failure == "registration" {
					wantErr = apprun.ErrInvalidService
				}
				var err error
				if value := recoverValue(func() {
					err = bootstrap.Start(
						factory("first"),
						factory("second"),
						func() (apprun.Service, error) {
							if failure == "construction" {
								return apprun.Service{}, constructErr
							}
							return apprun.Service{}, nil
						},
					)
				}); value != nil {
					t.Fatalf("回收 panic 不应中断其他服务清理: %v，已执行 %v", value, events)
				}
				if !errors.Is(err, wantErr) || !errors.Is(err, waitErr) {
					t.Fatalf("未保留启动及回收错误: %v", err)
				}
				var panicErr *apprun.PanicError
				if !errors.As(err, &panicErr) {
					t.Fatalf("未保留回收 panic: %v", err)
				}
				if panicErr.Service != "second" || panicErr.Operation != operation || panicErr.Value != panicValue || len(panicErr.Stack) == 0 {
					t.Fatalf("回收 panic 信息不完整: %+v", panicErr)
				}
				// 单个回调失败不打断逆序停收和排空；未确认排空的服务仍取消一次。
				want := []string{"stop:second", "stop:first", "wait:second", "cancel:second", "wait:first", "cancel:first"}
				if !slices.Equal(events, want) {
					t.Fatalf("启动失败回收顺序 = %v，预期 %v", events, want)
				}
			})
		}
	}
}

func TestStartRegistrationFailureClosesConstructedServices(t *testing.T) {
	for _, reason := range []string{"duplicate", "missing-run", "missing-shutdown"} {
		t.Run(reason, func(t *testing.T) {
			shutdownErr := errors.New("rejected service shutdown failed")
			var closed []string
			unexpectedRun := func() error {
				t.Error("登记未全部成功时不得启动服务")
				return nil
			}
			rejected := apprun.Service{
				Name: "rejected",
				Run:  unexpectedRun,
				Stop: func() error {
					closed = append(closed, "rejected")
					return shutdownErr
				},
				Wait: func(context.Context) error {
					return nil
				},
				Cancel: func() {},
			}
			wantErr := apprun.ErrInvalidService
			wantClosed := []string{"rejected", "first"}
			switch reason {
			case "duplicate":
				rejected.Name = "first"
				wantErr = apprun.ErrDuplicateService
			case "missing-run":
				rejected.Run = nil
			case "missing-shutdown":
				rejected.Stop = nil
				wantClosed = []string{"first"}
			}
			err := bootstrap.Start(
				func() (apprun.Service, error) {
					return apprun.Service{
						Name: "first",
						Run:  unexpectedRun,
						Stop: func() error {
							closed = append(closed, "first")
							return nil
						},
						Wait: func(context.Context) error {
							return nil
						},
						Cancel: func() {},
					}, nil
				},
				func() (apprun.Service, error) {
					return rejected, nil
				},
				func() (apprun.Service, error) {
					t.Error("登记失败后不得继续构造服务")
					return apprun.Service{}, nil
				},
			)
			if !errors.Is(err, wantErr) {
				t.Fatalf("未保留登记错误: %v", err)
			}
			if rejected.Stop != nil && !errors.Is(err, shutdownErr) {
				t.Fatalf("未保留登记失败服务的回收错误: %v", err)
			}
			if !slices.Equal(closed, wantClosed) {
				t.Fatalf("登记失败回收顺序 = %v，预期 %v", closed, wantClosed)
			}
		})
	}
}

func TestStartNilFactoryClosesPreviousServices(t *testing.T) {
	closed := false
	err := bootstrap.Start(func() (apprun.Service, error) {
		return apprun.Service{
			Name: "first",
			Run: func() error {
				t.Error("存在 nil 构造函数时不得启动服务")
				return nil
			},
			Stop: func() error {
				closed = true
				return nil
			},
			Wait: func(context.Context) error {
				return nil
			},
			Cancel: func() {},
		}, nil
	}, nil)
	if err == nil || !closed {
		t.Fatalf("nil 构造函数应报错并回收此前服务: closed=%v, err=%v", closed, err)
	}
}

func TestStartRunsAndShutsDownOnce(t *testing.T) {
	runErr := errors.New("service stopped")
	var constructed, closed []string
	var mu sync.Mutex
	secondStop := make(chan struct{})
	factory := func(name string) bootstrap.ServiceFactory {
		return func() (apprun.Service, error) {
			constructed = append(constructed, name)
			return apprun.Service{
				Name: name,
				Run: func() error {
					if !slices.Equal(constructed, []string{"first", "second"}) {
						t.Error("全部服务构造完成前启动了服务")
					}
					if name == "first" {
						return runErr
					}
					<-secondStop
					return nil
				},
				Stop: func() error {
					mu.Lock()
					closed = append(closed, name)
					mu.Unlock()
					if name == "second" {
						close(secondStop)
					}
					return nil
				},
				Wait: func(context.Context) error {
					return nil
				},
				Cancel: func() {},
			}, nil
		}
	}
	if err := bootstrap.Start(factory("first"), factory("second")); !errors.Is(err, runErr) {
		t.Fatalf("未保留服务运行错误: %v", err)
	}
	slices.Sort(closed)
	if !slices.Equal(closed, []string{"first", "second"}) {
		t.Fatalf("服务应由管理器统一停收且不重复关闭: %v", closed)
	}
}

func TestStartWithoutServices(t *testing.T) {
	if err := bootstrap.Start(); !errors.Is(err, apprun.ErrNoServices) {
		t.Fatalf("未保留空服务错误: %v", err)
	}
}

func TestStartWithConfigValidation(t *testing.T) {
	for _, timeout := range []time.Duration{
		-time.Second,
		0,
		time.Second,
		5*time.Second - time.Nanosecond,
	} {
		called := false
		err := bootstrap.StartWithConfig(bootstrap.StartConfig{
			ShutdownTimeout: timeout,
		}, func() (apprun.Service, error) {
			called = true
			return apprun.Service{}, nil
		})
		if err == nil || called {
			t.Fatalf("非法期限 %s: %v", timeout, err)
		}
	}
}
