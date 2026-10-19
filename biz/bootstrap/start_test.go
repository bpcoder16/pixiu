package bootstrap_test

import (
	"errors"
	"slices"
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
				Shutdown: func() error {
					events = append(events, "close:"+name)
					if name == "second" {
						return shutdownErr
					}
					return nil
				},
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
				Shutdown: func() error {
					closed = append(closed, "rejected")
					return shutdownErr
				},
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
				rejected.Shutdown = nil
				wantClosed = []string{"first"}
			}
			err := bootstrap.Start(
				func() (apprun.Service, error) {
					return apprun.Service{
						Name: "first",
						Run:  unexpectedRun,
						Shutdown: func() error {
							closed = append(closed, "first")
							return nil
						},
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
			if rejected.Shutdown != nil && !errors.Is(err, shutdownErr) {
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
			Shutdown: func() error {
				closed = true
				return nil
			},
		}, nil
	}, nil)
	if err == nil || !closed {
		t.Fatalf("nil 构造函数应报错并回收此前服务: closed=%v, err=%v", closed, err)
	}
}

func TestStartRunsAndShutsDownOnce(t *testing.T) {
	runErr := errors.New("service stopped")
	var constructed, closed []string
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
				Shutdown: func() error {
					closed = append(closed, name)
					if name == "second" {
						close(secondStop)
					}
					return nil
				},
			}, nil
		}
	}
	if err := bootstrap.Start(factory("first"), factory("second")); !errors.Is(err, runErr) {
		t.Fatalf("未保留服务运行错误: %v", err)
	}
	if !slices.Equal(closed, []string{"second", "first"}) {
		t.Fatalf("服务应由管理器逆序关闭且不重复关闭: %v", closed)
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
