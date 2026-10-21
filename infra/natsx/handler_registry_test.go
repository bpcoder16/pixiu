package natsx_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bpcoder16/pixiu/infra/natsx"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func registrationTask(context.Context, jetstream.Msg) error { return nil }
func registrationCore(context.Context, *nats.Msg) error     { return nil }

type registrationService struct{}

func (registrationService) Task(context.Context, jetstream.Msg) error { return nil }
func registrationGeneric[T any](context.Context, jetstream.Msg) error { return nil }

func TestNATSHandlerRegistryNamesAndSnapshots(t *testing.T) {
	js := natsx.MustNewJetStreamHandlerRegistry(registrationTask)
	core := natsx.MustNewCoreHandlerRegistry()
	core.MustRegister(registrationCore, "registrationTask")
	js.MustRegister(registrationService{}.Task, "method")
	js.MustRegister(registrationGeneric[int], "generic")
	called := false
	core.MustRegister(func(context.Context, *nats.Msg) error {
		called = true
		return nil
	}, "closure")
	if called {
		t.Fatal("登记不得执行业务")
	}
	snapshot := js.Handlers()
	delete(snapshot, "method")
	js.MustRegister(registrationTask, "Alias")
	if len(snapshot) != 2 || len(js.Handlers()) != 4 {
		t.Fatal("快照不应共享 map 或受后续追加影响")
	}
	if js.Handlers()["registrationtask"] != nil || js.Handlers()["registrationTask"] == nil {
		t.Fatal("自动名称必须保留大小写")
	}
	if err := core.Handlers()["closure"](context.Background(), nil); err != nil || !called {
		t.Fatal("闭包绑定丢失")
	}
	var zero natsx.JetStreamHandlerRegistry
	zero.MustRegister(registrationTask)
	if len(zero.Handlers()) != 1 {
		t.Fatal("零值注册表不可用")
	}
}

func TestNATSHandlerRegistryPanicCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		register func()
		want     string
	}{
		{
			name:     "jetstream-nil",
			register: func() { natsx.MustNewJetStreamHandlerRegistry(nil) },
			want:     "natsx: nil JetStream handler",
		},
		{
			name:     "core-nil",
			register: func() { natsx.MustNewCoreHandlerRegistry(nil) },
			want:     "natsx: nil Core handler",
		},
		{
			name:     "jetstream-duplicate",
			register: func() { natsx.MustNewJetStreamHandlerRegistry(registrationTask, registrationTask) },
			want:     `natsx: duplicate JetStream handler "registrationTask"`,
		},
		{
			name:     "core-duplicate",
			register: func() { natsx.MustNewCoreHandlerRegistry(registrationCore, registrationCore) },
			want:     `natsx: duplicate Core handler "registrationCore"`,
		},
		{
			name:     "core-invalid-name",
			register: func() { natsx.MustNewCoreHandlerRegistry().MustRegister(registrationCore, " bad") },
			want:     `natsx: invalid Core handler " bad"`,
		},
		{
			name:     "core-multiple-names",
			register: func() { natsx.MustNewCoreHandlerRegistry().MustRegister(registrationCore, "a", "b") },
			want:     "natsx: expected at most one Core handler name",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				value := recover()
				if fmt.Sprint(value) != tc.want {
					t.Fatalf("panic = %v，期望 %q", value, tc.want)
				}
				if _, ok := value.(error); !ok {
					t.Fatalf("NATS 登记错误应保留 error panic: %T", value)
				}
			}()
			tc.register()
		})
	}
}

func TestNATSHandlerRegistryEmptyAndCoreSnapshots(t *testing.T) {
	var js *natsx.JetStreamHandlerRegistry
	var core *natsx.CoreHandlerRegistry
	if js.Handlers() != nil || core.Handlers() != nil {
		t.Fatal("nil 注册表应提供空清单，允许配置不启用对应消费者")
	}
	var zeroJS natsx.JetStreamHandlerRegistry
	var zeroCore natsx.CoreHandlerRegistry
	if zeroJS.Handlers() != nil || zeroCore.Handlers() != nil {
		t.Fatal("零值注册表应提供空清单")
	}
	zeroCore.MustRegister(registrationCore)
	snapshot := zeroCore.Handlers()
	snapshot["registrationCore"] = nil
	snapshot["injected"] = registrationCore
	zeroCore.MustRegister(registrationCore, "alias")
	if snapshot["alias"] != nil || zeroCore.Handlers()["registrationCore"] == nil || zeroCore.Handlers()["injected"] != nil {
		t.Fatal("Core 快照修改与后续登记应互不影响")
	}
}
