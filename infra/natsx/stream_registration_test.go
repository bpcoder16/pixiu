package natsx

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestStreamRegistrationModes(t *testing.T) {
	captureLogs(t)
	c, err := New(Config{
		Name:      "stream-modes",
		Namespace: "qa",
		URLs:      []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, name := range []string{"UPDATE", "EXTERNAL"} {
		if _, err := c.JetStream().CreateStream(ctx, jetstream.StreamConfig{
			Name:        "QA_" + name,
			Subjects:    []string{"qa." + name},
			Storage:     jetstream.MemoryStorage,
			Description: "由外部管理",
		}); err != nil {
			t.Fatal(err)
		}
	}
	// SDK 追踪只记录注册请求，验证 existing 仅查询一次，不发送创建或更新。
	var requests []string
	c.js, err = jetstream.New(c.Conn(), jetstream.WithClientTrace(&jetstream.ClientTrace{
		RequestSent: func(subject string, _ []byte) {
			requests = append(requests, subject)
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	specs := []StreamRegistration{
		{Config: DefaultStreamConfig("CREATE", 1, "create")},
		{Mode: StreamModeCreate, Config: DefaultStreamConfig("EXPLICIT", 1, "explicit")},
		{Mode: StreamModeUpsert, Config: DefaultStreamConfig("NEW", 1, "new")},
		{
			Mode: StreamModeUpsert,
			Config: jetstream.StreamConfig{
				Name:        "UPDATE",
				Subjects:    []string{"UPDATE"},
				Storage:     jetstream.MemoryStorage,
				Description: "更新后的描述",
			},
		},
		{Mode: StreamModeExisting, Config: jetstream.StreamConfig{Name: "EXTERNAL"}},
	}
	if err := c.RegisterStreamRegistrations(ctx, specs...); err != nil {
		t.Fatal(err)
	}
	wantRequests := []string{
		"$JS.API.STREAM.CREATE.QA_CREATE",
		"$JS.API.STREAM.CREATE.QA_EXPLICIT",
		"$JS.API.STREAM.UPDATE.QA_NEW",
		"$JS.API.STREAM.CREATE.QA_NEW",
		"$JS.API.STREAM.UPDATE.QA_UPDATE",
		"$JS.API.STREAM.INFO.QA_EXTERNAL",
	}
	if !reflect.DeepEqual(requests, wantRequests) {
		t.Fatalf("三种模式应使用各自的 SDK 操作: got %v, want %v", requests, wantRequests)
	}
	for _, name := range []string{"CREATE", "EXPLICIT", "NEW", "UPDATE", "EXTERNAL"} {
		stream, err := c.JetStream().Stream(ctx, "QA_"+name)
		if err != nil {
			t.Fatal(err)
		}
		cfg := stream.CachedInfo().Config
		if len(cfg.Subjects) != 1 || !strings.HasPrefix(cfg.Subjects[0], "qa.") {
			t.Fatalf("实际 Stream 应使用 namespace: %+v", cfg)
		}
		if name == "EXTERNAL" && cfg.Description != "由外部管理" || name == "UPDATE" && cfg.Description != "更新后的描述" {
			t.Fatalf("Stream 更新范围错误: %+v", cfg)
		}
	}
	if specs[3].Config.Name != "UPDATE" || specs[3].Config.Subjects[0] != "UPDATE" {
		t.Fatalf("不能修改调用方声明: %+v", specs[3])
	}
	if err := c.RegisterStreams(ctx); err == nil {
		t.Fatal("新旧入口必须共用一次性注册限制")
	}
}

func TestStreamRegistrationFailures(t *testing.T) {
	captureLogs(t)
	for _, tc := range []struct {
		name string
		mode StreamMode
	}{
		{name: "默认创建拒绝配置冲突"},
		{name: "创建拒绝配置冲突", mode: StreamModeCreate},
		{name: "更新拒绝不可变字段", mode: StreamModeUpsert},
		{name: "引用不存在的流", mode: StreamModeExisting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := registrationClient(t)
			ctx := t.Context()
			cfg := jetstream.StreamConfig{Name: "TASKS"}
			if tc.mode != StreamModeExisting {
				if _, err := c.JetStream().CreateStream(ctx, jetstream.StreamConfig{
					Name:    "TASKS",
					Storage: jetstream.MemoryStorage,
				}); err != nil {
					t.Fatal(err)
				}
				cfg.Storage = jetstream.FileStorage
			}
			err := c.RegisterStreamRegistrations(ctx, StreamRegistration{Mode: tc.mode, Config: cfg})
			if err == nil || !strings.Contains(err.Error(), "TASKS") {
				t.Fatalf("失败须保留 Stream 定位: %v", err)
			}
			var apiErr *jetstream.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("须保留 SDK 错误链: %v", err)
			}
			if tc.mode == StreamModeExisting && !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("缺失 Stream 不应被创建: %v", err)
			}
			if tc.mode == "" || tc.mode == StreamModeCreate {
				if !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
					t.Fatalf("创建模式应保留配置冲突错误: %v", err)
				}
			}
			if err := c.RegisterStreams(ctx); err == nil {
				t.Fatal("失败也应占用注册次数")
			}
		})
	}
}

func TestStreamRegistrationPreflight(t *testing.T) {
	captureLogs(t)
	for _, spec := range []StreamRegistration{
		{Mode: "unknown", Config: jetstream.StreamConfig{Name: "OTHER"}},
		{Mode: StreamModeExisting, Config: jetstream.StreamConfig{Name: "FIRST"}},
		{Mode: StreamModeExisting, Config: jetstream.StreamConfig{Name: "OTHER", Subjects: []string{"other"}}},
		{Mode: StreamModeExisting, Config: jetstream.StreamConfig{Name: "OTHER", MaxBytes: -1}},
	} {
		c := registrationClient(t)
		err := c.RegisterStreamRegistrations(t.Context(),
			StreamRegistration{Config: DefaultStreamConfig("FIRST", 1, "first")},
			spec,
		)
		if err == nil {
			t.Fatalf("非法模式、重复名称或 existing 携带配置必须拒绝: %+v", spec)
		}
		if _, err := c.JetStream().Stream(t.Context(), "FIRST"); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("整批预检失败不得创建前序 Stream: %v", err)
		}
	}
	c := registrationClient(t)
	if err := c.RegisterStreams(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterStreamRegistrations(t.Context()); err == nil {
		t.Fatal("旧入口调用后不能通过新入口再次注册")
	}
}
