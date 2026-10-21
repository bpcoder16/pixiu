//go:build natsintegration

package bootstrap_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const natsRegistrationTemplate = `
streamDefaults: {replicas: 1}
streams:
  - name: EVENTS
    subjects: [events]
jetStreamConsumers:
  - stream: EVENTS
    durable: worker
    handler: task
coreConsumers:
  - name: broadcast
    subject: updates
    handler: core
  - name: queue
    subject: jobs
    queue: workers
    subscribers: 4
    handler: core
`

// 本组只初始化 env 和注册 NATS，不调用会修改 time.Local 的 MustBaseInit。
// 通过 make test / make test-race 单独运行，避免服务端后台采样与时区测试混跑。
func TestNATSFromConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "MissingClientBeforeEnvironment", run: testNATSFromConfigMissingClientBeforeEnvironment},
		{name: "PublishConsumeAndReuse", run: testNATSFromConfigPublishConsumeAndReuse},
		{name: "PreflightAndErrors", run: testNATSFromConfigPreflightAndErrors},
		{name: "DuplicateStreams", run: testNATSFromConfigDuplicateStreams},
		{name: "StreamValidation", run: testNATSFromConfigStreamValidation},
		{name: "StreamOptions", run: testNATSFromConfigStreamOptions},
		{name: "JetStreamConsumerValidation", run: testNATSFromConfigJetStreamConsumerValidation},
		{name: "CoreConsumerValidation", run: testNATSFromConfigCoreConsumerValidation},
		{name: "CoreSubscribers", run: testNATSFromConfigCoreSubscribers},
		{name: "CoreOnly", run: testNATSFromConfigCoreOnly},
		{name: "RemoteConflictsAndPartialCleanup", run: testNATSFromConfigRemoteConflictsAndPartialCleanup},
	} {
		t.Run(tc.name, tc.run)
	}
}

func startNATSTestServer(t *testing.T, jetStream bool) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: jetStream,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("临时 NATS 服务未就绪")
	}
	return s.ClientURL()
}

func registrationClients(t *testing.T, enableJetStream bool, names ...string) []*natsx.Client {
	t.Helper()
	if err := env.Init(env.Config{
		AppName:       "nats-registration-test",
		RunMode:       env.RunModeRelease,
		TimeLocation:  "UTC",
		ConfigDirPath: t.TempDir(),
		LocalIP:       "127.0.0.1",
	}); err != nil {
		t.Fatal(err)
	}
	url := startNATSTestServer(t, enableJetStream)
	t.Cleanup(func() {
		if err := natsx.CloseAll(); err != nil {
			t.Error(err)
		}
	})
	var clients []*natsx.Client
	for _, name := range names {
		client, err := natsx.NewNamed(natsx.Config{
			Name: name,
			URLs: []string{url},
		})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, client)
	}
	return clients
}

func writeNATSRegistration(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "nats-register."+name+".yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// env 与命名客户端不可重新初始化，各场景通过独立测试进程隔离。
func isolatedNATS(t *testing.T, run func()) {
	t.Helper()
	const marker = "PIXIU_NATS_REGISTRATION_TEST"
	if os.Getenv(marker) == t.Name() {
		run()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("NATS 装配独立进程测试失败: %v\n%s", err, output)
	}
}

func testNATSFromConfigMissingClientBeforeEnvironment(t *testing.T) {
	isolatedNATS(t, func() {
		// 生命周期误用沿用 Named 的原始 panic，不先触发 env 的 panic。
		value := recoverValue(func() {
			_ = bootstrap.RegisterNATSFromConfig(context.Background(), "missing", nil, nil)
		})
		if value != `natsx: client "missing" is not registered` {
			t.Fatalf("未初始化实例应沿用 Named 的 panic，实际: %v", value)
		}
	})
}

func testNATSFromConfigPublishConsumeAndReuse(t *testing.T) {
	isolatedNATS(t, func() {
		clients := registrationClients(t, true, "first", "second")
		received := make(chan string, 16)
		var unused atomic.Int32
		js := natsx.MustNewJetStreamHandlerRegistry()
		js.MustRegister(func(context.Context, jetstream.Msg) error {
			received <- "events"
			return nil
		}, "task")
		js.MustRegister(func(context.Context, jetstream.Msg) error {
			unused.Add(1)
			return nil
		}, "unused")
		core := natsx.MustNewCoreHandlerRegistry()
		core.MustRegister(func(_ context.Context, msg *nats.Msg) error {
			received <- msg.Subject
			return nil
		}, "core")
		// 无 deadline 的启动 context 不传入长期 Handler；两实例复用同一远端 Durable。
		for _, name := range []string{"first", "second"} {
			// 不符合条件的可选覆盖不应用，继续使用全局副本数。
			content := strings.Replace(natsRegistrationTemplate, "subjects: [events]", "subjects: [events]\n    replicas: 0", 1)
			writeNATSRegistration(t, name, content)
			if err := bootstrap.RegisterNATSFromConfig(context.Background(), name, js, core); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(logit.WithContextLogID(context.Background()), 10*time.Second)
		defer cancel()
		if _, err := clients[0].PublishJetStream(ctx, "events", []byte("event"), "event-1"); err != nil {
			t.Fatal(err)
		}
		for _, subject := range []string{"updates", "jobs"} {
			if err := clients[0].Publish(ctx, subject, nil); err != nil {
				t.Fatal(err)
			}
		}
		counts := map[string]int{}
		for range 4 {
			select {
			case subject := <-received:
				counts[subject]++
			case <-ctx.Done():
				t.Fatalf("消费未完成: %v", counts)
			}
		}
		consumer, err := clients[0].JetStream().Consumer(ctx, "EVENTS", "worker")
		if err != nil {
			t.Fatal(err)
		}
		for {
			info, err := consumer.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if info.AckFloor.Stream == 1 && info.NumAckPending == 0 {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("Handler 返回后未 ACK")
			case <-time.After(10 * time.Millisecond):
			}
		}
		if err := bootstrap.RegisterNATSFromConfig(ctx, "first", js, core); err == nil || !strings.Contains(err.Error(), "already called") {
			t.Fatalf("应拒绝同一客户端重复装配: %v", err)
		}
		if err := natsx.CloseAll(); err != nil {
			t.Fatal(err)
		}
		if counts["events"] != 1 || counts["updates"] != 2 || counts["jobs"] != 1 || len(received) != 0 || unused.Load() != 0 {
			t.Fatalf("选择性消费、广播或 Queue 语义错误: %v, extra=%d, unused=%d", counts, len(received), unused.Load())
		}
	})
}

func testNATSFromConfigPreflightAndErrors(t *testing.T) {
	isolatedNATS(t, func() {
		client := registrationClients(t, true, "demo")[0]
		ctx := context.Background()
		js := natsx.MustNewJetStreamHandlerRegistry()
		js.MustRegister(func(context.Context, jetstream.Msg) error { return nil }, "task")
		if err := bootstrap.RegisterNATSFromConfig(ctx, "demo", js, nil); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("缺文件应保留错误链: %v", err)
		}
		for _, value := range []string{"'1'", "[]", "{}", "2"} {
			content := strings.Replace(natsRegistrationTemplate, "subjects: [events]", "subjects: [events]\n    replicas: "+value, 1)
			writeNATSRegistration(t, "demo", content)
			err := bootstrap.RegisterNATSFromConfig(ctx, "demo", js, nil)
			if err == nil || !strings.Contains(err.Error(), "streams[0]") || !strings.Contains(err.Error(), "nats-register.demo.yaml") {
				t.Fatalf("副本配置错误应带路径及位置: %v", err)
			}
			if _, err := client.JetStream().Stream(ctx, "EVENTS"); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("副本配置校验失败前不应创建 Stream: %v", err)
			}
		}
		writeNATSRegistration(t, "demo", natsRegistrationTemplate)
		err := bootstrap.RegisterNATSFromConfig(ctx, "demo", js, nil)
		if err == nil || !strings.Contains(err.Error(), "coreConsumers[0]") || !strings.Contains(err.Error(), "nats-register.demo.yaml") {
			t.Fatalf("后置未知 Handler 应带路径及位置: %v", err)
		}
		if _, err := client.JetStream().Stream(ctx, "EVENTS"); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("本地校验失败前不应创建 Stream: %v", err)
		}
		core := natsx.MustNewCoreHandlerRegistry()
		core.MustRegister(func(context.Context, *nats.Msg) error { return nil }, "core")
		subscriptions := client.Conn().NumSubscriptions()
		for _, declaration := range []string{
			"",
			"    subscribers: null\n",
			"    subscribers: -1\n",
			"    subscribers: 0\n",
			"    subscribers: 1\n",
		} {
			content := strings.Replace(natsRegistrationTemplate, "    subscribers: 4\n", declaration, 1)
			writeNATSRegistration(t, "demo", content)
			err := bootstrap.RegisterNATSFromConfig(ctx, "demo", js, core)
			if err == nil || !strings.Contains(err.Error(), `coreConsumers[1] "queue" subscribers`) || !strings.Contains(err.Error(), "nats-register.demo.yaml") {
				t.Fatalf("队列订阅数错误应带路径及位置: %v", err)
			}
			if _, err := client.JetStream().Stream(ctx, "EVENTS"); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("队列订阅数校验失败前不应创建 Stream: %v", err)
			}
			if client.Conn().NumSubscriptions() != subscriptions {
				t.Fatal("队列订阅数校验失败前不应建立订阅")
			}
		}
		// 本地预检失败和 nil ctx 均不应消耗注册次数，随后用同一实例成功提交空声明。
		writeNATSRegistration(t, "demo", "streamDefaults: {replicas: 1}")
		if err := bootstrap.RegisterNATSFromConfig(nil, "demo", nil, nil); err == nil || !strings.Contains(err.Error(), "natsx: nil registration context") {
			t.Fatalf("nil context 应沿用 natsx 的错误: %v", err)
		}
		if err := bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, nil); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"", "../escape", " demo"} {
			if err := bootstrap.RegisterNATSFromConfig(ctx, name, nil, nil); err == nil {
				t.Fatalf("应拒绝无效实例名 %q", name)
			}
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if err := bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, nil); !errors.Is(err, natsx.ErrClosed) {
			t.Fatalf("直接关闭已登记的 Client 后应返回 ErrClosed: %v", err)
		}
		if err := natsx.CloseAll(); err != nil {
			t.Fatal(err)
		}
		value := recoverValue(func() {
			_ = bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, nil)
		})
		if value != "natsx: named clients closed" {
			t.Fatalf("CloseAll 后查询应沿用 Named 的 panic，实际: %v", value)
		}
	})
}

func testNATSFromConfigDuplicateStreams(t *testing.T) {
	isolatedNATS(t, func() {
		client := registrationClients(t, true, "demo")[0]
		ctx := context.Background()
		writeNATSRegistration(t, "demo", `
streamDefaults:
  replicas: 1
streams:
  - name: FIRST
    subjects: [first]
  - name: EVENTS
    subjects: [events]
  - name: EVENTS
    subjects: [events]
`)
		err := bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, nil)
		if err == nil || !strings.Contains(err.Error(), `natsx: duplicate stream "EVENTS"`) {
			t.Fatalf("Stream 重名应由 natsx 拒绝: %v", err)
		}
		for _, name := range []string{"FIRST", "EVENTS"} {
			if _, err = client.JetStream().Stream(ctx, name); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("重名失败前不应创建本批 Stream %q: %v", name, err)
			}
		}
		// 重名已进入底层入口，修正配置也不能复用本次 Stream 注册机会。
		writeNATSRegistration(t, "demo", "streamDefaults: {replicas: 1}")
		if err = bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, nil); err == nil || !strings.Contains(err.Error(), "RegisterStreams already called") {
			t.Fatalf("重名失败应消耗 Stream 注册次数: %v", err)
		}
	})
}

func testNATSFromConfigStreamValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		stream string
	}{
		{
			name: "name-whitespace",
			stream: `
  - name: ' EVENTS '
    subjects: [events]
`,
		},
		{
			name: "subject-whitespace",
			stream: `
  - name: EVENTS
    subjects: [' events']
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolatedNATS(t, func() {
				client := registrationClients(t, true, "demo")[0]
				ctx := context.Background()
				writeNATSRegistration(t, "demo", `
streamDefaults: {replicas: 1}
streams:
  - name: FIRST
    subjects: [first]
`+test.stream)
				err := bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, nil)
				if err == nil || !strings.Contains(err.Error(), "register streams: natsx:") {
					t.Fatalf("Stream 通用校验应由 natsx 执行: %v", err)
				}
				if _, err = client.JetStream().Stream(ctx, "FIRST"); !errors.Is(err, jetstream.ErrStreamNotFound) {
					t.Fatalf("后续声明非法时不应创建首个 Stream: %v", err)
				}
				writeNATSRegistration(t, "demo", "streamDefaults: {replicas: 1}")
				if err = bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, nil); err == nil || !strings.Contains(err.Error(), "RegisterStreams already called") {
					t.Fatalf("通用校验失败应消耗 Stream 注册次数: %v", err)
				}
			})
		})
	}
}

func testNATSFromConfigStreamOptions(t *testing.T) {
	isolatedNATS(t, func() {
		clients := registrationClients(t, true, "options", "negative-age", "negative-duplicates", "invalid-window")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		const content = `
streamDefaults: {replicas: 1}
streams:
  - name: DEFAULTS
    subjects: [defaults]
  - name: NULLS
    subjects: [nulls]
    retention: null
    maxAge: null
    duplicates: null
  - name: TASKS
    subjects: [tasks]
    retention: workqueue
    maxAge: 1h
    duplicates: 30s
  - name: EVENTS
    subjects: [events]
    retention: limits
    maxAge: 1m
    duplicates: 0
  - name: UPDATES
    subjects: [updates]
    retention: interest
    maxAge: 0
    duplicates: 5m
`
		// 后续声明的策略或时长格式非法时，配置预检应阻止前面的 Stream 创建。
		for _, invalid := range []string{
			strings.Replace(content, "retention: interest", "retention: invalid", 1),
			strings.Replace(content, "maxAge: 1h", "maxAge: 1d", 1),
			strings.Replace(content, "duplicates: 5m", "duplicates: forever", 1),
		} {
			writeNATSRegistration(t, "options", invalid)
			err := bootstrap.RegisterNATSFromConfig(ctx, "options", nil, nil)
			if err == nil || !strings.Contains(err.Error(), "nats-register.options.yaml") {
				t.Fatalf("非法 Stream 配置应在预检阶段报错并包含路径: %v", err)
			}
			if _, err := clients[0].JetStream().Stream(ctx, "DEFAULTS"); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("预检失败前不应创建 Stream: %v", err)
			}
		}
		writeNATSRegistration(t, "options", content)
		if err := bootstrap.RegisterNATSFromConfig(ctx, "options", nil, nil); err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			name       string
			retention  jetstream.RetentionPolicy
			maxAge     time.Duration
			duplicates time.Duration
		}{
			{
				name:       "DEFAULTS",
				retention:  jetstream.LimitsPolicy,
				maxAge:     90 * 24 * time.Hour,
				duplicates: 2 * time.Minute,
			},
			{
				name:       "NULLS",
				retention:  jetstream.LimitsPolicy,
				maxAge:     90 * 24 * time.Hour,
				duplicates: 2 * time.Minute,
			},
			{
				name:       "TASKS",
				retention:  jetstream.WorkQueuePolicy,
				maxAge:     time.Hour,
				duplicates: 30 * time.Second,
			},
			{
				name:       "EVENTS",
				retention:  jetstream.LimitsPolicy,
				maxAge:     time.Minute,
				duplicates: time.Minute,
			},
			{
				name:       "UPDATES",
				retention:  jetstream.InterestPolicy,
				maxAge:     0,
				duplicates: 5 * time.Minute,
			},
		} {
			stream, err := clients[0].JetStream().Stream(ctx, test.name)
			if err != nil {
				t.Fatal(err)
			}
			got := stream.CachedInfo().Config
			if got.Retention != test.retention || got.MaxAge != test.maxAge || got.Duplicates != test.duplicates {
				t.Fatalf("Stream %s 有效配置错误: retention=%v, maxAge=%v, duplicates=%v", test.name, got.Retention, got.MaxAge, got.Duplicates)
			}
		}
		for i, test := range []struct {
			instance string
			fields   string
		}{
			{
				instance: "negative-age",
				fields:   "    maxAge: -1s\n",
			},
			{
				instance: "negative-duplicates",
				fields:   "    duplicates: -1s\n",
			},
			{
				instance: "invalid-window",
				fields:   "    maxAge: 1s\n    duplicates: 2m\n",
			},
		} {
			writeNATSRegistration(t, test.instance, `
streamDefaults: {replicas: 1}
streams:
  - name: INVALID
    subjects: [invalid]
`+test.fields)
			err := bootstrap.RegisterNATSFromConfig(ctx, test.instance, nil, nil)
			var apiErr *jetstream.APIError
			if !errors.As(err, &apiErr) || !strings.Contains(err.Error(), "register streams") {
				t.Fatalf("%s 的时长范围应由服务端校验并保留错误链: %v", test.instance, err)
			}
			if _, err := clients[i+1].JetStream().Stream(ctx, "INVALID"); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("非法 Stream 不应创建: %v", err)
			}
		}
	})
}

func testNATSFromConfigJetStreamConsumerValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		durable string
	}{
		{
			name:    "durable-whitespace",
			durable: " second ",
		},
		{
			name:    "duplicate",
			durable: "first",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolatedNATS(t, func() {
				client := registrationClients(t, true, "demo")[0]
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				writeNATSRegistration(t, "demo", `
streamDefaults: {replicas: 1}
streams:
  - name: EVENTS
    subjects: [events]
jetStreamConsumers:
  - stream: EVENTS
    durable: first
    handler: task
  - stream: EVENTS
    durable: '`+test.durable+`'
    handler: task
`)
				handlers := natsx.MustNewJetStreamHandlerRegistry()
				handlers.MustRegister(func(context.Context, jetstream.Msg) error { return nil }, "task")
				err := bootstrap.RegisterNATSFromConfig(ctx, "demo", handlers, nil)
				if err == nil || !strings.Contains(err.Error(), "register JetStream consumers: natsx:") {
					t.Fatalf("Consumer 通用校验及判重应由 natsx 执行: %v", err)
				}
				if _, err = client.JetStream().Stream(ctx, "EVENTS"); err != nil {
					t.Fatalf("Consumer 校验失败应保留前序已创建的 Stream: %v", err)
				}
				if _, err = client.JetStream().Consumer(ctx, "EVENTS", "first"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
					t.Fatalf("后续声明非法时不应创建本批任何 Consumer: %v", err)
				}
				if err = client.RegisterJetStreamConsumers(ctx); err == nil || !strings.Contains(err.Error(), "RegisterJetStreamConsumers already called") {
					t.Fatalf("Consumer 校验失败应消耗注册次数: %v", err)
				}
			})
		})
	}
}

func testNATSFromConfigCoreConsumerValidation(t *testing.T) {
	for _, test := range []struct {
		name         string
		consumerName string
		extraConfig  string
	}{
		{
			name:         "name-whitespace",
			consumerName: " second ",
		},
		{
			name:         "duplicate",
			consumerName: "first",
		},
		{
			name:         "negative-broadcast-subscribers",
			consumerName: "second",
			extraConfig:  "    subscribers: -1\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolatedNATS(t, func() {
				client := registrationClients(t, false, "demo")[0]
				ctx := context.Background()
				writeNATSRegistration(t, "demo", `
streamDefaults: {replicas: 1}
coreConsumers:
  - name: first
    subject: core.first
    handler: core
  - name: '`+test.consumerName+`'
    subject: core.second
    handler: core
`+test.extraConfig)
				handlers := natsx.MustNewCoreHandlerRegistry()
				handlers.MustRegister(func(context.Context, *nats.Msg) error { return nil }, "core")
				subscriptions := client.Conn().NumSubscriptions()
				err := bootstrap.RegisterNATSFromConfig(ctx, "demo", nil, handlers)
				if err == nil || !strings.Contains(err.Error(), "register Core consumers: natsx:") {
					t.Fatalf("Core Consumer 通用校验及判重应由 natsx 执行: %v", err)
				}
				if client.Conn().NumSubscriptions() != subscriptions {
					t.Fatal("后续声明非法时不应建立本批任何订阅")
				}
				if err = client.RegisterCoreConsumers(ctx); err == nil || !strings.Contains(err.Error(), "RegisterCoreConsumers already called") {
					t.Fatalf("Core Consumer 校验失败应消耗注册次数: %v", err)
				}
			})
		})
	}
}

func testNATSFromConfigCoreSubscribers(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		want   int
	}{
		{
			name:   "queue-minimum",
			config: "    queue: workers\n    subscribers: 2\n",
			want:   2,
		},
		{
			name: "broadcast-omitted",
			want: 1,
		},
		{
			name:   "queue-multiple",
			config: "    queue: workers\n    subscribers: 4\n",
			want:   4,
		},
		{
			name:   "broadcast",
			config: "    subscribers: 4\n",
			want:   1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolatedNATS(t, func() {
				client := registrationClients(t, false, "core")[0]
				writeNATSRegistration(t, "core", `
streamDefaults: {replicas: 1}
coreConsumers:
  - name: worker
    subject: tasks
    handler: core
`+test.config)
				handlers := natsx.MustNewCoreHandlerRegistry()
				handlers.MustRegister(func(context.Context, *nats.Msg) error { return nil }, "core")
				before := client.Conn().NumSubscriptions()
				if err := bootstrap.RegisterNATSFromConfig(context.Background(), "core", nil, handlers); err != nil {
					t.Fatal(err)
				}
				if got := client.Conn().NumSubscriptions() - before; got != test.want {
					t.Fatalf("配置创建的订阅数 = %d，期望 %d", got, test.want)
				}
			})
		})
	}
}

func testNATSFromConfigCoreOnly(t *testing.T) {
	isolatedNATS(t, func() {
		client := registrationClients(t, false, "core")[0]
		writeNATSRegistration(t, "core", `
streamDefaults: {replicas: 1}
coreConsumers:
  - name: responder
    subject: requests
    handler: reply
`)
		core := natsx.MustNewCoreHandlerRegistry()
		core.MustRegister(func(_ context.Context, msg *nats.Msg) error {
			return msg.Respond([]byte("ok"))
		}, "reply")
		if err := bootstrap.RegisterNATSFromConfig(context.Background(), "core", nil, core); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(logit.WithContext(context.Background()), 5*time.Second)
		defer cancel()
		reply, err := client.Request(ctx, "requests", nil)
		if err != nil || string(reply.Data) != "ok" {
			t.Fatalf("未开启 JetStream 时 Core 应正常请求响应: %v", err)
		}
	})
}

func testNATSFromConfigRemoteConflictsAndPartialCleanup(t *testing.T) {
	for _, scenario := range []string{"stream-conflict", "consumer-conflict", "external-stream", "partial-core"} {
		t.Run(scenario, func(t *testing.T) {
			isolatedNATS(t, func() {
				client := registrationClients(t, true, "demo")[0]
				ctx := context.Background()
				content := natsRegistrationTemplate
				js := natsx.MustNewJetStreamHandlerRegistry()
				js.MustRegister(func(context.Context, jetstream.Msg) error { return nil }, "task")
				core := natsx.MustNewCoreHandlerRegistry()
				core.MustRegister(func(context.Context, *nats.Msg) error { return nil }, "core")
				switch scenario {
				case "stream-conflict":
					_, err := client.JetStream().CreateStream(ctx, natsx.DefaultStreamConfig("EVENTS", 1, "other"))
					if err != nil {
						t.Fatal(err)
					}
				case "consumer-conflict", "external-stream":
					if _, err := client.JetStream().CreateStream(ctx, natsx.DefaultStreamConfig("EVENTS", 1, "events")); err != nil {
						t.Fatal(err)
					}
					if scenario == "consumer-conflict" {
						config := natsx.DefaultJetStreamConsumer("EVENTS", "worker", nil).Config
						config.AckWait = time.Second
						if _, err := client.JetStream().CreateConsumer(ctx, "EVENTS", config); err != nil {
							t.Fatal(err)
						}
					} else {
						content = strings.Replace(content, "streams:\n  - name: EVENTS\n    subjects: [events]", "streams: []", 1)
					}
				case "partial-core":
					// SDK 拒绝包含内部空白的主题；前一个 Core 订阅及 JetStream worker 已建立。
					content = strings.Replace(content, "subject: jobs", "subject: 'bad subject'", 1)
				}
				writeNATSRegistration(t, "demo", content)
				err := bootstrap.RegisterNATSFromConfig(ctx, "demo", js, core)
				switch scenario {
				case "stream-conflict":
					if !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
						t.Fatalf("未保留 Stream 冲突: %v", err)
					}
				case "consumer-conflict":
					if !errors.Is(err, jetstream.ErrConsumerExists) {
						t.Fatalf("未保留 Consumer 冲突: %v", err)
					}
				case "external-stream":
					if err != nil {
						t.Fatalf("引用已有外部 Stream 应成功: %v", err)
					}
				case "partial-core":
					if err == nil || !strings.Contains(err.Error(), "register Core consumers") {
						t.Fatalf("应返回后置订阅失败: %v", err)
					}
					if _, err := client.JetStream().Consumer(ctx, "EVENTS", "worker"); err != nil {
						t.Fatalf("失败不得删除已创建资源: %v", err)
					}
				}
				if err := natsx.CloseAll(); err != nil {
					t.Fatalf("部分注册后无法正常收尾: %v", err)
				}
				if !client.Conn().IsClosed() {
					t.Fatal("客户端未关闭")
				}
			})
		})
	}
}
