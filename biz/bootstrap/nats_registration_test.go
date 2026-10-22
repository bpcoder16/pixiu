package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func registrationTask(context.Context, jetstream.Msg) error { return nil }
func registrationCore(context.Context, *nats.Msg) error     { return nil }

const registrationYAML = `
streamDefaults:
  replicas: 3
streams:
  - name: ORDERS
    subjects: [orders.>]
    consumers:
      - durable: worker
        filterSubjects: [orders.created]
        handler: registrationTask
  - name: TEMP
    replicas: 1
    subjects: [temp.>]
coreConsumers:
  - name: broadcast
    subject: updates
    handler: registrationCore
  - name: queue
    subject: tasks
    queue: workers
    subscribers: 4
    handler: registrationCore
`

func parseNATSRegistration(t *testing.T, content string) (*natsRegistrationConfig, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nats-register.test.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return configx.Parse[natsRegistrationConfig](path)
}

func TestNATSRegistrationNestedConsumers(t *testing.T) {
	cfg, err := parseNATSRegistration(t, `
streamDefaults: {replicas: 1}
streams:
  - name: ORDERS
    subjects: [orders.>]
    consumers:
      - durable: worker
        handler: registrationTask
        filterSubjects: [orders.created]
      - durable: audit
        handler: registrationTask
  - name: OTHER
    subjects: [other.>]
    consumers:
      - durable: worker
        handler: registrationTask
  - name: EMPTY
    subjects: [empty.>]
    consumers: []
  - name: NULL_CONSUMERS
    subjects: [null.>]
    consumers: null
  - name: OMITTED
    subjects: [omitted.>]
`)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cfg.bind(1, natsx.MustNewJetStreamHandlerRegistry(registrationTask), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.streams) != 5 || len(batch.jetStreamConsumers) != 3 {
		t.Fatalf("应保留无消费者的 Stream，并展开全部嵌套消费者: %+v", batch)
	}
	want := []natsx.JetStreamConsumer{
		natsx.DefaultJetStreamConsumer("ORDERS", "worker", nil, "orders.created"),
		natsx.DefaultJetStreamConsumer("ORDERS", "audit", nil),
		natsx.DefaultJetStreamConsumer("OTHER", "worker", nil),
	}
	for i, consumer := range batch.jetStreamConsumers {
		if consumer.Handler == nil {
			t.Fatalf("消费者 %d 未绑定 Handler", i)
		}
		consumer.Handler = nil
		if !reflect.DeepEqual(consumer, want[i]) {
			t.Fatalf("消费者应按声明顺序继承所属 Stream 和默认配置: got %+v, want %+v", consumer, want[i])
		}
	}
}

func TestNATSRegistrationRejectsLegacyConsumers(t *testing.T) {
	for _, value := range []string{"null", "[]", "[{stream: EXTERNAL, durable: worker, handler: registrationTask}]"} {
		t.Run(value, func(t *testing.T) {
			_, err := parseNATSRegistration(t, "streamDefaults: {replicas: 1}\njetStreamConsumers: "+value)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "jetstreamconsumers") {
				t.Fatalf("旧顶层清单即使为空也须拒绝: %v", err)
			}
		})
	}
	for _, value := range []string{"null", "OTHER"} {
		t.Run("stream-"+value, func(t *testing.T) {
			_, err := parseNATSRegistration(t, `
streamDefaults: {replicas: 1}
streams:
  - name: ORDERS
    subjects: [orders.>]
    consumers:
      - durable: worker
        handler: registrationTask
        stream: `+value)
			if err == nil || !strings.Contains(err.Error(), "streams[0].consumers[0]") || !strings.Contains(err.Error(), "invalid keys: stream") {
				t.Fatalf("消费者不得覆盖所属 Stream: %v", err)
			}
		})
	}
}

func TestNATSRegistrationNestedHandlerLocation(t *testing.T) {
	cfg, err := parseNATSRegistration(t, `
streamDefaults: {replicas: 1}
streams:
  - name: ORDERS
    subjects: [orders.>]
    consumers:
      - durable: worker
        handler: registrationTask
      - durable: audit
        handler: missing
`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cfg.bind(1, natsx.MustNewJetStreamHandlerRegistry(registrationTask), nil)
	if err == nil || !strings.Contains(err.Error(), `streams[0].consumers[1] "ORDERS"/"audit"`) {
		t.Fatalf("未知 Handler 应定位到嵌套声明: %v", err)
	}
}

func TestNATSRegistrationExampleOptions(t *testing.T) {
	content, err := os.ReadFile("conf.example/nats-register.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	optional := regexp.MustCompile(`(?m)^( *)# ( *)(streamDefaults|replicas|maxBytes|retention|maxAge|duplicates|ackWait|maxDeliver|maxAckPending|consume|workers|maxMessages|retryDelay):`)
	if got := len(optional.FindAll(content, -1)); got != 14 {
		t.Fatalf("模板应保留全部可选参数的注释示例，实际 %d 项", got)
	}
	js := natsx.MustNewJetStreamHandlerRegistry()
	js.MustRegister(registrationTask, "HandleOrderCreated")
	core := natsx.MustNewCoreHandlerRegistry()
	core.MustRegister(registrationCore, "HandleConfigRefresh")
	core.MustRegister(registrationCore, "HandleNotification")
	for _, uncomment := range []bool{false, true} {
		name := "defaults"
		if uncomment {
			name = "uncommented-options"
		}
		t.Run(name, func(t *testing.T) {
			source := string(content)
			wantReplicas := 3
			if uncomment {
				// 只去掉注释标记，保留原缩进，验证用户可直接取消注释使用。
				source = optional.ReplaceAllString(source, "${1}${2}${3}:")
				wantReplicas = 1
			}
			cfg, err := parseNATSRegistration(t, source)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := cfg.bind(3, js, core)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.streams) != 1 || len(batch.jetStreamConsumers) != 1 || len(batch.coreConsumers) != 2 {
				t.Fatalf("模板应完整绑定 Stream 和两类消费者: %+v", batch)
			}
			consumer := batch.jetStreamConsumers[0]
			consumer.Handler = nil
			if !reflect.DeepEqual(batch.streams[0], natsx.DefaultStreamConfig("ORDERS", wantReplicas, "orders.>")) ||
				!reflect.DeepEqual(consumer, natsx.DefaultJetStreamConsumer("ORDERS", "order-created-worker", nil, "orders.created")) {
				t.Fatalf("模板应默认推导副本数、取消注释后显式覆盖，其他默认值保持一致: stream=%+v, consumer=%+v", batch.streams[0], consumer)
			}
		})
	}
}

func TestNATSRegistrationDefaultsAndOverrides(t *testing.T) {
	cfg, err := parseNATSRegistration(t, registrationYAML)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cfg.bind(
		1,
		natsx.MustNewJetStreamHandlerRegistry(registrationTask),
		natsx.MustNewCoreHandlerRegistry(registrationCore),
	)
	if err != nil {
		t.Fatal(err)
	}
	wantStreams := []jetstream.StreamConfig{
		natsx.DefaultStreamConfig("ORDERS", 3, "orders.>"),
		natsx.DefaultStreamConfig("TEMP", 1, "temp.>"),
	}
	if !reflect.DeepEqual(batch.streams, wantStreams) {
		t.Fatalf("副本继承、覆盖或 Stream 默认值错误: %+v", batch.streams)
	}
	consumer := batch.jetStreamConsumers[0]
	if consumer.Handler == nil {
		t.Fatal("未绑定 JetStream Handler")
	}
	consumer.Handler = nil
	want := natsx.DefaultJetStreamConsumer("ORDERS", "worker", nil, "orders.created")
	if !reflect.DeepEqual(consumer, want) {
		t.Fatalf("未沿用 JetStream Default: %+v", consumer)
	}
	for i := range batch.coreConsumers {
		if batch.coreConsumers[i].Handler == nil {
			t.Fatal("未绑定 Core Handler")
		}
		batch.coreConsumers[i].Handler = nil
	}
	wantCore := []natsx.CoreConsumer{
		natsx.DefaultCoreQueueConsumer("broadcast", "updates", "", 0, nil),
		natsx.DefaultCoreQueueConsumer("queue", "tasks", "workers", 4, nil),
	}
	if !reflect.DeepEqual(batch.coreConsumers, wantCore) {
		t.Fatalf("未沿用 Core Default: %+v", batch.coreConsumers)
	}
}

func TestNATSRegistrationMaxBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  int64
	}{
		{
			name: "omitted",
			want: -1,
		},
		{
			name:  "null",
			value: "null",
			want:  -1,
		},
		{
			name:  "small-stream",
			value: "104857600",
			want:  100 << 20,
		},
		{
			name:  "above-int32",
			value: "8589934592",
			want:  8 << 30,
		},
		{
			name:  "zero",
			value: "0",
		},
		{
			name:  "unlimited",
			value: "-1",
			want:  -1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := registrationYAML
			if tc.value != "" {
				content = strings.Replace(content, "subjects: [orders.>]", "subjects: [orders.>]\n    maxBytes: "+tc.value, 1)
			}
			cfg, err := parseNATSRegistration(t, content)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := cfg.bind(
				1,
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := batch.streams[0].MaxBytes; got != tc.want {
				t.Fatalf("Stream 容量覆盖错误: got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestNATSRegistrationRejectsLegacyNamespace(t *testing.T) {
	for _, value := range []string{"null", "''", "test1"} {
		t.Run(value, func(t *testing.T) {
			_, err := parseNATSRegistration(t, "namespace: "+value+"\n"+registrationYAML)
			if err == nil || !strings.Contains(err.Error(), "namespace") {
				t.Fatalf("旧注册配置的 namespace 应作为未知字段拒绝: %v", err)
			}
		})
	}
}

func TestNATSRegistrationPreservesEmptyAndInvalidFields(t *testing.T) {
	for _, value := range []string{"", " leading", "trailing ", " "} {
		t.Run(value, func(t *testing.T) {
			cfg, err := parseNATSRegistration(t, registrationYAML)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Streams[0].Name = value
			cfg.Streams[0].Subjects = []string{value}
			cfg.Streams[0].Consumers[0].Durable = value
			cfg.Streams[0].Consumers[0].FilterSubjects = []string{value}
			cfg.CoreConsumers[0].Name = value
			cfg.CoreConsumers[0].Subject = value
			cfg.CoreConsumers[1].Queue = value
			batch, err := cfg.bind(
				1,
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range []string{
				batch.streams[0].Name,
				batch.streams[0].Subjects[0],
				batch.jetStreamConsumers[0].Stream,
				batch.jetStreamConsumers[0].Config.Durable,
				batch.jetStreamConsumers[0].Config.FilterSubjects[0],
				batch.coreConsumers[0].Name,
				batch.coreConsumers[0].Subject,
				batch.coreConsumers[1].Queue,
			} {
				if got != value {
					t.Fatalf("空字段和首尾空白应保留原值，交给原注册入口校验: got %q, want %q", got, value)
				}
			}
			cfg.Streams[0].Consumers[0].FilterSubjects = nil
			batch, err = cfg.bind(
				1,
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
			if err != nil || len(batch.jetStreamConsumers[0].Config.FilterSubjects) != 0 {
				t.Fatalf("未配置过滤主题时仍应消费整个 Stream: %v", err)
			}
		})
	}
}

func TestNATSRegistrationJetStreamOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options string
		want    func(*natsx.JetStreamConsumer)
	}{
		{
			name: "null",
			options: `        ackWait: null
        maxDeliver: null
        maxAckPending: null
        consume: null
        retryDelay: null
`,
		},
		{
			name: "null-consume-fields",
			options: `        consume:
          workers: null
          maxMessages: null
`,
		},
		{
			name:    "empty-consume",
			options: "        consume: {}\n",
		},
		{
			name: "overrides",
			options: `        ackWait: 45s
        maxDeliver: 7
        maxAckPending: 200
        consume:
          workers: 4
          maxMessages: 16
        retryDelay: 250ms
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Config.AckWait = 45 * time.Second
				consumer.Config.MaxDeliver = 7
				consumer.Config.MaxAckPending = 200
				consumer.Consume.Workers = 4
				consumer.Consume.MaxMessages = 16
				consumer.RetryDelay = 250 * time.Millisecond
			},
		},
		{
			name: "workers-only",
			options: `        consume:
          workers: 3
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Consume.Workers = 3
			},
		},
		{
			name: "max-messages-only",
			options: `        consume:
          maxMessages: 8
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Consume.MaxMessages = 8
			},
		},
		{
			name: "explicit-zero",
			options: `        ackWait: 0
        maxDeliver: 0
        maxAckPending: 0
        consume:
          workers: 0
          maxMessages: 0
        retryDelay: 0
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Config.AckWait = 0
				consumer.Config.MaxDeliver = 0
				consumer.Config.MaxAckPending = 0
				consumer.Consume.Workers = 0
				consumer.Consume.MaxMessages = 0
				consumer.RetryDelay = 0
			},
		},
		{
			name: "unlimited",
			options: `        maxDeliver: -1
        maxAckPending: -1
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Config.MaxDeliver = -1
				consumer.Config.MaxAckPending = -1
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := strings.Replace(registrationYAML, "        handler: registrationTask\n", "        handler: registrationTask\n"+tc.options, 1)
			cfg, err := parseNATSRegistration(t, content)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := cfg.bind(
				1,
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
			if err != nil {
				t.Fatal(err)
			}
			consumer := batch.jetStreamConsumers[0]
			if consumer.Handler == nil {
				t.Fatal("覆盖可选配置后仍须绑定 Handler")
			}
			consumer.Handler = nil
			want := natsx.DefaultJetStreamConsumer("ORDERS", "worker", nil, "orders.created")
			if tc.want != nil {
				tc.want(&want)
			}
			if !reflect.DeepEqual(consumer, want) {
				t.Fatalf("可选配置应区分缺省与显式值，其余字段保留 Default: got %+v, want %+v", consumer, want)
			}
		})
	}
}

func TestNATSRegistrationRejectsInvalidJetStreamOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options string
		want    string
	}{
		{
			name:    "invalid-ack-wait",
			options: "        ackWait: 1d\n",
			want:    "ackWait",
		},
		{
			name:    "invalid-retry-delay",
			options: "        retryDelay: later\n",
			want:    "retryDelay",
		},
		{
			name:    "string-max-deliver",
			options: "        maxDeliver: '5'\n",
			want:    "maxDeliver",
		},
		{
			name:    "map-max-ack-pending",
			options: "        maxAckPending: {}\n",
			want:    "maxAckPending",
		},
		{
			name:    "scalar-consume",
			options: "        consume: invalid\n",
			want:    "consume",
		},
		{
			name:    "string-workers",
			options: "        consume:\n          workers: '2'\n",
			want:    "workers",
		},
		{
			name:    "bool-max-messages",
			options: "        consume:\n          maxMessages: true\n",
			want:    "maxMessages",
		},
		{
			name:    "unknown-null-consume-field",
			options: "        consume:\n          unknown: null\n",
			want:    "unknown",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := strings.Replace(registrationYAML, "        handler: registrationTask\n", "        handler: registrationTask\n"+tc.options, 1)
			_, err := parseNATSRegistration(t, content)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("应拒绝非法消费配置并包含 %q，实际: %v", tc.want, err)
			}
		})
	}
}

func TestNATSRegistrationPreservesSubjectDecoding(t *testing.T) {
	content := strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: orders.>,audit.>", 1)
	content = strings.Replace(content, "filterSubjects: [orders.created]", "filterSubjects: orders.created,audit.created", 1)
	cfg, err := parseNATSRegistration(t, content)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cfg.bind(
		1,
		natsx.MustNewJetStreamHandlerRegistry(registrationTask),
		natsx.MustNewCoreHandlerRegistry(registrationCore),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(batch.streams[0].Subjects, []string{"orders.>", "audit.>"}) ||
		!reflect.DeepEqual(batch.jetStreamConsumers[0].Config.FilterSubjects, []string{"orders.created", "audit.created"}) {
		t.Fatalf("主题字符串应继续按逗号解码为切片: streams=%+v, consumer=%+v", batch.streams, batch.jetStreamConsumers[0].Config)
	}
}

func TestNATSRegistrationOptionalReplicas(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{
			value: "null",
			want:  3,
		},
		{
			value: "0",
			want:  3,
		},
		{
			value: "-1",
			want:  3,
		},
		{
			value: "2",
			want:  2,
		},
		{
			value: "3",
			want:  3,
		},
		{
			value: "1.5",
			want:  1,
		},
	} {
		t.Run(tc.value, func(t *testing.T) {
			content := strings.Replace(registrationYAML, "replicas: 1", "replicas: "+tc.value, 1)
			cfg, err := parseNATSRegistration(t, content)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := cfg.bind(
				1,
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := batch.streams[1].Replicas; got != tc.want {
				t.Fatalf("可选副本数仅在解码后为正数时覆盖: got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestNATSRegistrationQueueSubscribers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config string
		valid  bool
	}{
		{name: "omitted"},
		{
			name:   "null",
			config: "    subscribers: null\n",
		},
		{
			name:   "negative",
			config: "    subscribers: -1\n",
		},
		{
			name:   "zero",
			config: "    subscribers: 0\n",
		},
		{
			name:   "one",
			config: "    subscribers: 1\n",
		},
		{
			name:   "two",
			config: "    subscribers: 2\n",
			valid:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := strings.Replace(registrationYAML, "    subscribers: 4\n", tc.config, 1)
			cfg, err := parseNATSRegistration(t, content)
			if err != nil {
				t.Fatal(err)
			}
			_, err = cfg.bind(
				1,
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
			if tc.valid {
				if err != nil {
					t.Fatalf("队列订阅数大于 1 应合法: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), `coreConsumers[1] "queue" subscribers`) || !strings.Contains(err.Error(), "must be greater than 1") {
				t.Fatalf("队列订阅数不大于 1 应在配置层拒绝: %v", err)
			}
		})
	}
}

func TestNATSRegistrationRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "zero-default", content: "streamDefaults: {replicas: 0}", want: "streamDefaults.replicas"},
		{name: "negative-default", content: "streamDefaults: {replicas: -1}", want: "streamDefaults.replicas"},
		{name: "string-default", content: "streamDefaults: {replicas: '3'}", want: "streamDefaults.replicas"},
		{name: "bool-default", content: "streamDefaults: {replicas: true}", want: "streamDefaults.replicas"},
		{name: "invalid-default-with-overrides", content: "streamDefaults: {replicas: 0}\nstreams: [{name: A, replicas: 1, subjects: [a]}]", want: "streamDefaults.replicas"},
		{name: "string-override", content: strings.Replace(registrationYAML, "replicas: 1", "replicas: '1'", 1), want: "streams[1]"},
		{
			name:    "override-above-default",
			content: strings.Replace(registrationYAML, "replicas: 1", "replicas: 4", 1),
			want:    `streams[1] "TEMP" replicas 4 must not exceed streamDefaults.replicas 3`,
		},
		{name: "unknown", content: registrationYAML + "\nextra: true", want: "extra"},
		{name: "unknown-stream", content: strings.Replace(registrationYAML, "replicas: 1", "replica: 1", 1), want: "replica"},
		{
			name:    "string-max-bytes",
			content: strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: [orders.>]\n    maxBytes: '100MiB'", 1),
			want:    "maxBytes",
		},
		{
			name:    "bool-max-bytes",
			content: strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: [orders.>]\n    maxBytes: true", 1),
			want:    "maxBytes",
		},
		{name: "runtime-option", content: strings.Replace(registrationYAML, "durable: worker", "durable: worker\n        workers: 2", 1), want: "workers"},
		{
			name:    "unknown-retention",
			content: strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: [orders.>]\n    retention: unknown", 1),
			want:    `streams[0] "ORDERS" retention`,
		},
		{
			name:    "empty-retention",
			content: strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: [orders.>]\n    retention: ''", 1),
			want:    `streams[0] "ORDERS" retention`,
		},
		{
			name:    "numeric-retention",
			content: strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: [orders.>]\n    retention: 1", 1),
			want:    "retention",
		},
		{
			name:    "invalid-max-age",
			content: strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: [orders.>]\n    maxAge: 1d", 1),
			want:    "maxAge",
		},
		{
			name:    "invalid-duplicates",
			content: strings.Replace(registrationYAML, "subjects: [orders.>]", "subjects: [orders.>]\n    duplicates: forever", 1),
			want:    "duplicates",
		},
		{
			name:    "string-subscribers",
			content: strings.Replace(registrationYAML, "subscribers: 4", "subscribers: '4'", 1),
			want:    "subscribers",
		},
		{name: "empty-subjects", content: strings.Replace(registrationYAML, "[orders.>]", "[]", 1), want: "subjects"},
		{
			name:    "missing-subjects",
			content: strings.Replace(registrationYAML, "    subjects: [orders.>]\n", "", 1),
			want:    "subjects",
		},
		{name: "duplicate-yaml-key", content: registrationYAML + "\nstreams: []", want: "already defined"},
		{name: "wrong-kind", content: strings.Replace(registrationYAML, "handler: registrationTask", "handler: registrationCore", 1), want: "unknown JetStream handler"},
		{name: "unknown-core", content: strings.Replace(registrationYAML, "handler: registrationCore", "handler: missing", 1), want: "unknown Core handler"},
		{
			name:    "empty-jetstream-handler",
			content: strings.Replace(registrationYAML, "handler: registrationTask", "handler: ''", 1),
			want:    "unknown JetStream handler",
		},
		{
			name:    "whitespace-jetstream-handler",
			content: strings.Replace(registrationYAML, "handler: registrationTask", "handler: ' registrationTask '", 1),
			want:    "unknown JetStream handler",
		},
		{
			name:    "empty-core-handler",
			content: strings.Replace(registrationYAML, "handler: registrationCore", "handler: ''", 1),
			want:    "unknown Core handler",
		},
		{
			name:    "whitespace-core-handler",
			content: strings.Replace(registrationYAML, "handler: registrationCore", "handler: ' registrationCore '", 1),
			want:    "unknown Core handler",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseNATSRegistration(t, tc.content)
			if err == nil {
				_, err = cfg.bind(
					1,
					natsx.MustNewJetStreamHandlerRegistry(registrationTask),
					natsx.MustNewCoreHandlerRegistry(registrationCore),
				)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("应拒绝配置并包含 %q，实际: %v", tc.want, err)
			}
		})
	}
}

func TestNATSRegistrationInferredReplicas(t *testing.T) {
	cfg, err := parseNATSRegistration(t, `
streams:
  - name: EVENTS
    subjects: [events]
  - name: TEMP
    subjects: [temp]
    replicas: 1
`)
	if err != nil {
		t.Fatal(err)
	}
	for _, urlCount := range []int{0, 3, 1, 6} {
		batch, err := cfg.bind(urlCount, nil, nil)
		if urlCount == 0 {
			if err == nil || !strings.Contains(err.Error(), "streamDefaults.replicas") {
				t.Fatalf("不能从空 URL 列表推导副本数: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if batch.streams[0].Replicas != urlCount || batch.streams[1].Replicas != 1 {
			t.Fatalf("全局副本数应随本次 URL 数量推导且保留逐项覆盖，不裁剪服务端上限: %+v", batch.streams)
		}
	}
}

func TestNATSRegistrationEmpty(t *testing.T) {
	for _, content := range []string{
		"",
		"streamDefaults: null",
		"streamDefaults: {}",
		"streamDefaults: {replicas: null}",
		"streamDefaults: {replicas: 1}",
	} {
		cfg, err := parseNATSRegistration(t, content)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := cfg.bind(1, nil, nil)
		if err != nil || len(batch.streams)+len(batch.jetStreamConsumers)+len(batch.coreConsumers) != 0 {
			t.Fatalf("显式或自动推导全局副本数的空清单应合法: config=%q, batch=%+v, err=%v", content, batch, err)
		}
	}
}

func TestNATSRegistrationRejectsReplicaContainersAndUnknownNulls(t *testing.T) {
	for _, value := range []string{"{}", "[]"} {
		for _, scope := range []string{"default", "override"} {
			t.Run(scope+"/"+value, func(t *testing.T) {
				original := "replicas: 3"
				location := "streamDefaults.replicas"
				if scope == "override" {
					original = "replicas: 1"
					location = "streams[1]"
				}
				content := strings.Replace(registrationYAML, original, "replicas: "+value, 1)
				cfg, err := parseNATSRegistration(t, content)
				if err == nil {
					_, err = cfg.bind(
						1,
						natsx.MustNewJetStreamHandlerRegistry(registrationTask),
						natsx.MustNewCoreHandlerRegistry(registrationCore),
					)
				}
				if err == nil || !strings.Contains(err.Error(), location) {
					t.Fatalf("应拒绝非整数副本配置并包含 %q，实际: %v", location, err)
				}
			})
		}
	}
	for _, content := range []string{
		registrationYAML + "\nunknown: null",
		strings.Replace(registrationYAML, "replicas: 3", "replicas: 3\n  unknown: null", 1),
		strings.Replace(registrationYAML, "replicas: 1", "replicas: 1\n    unknown: null", 1),
		strings.Replace(registrationYAML, "durable: worker", "durable: worker\n        unknown: null", 1),
		strings.Replace(registrationYAML, "name: queue", "name: queue\n    unknown: null", 1),
	} {
		cfg, err := parseNATSRegistration(t, content)
		if err == nil {
			_, err = cfg.bind(
				1,
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
		}
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Fatalf("未知字段即使为 null 也应报错: %v", err)
		}
	}
}
