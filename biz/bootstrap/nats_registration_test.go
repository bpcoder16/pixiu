package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
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
  - name: TEMP
    replicas: 1
    subjects: [temp.>]
jetStreamConsumers:
  - stream: ORDERS
    durable: worker
    filterSubjects: [orders.created]
    handler: registrationTask
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

func TestNATSRegistrationDefaultsAndOverrides(t *testing.T) {
	cfg, err := parseNATSRegistration(t, registrationYAML)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cfg.bind(
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
			cfg.JetStreamConsumers[0].Stream = value
			cfg.JetStreamConsumers[0].Durable = value
			cfg.JetStreamConsumers[0].FilterSubjects = []string{value}
			cfg.CoreConsumers[0].Name = value
			cfg.CoreConsumers[0].Subject = value
			cfg.CoreConsumers[1].Queue = value
			batch, err := cfg.bind(
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
			cfg.JetStreamConsumers[0].FilterSubjects = nil
			batch, err = cfg.bind(
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
			options: `    ackWait: null
    maxDeliver: null
    maxAckPending: null
    consume: null
    retryDelay: null
`,
		},
		{
			name: "null-consume-fields",
			options: `    consume:
      workers: null
      maxMessages: null
`,
		},
		{
			name:    "empty-consume",
			options: "    consume: {}\n",
		},
		{
			name: "overrides",
			options: `    ackWait: 45s
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
			options: `    consume:
      workers: 3
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Consume.Workers = 3
			},
		},
		{
			name: "max-messages-only",
			options: `    consume:
      maxMessages: 8
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Consume.MaxMessages = 8
			},
		},
		{
			name: "explicit-zero",
			options: `    ackWait: 0
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
			options: `    maxDeliver: -1
    maxAckPending: -1
`,
			want: func(consumer *natsx.JetStreamConsumer) {
				consumer.Config.MaxDeliver = -1
				consumer.Config.MaxAckPending = -1
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := strings.Replace(registrationYAML, "    handler: registrationTask\n", "    handler: registrationTask\n"+tc.options, 1)
			cfg, err := parseNATSRegistration(t, content)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := cfg.bind(
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
			options: "    ackWait: 1d\n",
			want:    "ackWait",
		},
		{
			name:    "invalid-retry-delay",
			options: "    retryDelay: later\n",
			want:    "retryDelay",
		},
		{
			name:    "string-max-deliver",
			options: "    maxDeliver: '5'\n",
			want:    "maxDeliver",
		},
		{
			name:    "map-max-ack-pending",
			options: "    maxAckPending: {}\n",
			want:    "maxAckPending",
		},
		{
			name:    "scalar-consume",
			options: "    consume: invalid\n",
			want:    "consume",
		},
		{
			name:    "string-workers",
			options: "    consume:\n      workers: '2'\n",
			want:    "workers",
		},
		{
			name:    "bool-max-messages",
			options: "    consume:\n      maxMessages: true\n",
			want:    "maxMessages",
		},
		{
			name:    "unknown-null-consume-field",
			options: "    consume:\n      unknown: null\n",
			want:    "unknown",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := strings.Replace(registrationYAML, "    handler: registrationTask\n", "    handler: registrationTask\n"+tc.options, 1)
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
		{name: "missing-default", content: "streams: []", want: "streamDefaults.replicas"},
		{name: "null-default", content: "streamDefaults: {replicas: null}", want: "streamDefaults.replicas"},
		{name: "zero-default", content: "streamDefaults: {replicas: 0}", want: "streamDefaults.replicas"},
		{name: "negative-default", content: "streamDefaults: {replicas: -1}", want: "streamDefaults.replicas"},
		{name: "string-default", content: "streamDefaults: {replicas: '3'}", want: "streamDefaults.replicas"},
		{name: "bool-default", content: "streamDefaults: {replicas: true}", want: "streamDefaults.replicas"},
		{name: "override-still-requires-default", content: "streams: [{name: A, replicas: 1, subjects: [a]}]", want: "streamDefaults.replicas"},
		{name: "invalid-default-with-overrides", content: "streamDefaults: {replicas: 0}\nstreams: [{name: A, replicas: 1, subjects: [a]}]", want: "streamDefaults.replicas"},
		{name: "string-override", content: strings.Replace(registrationYAML, "replicas: 1", "replicas: '1'", 1), want: "streams[1]"},
		{
			name:    "override-above-default",
			content: strings.Replace(registrationYAML, "replicas: 1", "replicas: 4", 1),
			want:    `streams[1] "TEMP" replicas 4 must not exceed streamDefaults.replicas 3`,
		},
		{name: "unknown", content: registrationYAML + "\nextra: true", want: "extra"},
		{name: "unknown-stream", content: strings.Replace(registrationYAML, "replicas: 1", "replica: 1", 1), want: "replica"},
		{name: "runtime-option", content: strings.Replace(registrationYAML, "durable: worker", "durable: worker\n    workers: 2", 1), want: "workers"},
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
		{name: "duplicate-yaml-key", content: registrationYAML + "\njetStreamConsumers:\n  - stream: ORDERS\n    durable: worker\n    handler: registrationTask", want: "already defined"},
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

func TestNATSRegistrationEmptyAndExternalStream(t *testing.T) {
	cfg, err := parseNATSRegistration(t, "streamDefaults: {replicas: 1}")
	if err != nil {
		t.Fatal(err)
	}
	batch, err := cfg.bind(nil, nil)
	if err != nil || len(batch.streams)+len(batch.jetStreamConsumers)+len(batch.coreConsumers) != 0 {
		t.Fatalf("带全局默认的空清单应合法: %+v, %v", batch, err)
	}
	cfg, err = parseNATSRegistration(t, `
streamDefaults: {replicas: 1}
jetStreamConsumers:
  - stream: EXTERNAL
    durable: worker
    handler: registrationTask
  - stream: OTHER
    durable: worker
    handler: registrationTask
`)
	if err != nil {
		t.Fatal(err)
	}
	batch, err = cfg.bind(natsx.MustNewJetStreamHandlerRegistry(registrationTask), nil)
	if err != nil || len(batch.jetStreamConsumers) != 2 {
		t.Fatalf("应允许外部 Stream 及不同 Stream 的同名 Durable: %+v, %v", batch, err)
	}
	if len(batch.jetStreamConsumers[0].Config.FilterSubjects) != 0 {
		t.Fatal("省略过滤条件应消费整个 Stream")
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
		strings.Replace(registrationYAML, "durable: worker", "durable: worker\n    unknown: null", 1),
		strings.Replace(registrationYAML, "name: queue", "name: queue\n    unknown: null", 1),
	} {
		cfg, err := parseNATSRegistration(t, content)
		if err == nil {
			_, err = cfg.bind(
				natsx.MustNewJetStreamHandlerRegistry(registrationTask),
				natsx.MustNewCoreHandlerRegistry(registrationCore),
			)
		}
		if err == nil || !strings.Contains(err.Error(), "unknown") {
			t.Fatalf("未知字段即使为 null 也应报错: %v", err)
		}
	}
}
