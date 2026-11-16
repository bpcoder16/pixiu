//go:build !pixiu_bootstrap_no_nats

package bootstrap

import (
	"reflect"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNATSStreamModes(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want natsx.StreamMode
	}{
		{want: natsx.StreamModeCreate},
		{mode: "null", want: natsx.StreamModeCreate},
		{mode: "create", want: natsx.StreamModeCreate},
		{mode: "upsert", want: natsx.StreamModeUpsert},
		{mode: "existing", want: natsx.StreamModeExisting},
	} {
		source := "streams:\n  - name: TASKS\n"
		if tc.mode != "" {
			source += "    mode: " + tc.mode + "\n"
		}
		if tc.want != natsx.StreamModeExisting {
			source += "    subjects: [tasks]\n"
		}
		source += "    consumers:\n      - durable: worker\n        handler: registrationTask\n"
		cfg, err := parseNATSRegistration(t, source)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := cfg.bind(1, natsx.MustNewJetStreamHandlerRegistry(registrationTask), nil)
		if err != nil {
			t.Fatal(err)
		}
		wantConfig := natsx.DefaultStreamConfig("TASKS", 1, "tasks")
		if tc.want == natsx.StreamModeExisting {
			wantConfig = jetstream.StreamConfig{Name: "TASKS"}
		}
		if batch.streams[0].Mode != tc.want || !reflect.DeepEqual(batch.streams[0].Config, wantConfig) || batch.jetStreamConsumers[0].Stream != "TASKS" {
			t.Fatalf("模式 %q 的配置或消费者归属错误: %+v", tc.mode, batch)
		}
	}
}

func TestNATSStreamModesRejectInvalidConfig(t *testing.T) {
	for _, fields := range []string{
		"mode: unknown", "mode: ''", "mode: true", "mode: [existing]", "mode: EXISTING",
		"mode: create", "mode: upsert",
		"mode: existing\n    subjects: [tasks]",
		"mode: existing\n    replicas: 0",
		"mode: existing\n    maxBytes: 0",
		"mode: existing\n    retention: limits",
		"mode: existing\n    maxAge: 0s",
		"mode: existing\n    duplicates: 0s",
	} {
		t.Run(fields, func(t *testing.T) {
			cfg, err := parseNATSRegistration(t, "streams:\n  - name: TASKS\n    "+fields)
			if err == nil {
				_, err = cfg.bind(1, nil, nil)
			}
			if err == nil || !strings.Contains(err.Error(), "streams[0]") {
				t.Fatalf("应拒绝无效模式或配置并定位条目: %v", err)
			}
		})
	}
	if _, err := parseNATSRegistration(t, "streamDefaults: {mode: existing}"); err == nil {
		t.Fatal("mode 只能逐 Stream 配置")
	}
}
