//go:build !pixiu_bootstrap_no_nats

package bootstrap

import (
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/natsx"
)

func TestNATSConsumerModes(t *testing.T) {
	for _, streamMode := range []string{"create", "upsert", "existing"} {
		for _, tc := range []struct {
			value string
			want  natsx.ConsumerMode
		}{
			{want: natsx.ConsumerModeCreate},
			{value: "null", want: natsx.ConsumerModeCreate},
			{value: "create", want: natsx.ConsumerModeCreate},
			{value: "upsert", want: natsx.ConsumerModeUpsert},
		} {
			t.Run(streamMode+"/"+tc.value, func(t *testing.T) {
				source := "streams:\n  - name: TASKS\n    mode: " + streamMode + "\n"
				if streamMode != "existing" {
					source += "    subjects: [tasks]\n"
				}
				source += "    consumers:\n      - durable: worker\n        handler: registrationTask\n"
				if tc.value != "" {
					source += "        mode: " + tc.value + "\n"
				}
				// 同一 Stream 中未指定模式的消费者始终沿用 create。
				source += "      - durable: other\n        handler: registrationTask\n"
				cfg, err := parseNATSRegistration(t, source)
				if err != nil {
					t.Fatal(err)
				}
				batch, err := cfg.bind(1, natsx.MustNewJetStreamHandlerRegistry(registrationTask), nil)
				if err != nil {
					t.Fatal(err)
				}
				if batch.jetStreamConsumers[0].Mode != tc.want || batch.jetStreamConsumers[1].Mode != natsx.ConsumerModeCreate {
					t.Fatalf("Consumer 模式不应继承所属 Stream 或相邻 Consumer: %+v", batch.jetStreamConsumers)
				}
			})
		}
	}
}

func TestNATSConsumerModesRejectInvalidConfig(t *testing.T) {
	for _, value := range []string{"''", "unknown", "existing", "UPSERT", "' upsert '", "true", "1", "[upsert]"} {
		t.Run(value, func(t *testing.T) {
			cfg, err := parseNATSRegistration(t, "streams:\n  - name: TASKS\n    subjects: [tasks]\n    consumers:\n      - durable: worker\n        handler: registrationTask\n        mode: "+value+"\n")
			if err == nil {
				_, err = cfg.bind(1, natsx.MustNewJetStreamHandlerRegistry(registrationTask), nil)
			}
			if err == nil || !strings.Contains(err.Error(), "streams[0].consumers[0]") {
				t.Fatalf("应拒绝非法 Consumer 模式并定位条目: %v", err)
			}
		})
	}
	for _, source := range []string{
		"mode: upsert",
		"consumerDefaults: {mode: upsert}",
		"coreConsumers:\n  - name: worker\n    mode: upsert\n    subject: tasks\n    handler: registrationCore",
	} {
		if _, err := parseNATSRegistration(t, source); err == nil {
			t.Fatalf("mode 只能在单个 JetStream Consumer 上声明: %s", source)
		}
	}
}
