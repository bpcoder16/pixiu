package common_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/common"
	"github.com/bpcoder16/pixiu/infra/env"
)

func TestFormatTime(t *testing.T) {
	// env 只能初始化一次，用子进程隔离；系统时区与应用时区不同。
	if os.Getenv("PIXIU_COMMON_TIME_TEST_CHILD") == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFormatTime$")
		cmd.Env = append(os.Environ(), "PIXIU_COMMON_TIME_TEST_CHILD=1", "TZ=UTC")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("FormatTime test process: %v\n%s", err, output)
		}
		return
	}

	if got := common.FormatTime(nil); got != "" {
		t.Fatalf("FormatTime(nil) = %q, want empty string", got)
	}
	err := env.Init(env.Config{
		AppName:       "common-time-test",
		RunMode:       env.RunModeTest,
		TimeLocation:  "Asia/Shanghai",
		ConfigDirPath: t.TempDir(),
		LocalIP:       "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		value time.Time
		want  string
	}{
		{
			name:  "应用时区跨年并省略小数秒",
			value: time.Date(2025, time.December, 31, 20, 30, 5, 123456789, time.UTC),
			want:  "2026-01-01T04:30:05+08:00",
		},
		{
			name:  "输入带其他时区",
			value: time.Date(2026, time.January, 1, 12, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60)),
			want:  "2026-01-02T01:00:00+08:00",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.value
			if got := common.FormatTime(&tc.value); got != tc.want {
				t.Fatalf("FormatTime(%v) = %q, want %q", tc.value, got, tc.want)
			}
			if tc.value != original {
				t.Fatal("FormatTime changed the input time")
			}
		})
	}
}
