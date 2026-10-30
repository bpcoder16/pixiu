package common_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bpcoder16/pixiu/biz/common"
	"github.com/bpcoder16/pixiu/infra/env"
)

func TestGenerateIDWithYYMMDDHHmmssSSS(t *testing.T) {
	// env 只能初始化一次，用子进程隔离；系统时区与应用时区不同。
	if os.Getenv("PIXIU_COMMON_ID_TEST_CHILD") == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestGenerateIDWithYYMMDDHHmmssSSS$")
		cmd.Env = append(os.Environ(), "PIXIU_COMMON_ID_TEST_CHILD=1", "TZ=UTC")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("GenerateIDWithYYMMDDHHmmssSSS test process: %v\n%s", err, output)
		}
		return
	}

	err := env.Init(env.Config{
		AppName:       "common-id-test",
		RunMode:       env.RunModeTest,
		TimeLocation:  "Asia/Shanghai",
		ConfigDirPath: t.TempDir(),
		LocalIP:       "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{name: "业务前缀", prefix: "SP"},
		{name: "空前缀", prefix: ""},
		{name: "原样保留前缀", prefix: " 订单_x- "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := time.Now().In(env.TimeLocation()).Truncate(time.Millisecond)
			id, err := common.GenerateIDWithYYMMDDHHmmssSSS(tc.prefix)
			after := time.Now().In(env.TimeLocation()).Truncate(time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(id, tc.prefix) || len(id) != len(tc.prefix)+19 {
				t.Fatalf("GenerateIDWithYYMMDDHHmmssSSS(%q) = %q; want prefix followed by 15 time digits and 4 random digits", tc.prefix, id)
			}

			timestamp := id[len(tc.prefix) : len(tc.prefix)+15]
			generatedAt, err := time.ParseInLocation("060102150405.000", timestamp[:12]+"."+timestamp[12:], env.TimeLocation())
			if err != nil {
				t.Fatalf("invalid timestamp %q: %v", timestamp, err)
			}
			if generatedAt.Before(before) || generatedAt.After(after) {
				t.Fatalf("timestamp %v is outside application time range [%v, %v]", generatedAt, before, after)
			}

			for _, char := range id[len(tc.prefix)+15:] {
				if char < '0' || char > '9' {
					t.Fatalf("invalid random digit %q in %q", char, id)
				}
			}
		})
	}

	t.Run("秒毫秒及随机数补零", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// 虚拟时钟从 2000-01-01 00:00 UTC 开始，上海时间跨日且毫秒需要补零。
			time.Sleep(20*time.Hour + 15*time.Minute + 5*time.Second + 7*time.Millisecond)
			previousReader := rand.Reader
			rand.Reader = bytes.NewReader([]byte{0, 7, 0x27, 0x0f})
			t.Cleanup(func() {
				rand.Reader = previousReader
			})
			for _, tc := range []struct {
				prefix string
				want   string
			}{
				{prefix: "PD", want: "PD0001020415050070007"},
				{prefix: "SKU", want: "SKU0001020415050079999"},
			} {
				id, err := common.GenerateIDWithYYMMDDHHmmssSSS(tc.prefix)
				if err != nil {
					t.Fatal(err)
				}
				if id != tc.want {
					t.Fatalf("GenerateIDWithYYMMDDHHmmssSSS(%q) = %q; want %q", tc.prefix, id, tc.want)
				}
			}
		})
	})
}
