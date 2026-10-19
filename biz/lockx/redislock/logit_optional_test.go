package redislock

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/lockx"
)

func TestTryDoWithoutLogitInitialization(t *testing.T) {
	const helperEnv = "PIXIU_TEST_REDISLOCK_WITHOUT_LOGIT"
	if os.Getenv(helperEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTryDoWithoutLogitInitialization$")
		cmd.Env = append(os.Environ(), helperEnv+"=1")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("未配置 logit 时锁调用失败: %v; stdout=%q stderr=%q", err, stdout.String(), stderr.String())
		}
		if stdout.String() != "PASS\n" || stderr.Len() != 0 {
			t.Fatalf("未配置 logit 时不应输出 Redis 日志: stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
		return
	}
	for _, failed := range []bool{false, true} {
		client := newWireClient(t, -1, func(args []string) string {
			if strings.EqualFold(args[0], "EVAL") {
				return ":1\r\n"
			}
			if failed {
				return "-ERR unavailable\r\n"
			}
			return "+OK\r\n"
		})
		called := false
		executed, err := lockx.TryDo(context.Background(), mustNew(t, client), "key", func(context.Context) error {
			called = true
			return nil
		})
		if executed != !failed || called != !failed || (err != nil) != failed {
			t.Fatalf("锁调用结果改变: failed=%v executed=%v called=%v err=%v", failed, executed, called, err)
		}
	}
}
