package bootstrap_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

func TestBootstrapAppliesTimezoneBeforeOpeningLogs(t *testing.T) {
	for _, zone := range []string{"Asia/Shanghai", "Asia/Kathmandu"} {
		t.Run(zone, func(t *testing.T) {
			isolatedLog(t, func() {
				// 在独立进程内先模拟 UTC 部署环境，确保配置确实改变默认时区。
				time.Local = time.UTC
				dir := t.TempDir()
				path := filepath.Join(dir, "app.yaml")
				content := fmt.Sprintf(`env:
  appName: timezone-test
  runMode: debug
  timeLocation: %s
  localIP: 192.0.2.10
log:
  format: json
  dir: %q
  names: [worker]
`, zone, dir)
				if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
				cfg := httpconfig.MustLoadAppConfig(path)
				if time.Local != time.UTC {
					t.Fatal("配置加载仍应只发布环境，不修改进程时区")
				}
				location := env.TimeLocation()
				// bootstrap 使用已发布的环境快照，不重新解析调用方可修改的配置。
				cfg.Env.TimeLocation = "UTC"
				before := time.Now()
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInit(&cfg.AppConfig, &resources)
				afterInit := time.Now()
				if time.Local != location || afterInit.Location() != location {
					t.Fatalf("默认时区未使用已发布配置: got %v, want %v", time.Local, location)
				}
				for _, name := range []string{"", "worker"} {
					base := "timezone-test"
					if name != "" {
						base += "." + name
					}
					file := filepath.Join(dir, base+".info.log")
					// 在首次写入之前检查软链，确保创建轮转文件前已设置时区。
					target, err := os.Readlink(file)
					first := filepath.Base(file) + "." + before.In(location).Format("2006010215")
					last := filepath.Base(file) + "." + afterInit.In(location).Format("2006010215")
					if err != nil || target != first && target != last {
						t.Fatalf("初始轮转文件未使用配置时区: target=%q, err=%v", target, err)
					}
					logit.Info(logit.WithLoggerName(context.Background(), name), "timezone applied")
					data, err := os.ReadFile(file)
					if err != nil {
						t.Fatal(err)
					}
					var record struct {
						Time string `json:"ts"`
					}
					if err := json.Unmarshal(data, &record); err != nil {
						t.Fatal(err)
					}
					stamp, err := time.Parse(time.RFC3339Nano, record.Time)
					if err != nil {
						t.Fatalf("日志时间戳无效: %s, %v", data, err)
					}
					_, gotOffset := stamp.Zone()
					_, wantOffset := stamp.In(location).Zone()
					if gotOffset != wantOffset {
						t.Fatalf("日志时区偏移错误: got %d, want %d", gotOffset, wantOffset)
					}
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				if time.Local != location {
					t.Fatal("关闭资源不应恢复旧时区")
				}
			})
		})
	}
}
