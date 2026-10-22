package bootstrap_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/appconfig"
	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/cronx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

// 显式提供隔离 Redis 地址才执行；只操作本测试的随机前缀，不清空数据库。
func TestCronRedisLeaseAndDrain(t *testing.T) {
	address := os.Getenv("PIXIU_CRON_REDIS_ADDR")
	if address == "" {
		t.Skip("设置 PIXIU_CRON_REDIS_ADDR 运行真实 Redis 集成测试")
	}
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		host, portText, err := net.SplitHostPort(address)
		if err != nil {
			t.Fatal(err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			t.Fatal(err)
		}
		writeRedisConfig(t, "cron-lock", fmt.Sprintf("host: %q\nport: %d\nmaxRetries: -1\ndialTimeout: 500ms\nreadTimeout: 500ms\nwriteTimeout: 500ms\n", host, port))
		bootstrap.MustRegisterRedis("cron-lock", false)
		var resources lifecycle.Stack
		defer resources.Close()
		hook := false
		logDir := t.TempDir()
		bootstrap.MustBaseInit(&appconfig.AppConfig{Log: appconfig.LogConfig{
			Format:  "text",
			Dir:     logDir,
			HookStd: &hook,
		}}, &resources)
		client := redisx.Named("cron-lock").Client()
		prefix := "pixiu:cron:test:" + logit.NewLogID() + ":"
		defer client.Del(context.Background(), prefix+"job")
		writeCronConfig(t, fmt.Sprintf("lock:\n  redisName: cron-lock\n  keyPrefix: %q\n  ttl: 400ms\n  renewInterval: 80ms\njobs:\n  - name: job\n    every: 100ms\n    timeout: 5s\n", prefix))
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		finish := func() { releaseOnce.Do(func() { close(release) }) }
		var running, runs, overlaps atomic.Int32
		var canceled atomic.Bool
		handler := cronx.NamedHandler{
			Name: "job",
			Handler: func(ctx context.Context) error {
				if running.Add(1) != 1 {
					overlaps.Add(1)
				}
				if runs.Add(1) == 1 {
					close(entered)
				}
				defer running.Add(-1)
				<-release
				canceled.Store(ctx.Err() != nil)
				return nil
			},
		}
		var services []apprun.Service
		defer func() {
			finish()
			for _, s := range services {
				_ = stopAndWait(s.Stop, s.Wait, s.Cancel)
			}
		}()
		results := make(chan error, 2)
		for range 2 {
			s, err := bootstrap.NewCronService(handler)
			if err != nil {
				t.Fatal(err)
			}
			services = append(services, s)
			go func() { results <- s.Run() }()
		}
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("任务未进入")
		}
		// 覆盖多次原始 TTL，若续租未生效，另一个实例会进入同名任务。
		time.Sleep(1100 * time.Millisecond)
		if running.Load() != 1 || runs.Load() != 1 || overlaps.Load() != 0 {
			t.Fatalf("集群互斥或续租失败: running=%d runs=%d overlap=%d", running.Load(), runs.Load(), overlaps.Load())
		}
		if ttl, err := client.PTTL(context.Background(), prefix+"job").Result(); err != nil || ttl <= 0 {
			t.Fatalf("锁未续租: ttl=%v err=%v", ttl, err)
		}
		warnings, err := os.ReadFile(filepath.Join(logDir, env.AppName(), "biz", "wf.log"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(warnings), "lock_not_acquired") {
			t.Fatal("未记录另一个实例未获得锁的日志")
		}
		closed := make(chan error, 2)
		for _, s := range services {
			go func() { closed <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
		}
		time.Sleep(60 * time.Millisecond)
		finish()
		for range 2 {
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if canceled.Load() {
			t.Fatal("排空期不应取消业务 context")
		}
		if exists, err := client.Exists(context.Background(), prefix+"job").Result(); err != nil || exists != 0 {
			t.Fatalf("关闭后锁未释放: exists=%d err=%v", exists, err)
		}
		if err := client.Ping(context.Background()).Err(); err != nil {
			t.Fatalf("共享 Redis 被 Scheduler 关闭: %v", err)
		}
		if stats := logit.Default().(logit.WriteErrorStats); stats.WriteErrors() != 0 {
			t.Fatalf("排空期间日志写入失败: %v", stats.LastWriteError())
		}
	})
}
