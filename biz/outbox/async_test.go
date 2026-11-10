package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/logit"
	"gorm.io/gorm"
)

func TestDefaultSendAsyncTimeoutAndErrors(t *testing.T) {
	if !isolatedDefaults(t) {
		return
	}
	var output bytes.Buffer
	logger := logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(&output)),
	)
	previous := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() {
		logit.SetDefault(previous)
		_ = logit.Close(logger)
	})
	synctest.Test(t, func(t *testing.T) {
		var contexts []context.Context
		_, err := outbox.NewDefaultStore(outbox.StoreConfig{
			Backend: defaultTestBackend(t),
			MasterDB: func(ctx context.Context) *gorm.DB {
				contexts = append(contexts, ctx)
				// 停在数据库入口，验证调用立即返回及超时错误的异步日志。
				<-ctx.Done()
				return &gorm.DB{
					Config: &gorm.Config{},
					Error:  ctx.Err(),
				}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = outbox.NewDefaultWorker(map[outbox.HandlerKey]outbox.Handler{
			{Kind: "test.publish", Version: 1}: func(context.Context, outbox.Delivery) error {
				t.Error("数据库失败不应进入 Handler")
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		outbox.SendAsync(17)
		synctest.Wait()
		outbox.SendAsync(23)
		synctest.Wait()
		if time.Now() != start || len(contexts) != 2 {
			t.Fatalf("异步入口应立即返回并各执行一次: elapsed=%s calls=%d", time.Since(start), len(contexts))
		}
		if contexts[0] == contexts[1] {
			t.Fatal("不同任务不得共用执行 context")
		}
		for _, ctx := range contexts {
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Sub(start) != 5*time.Second || ctx.Err() != nil {
				t.Fatalf("异步发送应使用独立的 5 秒超时: deadline=%s err=%v", deadline, ctx.Err())
			}
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
		if len(contexts) != 2 || len(lines) != 2 {
			t.Fatalf("每个异步调用应只尝试一次并记录失败: calls=%d logs=%s", len(contexts), output.String())
		}
		ids := map[int64]bool{17: true, 23: true}
		for _, line := range lines {
			var record struct {
				Message string `json:"msg"`
				TaskID  int64  `json:"task_id"`
				Error   string `json:"err"`
			}
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record.Message != "OutboxAsyncFailed" || !ids[record.TaskID] || !strings.Contains(record.Error, context.DeadlineExceeded.Error()) {
				t.Fatalf("异步失败日志缺少任务标识或错误: %s", line)
			}
			delete(ids, record.TaskID)
		}
	})
}
